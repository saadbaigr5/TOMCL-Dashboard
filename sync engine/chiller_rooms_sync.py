"""Both-way chiller_rooms mirror with delete-wins tombstones.

If a room is deleted on local OR Hostinger, it must be deleted on the other side
and must never be restored by pull/reconcile.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from mysql_client import delete_row, fetch_all_rows
from queue_ops import get_json_state, set_json_state, write_log
from tables import record_uuid
from chiller_subtables import (
    clear_subtable_watermarks,
    drop_mysql_chiller_subtables,
    ensure_mysql_chiller_subtables,
)

_TOMCL = Path(__file__).resolve().parent.parent / "tomcl_python"
if str(_TOMCL) not in sys.path:
    sys.path.insert(0, str(_TOMCL))

REMOTE_IDS_KEY = "__chiller_rooms_remote_ids__"
TOMBSTONE_KEY = "__chiller_rooms_tombstones__"
TOMBSTONE_META_KEY = "__chiller_rooms_tombstone_meta__"


def _prefix_for_room(sqlite_conn: Any, room_id: int, fallback: str | None = None) -> str | None:
    if fallback and str(fallback).strip():
        return str(fallback).strip()
    row = sqlite_conn.execute(
        "SELECT table_prefix FROM chiller_rooms WHERE id = ? LIMIT 1",
        (int(room_id),),
    ).fetchone()
    if row is None:
        return None
    return str(row["table_prefix"] if hasattr(row, "keys") else row[0] or "").strip() or None


def _purge_room_subtables(
    sqlite_conn: Any,
    mysql_conn: Any,
    room_id: int,
    prefix: str | None,
) -> None:
    pref = (prefix or "").strip() or tombstone_prefix(sqlite_conn, room_id)
    if not pref:
        return
    try:
        drop_mysql_chiller_subtables(mysql_conn, pref)
    except Exception:
        pass
    try:
        clear_subtable_watermarks(sqlite_conn, pref)
    except Exception:
        pass


def get_tombstones(sqlite_conn: Any) -> set[int]:
    raw = get_json_state(sqlite_conn, TOMBSTONE_KEY, default=[])
    try:
        return {int(x) for x in (raw or [])}
    except Exception:
        return set()


def add_tombstone(
    sqlite_conn: Any, room_id: int, *, prefix: str | None = None
) -> None:
    stones = get_tombstones(sqlite_conn)
    stones.add(int(room_id))
    set_json_state(sqlite_conn, TOMBSTONE_KEY, sorted(stones))
    pref = (prefix or "").strip() or _prefix_for_room(sqlite_conn, room_id)
    if pref:
        meta = get_json_state(sqlite_conn, TOMBSTONE_META_KEY, default={}) or {}
        if not isinstance(meta, dict):
            meta = {}
        meta[str(int(room_id))] = pref
        set_json_state(sqlite_conn, TOMBSTONE_META_KEY, meta)


def tombstone_prefix(sqlite_conn: Any, room_id: int) -> str | None:
    meta = get_json_state(sqlite_conn, TOMBSTONE_META_KEY, default={}) or {}
    if isinstance(meta, dict):
        val = meta.get(str(int(room_id)))
        if val:
            return str(val).strip() or None
    return _prefix_for_room(sqlite_conn, room_id)


def prune_tombstones(sqlite_conn: Any, remote_ids: set[int], local_ids: set[int]) -> int:
    """Drop tombstones only when the id is gone from both local and Hostinger."""
    stones = get_tombstones(sqlite_conn)
    keep = {i for i in stones if i in remote_ids or i in local_ids}
    removed = len(stones) - len(keep)
    set_json_state(sqlite_conn, TOMBSTONE_KEY, sorted(keep))
    meta = get_json_state(sqlite_conn, TOMBSTONE_META_KEY, default={}) or {}
    if isinstance(meta, dict):
        meta = {k: v for k, v in meta.items() if int(k) in keep}
        set_json_state(sqlite_conn, TOMBSTONE_META_KEY, meta)
    return removed


def cancel_outbound_room_upserts(sqlite_conn: Any, room_id: int) -> int:
    """Drop pending INSERT/UPDATE so they cannot recreate a deleted room."""
    uuid = record_uuid("chiller_rooms", room_id)
    cur = sqlite_conn.execute(
        """
        UPDATE sync_queue
        SET status = 'SUPERSEDED',
            error_message = 'Cancelled: room deleted (tombstone) — will not restore'
        WHERE table_name = 'chiller_rooms'
          AND record_uuid = ?
          AND status IN ('PENDING', 'FAILED')
          AND UPPER(operation) IN ('INSERT', 'UPDATE')
        """,
        (uuid,),
    )
    return int(cur.rowcount or 0)


def _rooms_api():
    from chiller_rooms import delete_chiller_room, upsert_room_from_remote

    return delete_chiller_room, upsert_room_from_remote


def ensure_local_room_gone(sqlite_conn: Any, room_id: int) -> bool:
    """Delete local room + subtables if present. Returns True if a delete ran."""
    delete_chiller_room, _ = _rooms_api()
    row = sqlite_conn.execute(
        "SELECT 1 FROM chiller_rooms WHERE id = ? LIMIT 1", (int(room_id),)
    ).fetchone()
    if row is None:
        return False
    delete_chiller_room(int(room_id), enqueue=False)
    return True


def pull_chiller_rooms_mirror(
    sqlite_conn: Any,
    mysql_conn: Any,
) -> dict[str, int]:
    """Mirror Hostinger chiller_rooms with delete-wins semantics."""
    delete_chiller_room, upsert_room_from_remote = _rooms_api()
    tombstones = get_tombstones(sqlite_conn)

    remote_rows = fetch_all_rows(mysql_conn, "chiller_rooms")
    remote_ids: set[int] = set()
    created = 0
    updated = 0
    deleted_local = 0
    deleted_remote = 0
    failed = 0

    for data in remote_rows:
        try:
            rid = int(data.get("id"))
        except (TypeError, ValueError):
            failed += 1
            continue
        remote_ids.add(rid)

        # Deleted on either side → never restore; purge Hostinger copy + subtables.
        if rid in tombstones:
            prefix = str(
                data.get("table_prefix")
                or data.get("table_Prefix")
                or ""
            ).strip() or None
            _purge_room_subtables(sqlite_conn, mysql_conn, rid, prefix)
            cancel_outbound_room_upserts(sqlite_conn, rid)
            try:
                delete_row(mysql_conn, "chiller_rooms", rid)
                deleted_remote += 1
                write_log(
                    sqlite_conn,
                    direction="SENT",
                    table_name="chiller_rooms",
                    record_uuid_value=record_uuid("chiller_rooms", rid),
                    operation="TOMBSTONE_DELETE",
                    sync_status="SUCCESS",
                    error_message="Removed Hostinger room + subtables (tombstone)",
                )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                write_log(
                    sqlite_conn,
                    direction="SENT",
                    table_name="chiller_rooms",
                    record_uuid_value=record_uuid("chiller_rooms", rid),
                    operation="TOMBSTONE_DELETE",
                    sync_status="FAILED",
                    error_message=str(exc)[:1000],
                )
            try:
                if ensure_local_room_gone(sqlite_conn, rid):
                    deleted_local += 1
            except Exception:
                pass
            cancel_outbound_room_upserts(sqlite_conn, rid)
            remote_ids.discard(rid)
            continue

        existed = sqlite_conn.execute(
            "SELECT 1 FROM chiller_rooms WHERE id = ? LIMIT 1", (rid,)
        ).fetchone()
        try:
            upsert_room_from_remote(data)
            prefix = str(data.get("table_prefix") or "").strip()
            if prefix:
                try:
                    ensure_mysql_chiller_subtables(mysql_conn, prefix)
                except Exception:
                    pass
            write_log(
                sqlite_conn,
                direction="RECEIVED",
                table_name="chiller_rooms",
                record_uuid_value=record_uuid("chiller_rooms", rid),
                operation="UPSERT",
                sync_status="SUCCESS",
            )
            if existed:
                updated += 1
            else:
                created += 1
        except Exception as exc:  # noqa: BLE001
            failed += 1
            write_log(
                sqlite_conn,
                direction="RECEIVED",
                table_name="chiller_rooms",
                record_uuid_value=record_uuid("chiller_rooms", rid),
                operation="UPSERT",
                sync_status="FAILED",
                error_message=str(exc)[:1000],
            )

    prev_raw = get_json_state(sqlite_conn, REMOTE_IDS_KEY, default=[])
    try:
        prev_ids = {int(x) for x in (prev_raw or [])}
    except Exception:
        prev_ids = set()

    # Hostinger delete: id was seen before, now gone → tombstone + drop local + MySQL tables.
    for rid in sorted(prev_ids - remote_ids):
        add_tombstone(sqlite_conn, rid)
        tombstones.add(rid)
        cancel_outbound_room_upserts(sqlite_conn, rid)
        prefix = _prefix_for_room(sqlite_conn, rid)
        _purge_room_subtables(sqlite_conn, mysql_conn, rid, prefix)
        try:
            if ensure_local_room_gone(sqlite_conn, rid):
                deleted_local += 1
                write_log(
                    sqlite_conn,
                    direction="RECEIVED",
                    table_name="chiller_rooms",
                    record_uuid_value=record_uuid("chiller_rooms", rid),
                    operation="DELETE",
                    sync_status="SUCCESS",
                    error_message="Dropped local room + subtables after Hostinger delete",
                )
            else:
                cancel_outbound_room_upserts(sqlite_conn, rid)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            write_log(
                sqlite_conn,
                direction="RECEIVED",
                table_name="chiller_rooms",
                record_uuid_value=record_uuid("chiller_rooms", rid),
                operation="DELETE",
                sync_status="FAILED",
                error_message=str(exc)[:1000],
            )

    # Enforce tombstones on any leftover local rows
    for rid in sorted(get_tombstones(sqlite_conn)):
        cancel_outbound_room_upserts(sqlite_conn, rid)
        prefix = _prefix_for_room(sqlite_conn, rid)
        _purge_room_subtables(sqlite_conn, mysql_conn, rid, prefix)
        try:
            if ensure_local_room_gone(sqlite_conn, rid):
                deleted_local += 1
        except Exception:
            pass

    local_ids = {
        int(r[0])
        for r in sqlite_conn.execute("SELECT id FROM chiller_rooms").fetchall()
    }
    # Refresh remote after tombstone purges
    remote_ids = {
        int(r["id"])
        for r in fetch_all_rows(mysql_conn, "chiller_rooms")
        if r.get("id") is not None
    }
    prune_tombstones(sqlite_conn, remote_ids, local_ids)
    set_json_state(sqlite_conn, REMOTE_IDS_KEY, sorted(remote_ids))

    return {
        "created": created,
        "updated": updated,
        "deleted_local": deleted_local,
        "deleted_remote": deleted_remote,
        "failed": failed,
        "remote": len(remote_ids),
        "tombstones": len(get_tombstones(sqlite_conn)),
    }


def apply_chiller_rooms_change(
    sqlite_conn: Any,
    *,
    operation: str,
    payload: dict[str, Any],
    record_uuid_value: str,
) -> str:
    """Apply one sync_changes event for chiller_rooms with subtable create/drop."""
    delete_chiller_room, upsert_room_from_remote = _rooms_api()
    op = (operation or "UPSERT").upper()
    if op == "DELETE":
        pk = payload.get("id")
        if pk is None:
            prefix = "chiller_rooms:"
            if record_uuid_value.startswith(prefix):
                pk = record_uuid_value[len(prefix) :]
        rid = int(pk)
        add_tombstone(sqlite_conn, rid)
        cancel_outbound_room_upserts(sqlite_conn, rid)
        try:
            delete_chiller_room(rid, enqueue=False)
        except ValueError:
            pass
        cancel_outbound_room_upserts(sqlite_conn, rid)
        return record_uuid("chiller_rooms", rid)

    try:
        rid = int(payload.get("id"))
    except (TypeError, ValueError):
        rid = None
    if rid is not None and rid in get_tombstones(sqlite_conn):
        # Do not resurrect a tombstoned room from sync_changes
        cancel_outbound_room_upserts(sqlite_conn, rid)
        return record_uuid("chiller_rooms", rid)

    upsert_room_from_remote(payload)
    return record_uuid("chiller_rooms", payload.get("id"))
