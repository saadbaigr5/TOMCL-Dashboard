"""Push local changes to Hostinger MySQL."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any

from mysql_client import delete_row, upsert_row
from queue_ops import get_metadata, set_metadata, write_log
from tables import TABLE_MAP, record_uuid


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _pk_from_uuid(table_name: str, uuid: str) -> Any:
    prefix = f"{table_name}:"
    if not uuid.startswith(prefix):
        return uuid
    raw = uuid[len(prefix) :]
    meta = TABLE_MAP[table_name]
    pk_name = meta["pk"]
    # Heuristic: integer PKs
    if pk_name.lower() in ("id", "user_id") or pk_name == "user_ID":
        try:
            return int(raw)
        except ValueError:
            return raw
    return raw


def process_queue(sqlite_conn: sqlite3.Connection, mysql_conn: Any, *, limit: int = 100) -> dict[str, int]:
    # Prefer DELETEs first so local room deletes hit Hostinger before any upsert.
    rows = sqlite_conn.execute(
        """
        SELECT * FROM sync_queue
        WHERE status IN ('PENDING', 'FAILED')
          AND retry_count < 8
        ORDER BY
          CASE WHEN UPPER(operation) = 'DELETE' THEN 0 ELSE 1 END,
          id ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    ok = 0
    fail = 0
    skipped = 0
    for row in rows:
        qid = int(row["id"])
        table_name = str(row["table_name"])
        operation = str(row["operation"]).upper()
        uuid = str(row["record_uuid"])
        sqlite_conn.execute(
            """
            UPDATE sync_queue
            SET last_attempt = ?, retry_count = retry_count + 1
            WHERE id = ?
            """,
            (_now(), qid),
        )
        try:
            if table_name not in TABLE_MAP:
                raise RuntimeError(f"Unknown table {table_name}")
            if TABLE_MAP[table_name]["direction"] not in ("both", "push"):
                raise RuntimeError(f"Table {table_name} is not push-enabled")
            if operation == "DELETE":
                if table_name == "chiller_rooms":
                    from chiller_rooms_sync import (
                        add_tombstone,
                        cancel_outbound_room_upserts,
                        tombstone_prefix,
                    )
                    from chiller_subtables import (
                        clear_subtable_watermarks,
                        drop_mysql_chiller_subtables,
                    )

                    pk_val = _pk_from_uuid(table_name, uuid)
                    payload_preview = {}
                    try:
                        payload_preview = json.loads(row["data"] or "{}") or {}
                    except Exception:
                        payload_preview = {}
                    prefix = (
                        payload_preview.get("table_prefix")
                        or tombstone_prefix(sqlite_conn, int(pk_val))
                    )
                    add_tombstone(
                        sqlite_conn,
                        int(pk_val),
                        prefix=str(prefix) if prefix else None,
                    )
                    cancel_outbound_room_upserts(sqlite_conn, int(pk_val))
                    if prefix:
                        try:
                            drop_mysql_chiller_subtables(mysql_conn, str(prefix))
                            clear_subtable_watermarks(sqlite_conn, str(prefix))
                        except Exception:
                            pass
                delete_row(mysql_conn, table_name, _pk_from_uuid(table_name, uuid))
            else:
                # Never recreate a chiller room that was deleted (tombstone) or missing locally.
                if table_name == "chiller_rooms":
                    from chiller_rooms_sync import get_tombstones

                    pk_val = _pk_from_uuid(table_name, uuid)
                    if int(pk_val) in get_tombstones(sqlite_conn):
                        sqlite_conn.execute(
                            """
                            UPDATE sync_queue
                            SET status = 'SUPERSEDED',
                                error_message = 'Skipped upsert: room id is tombstoned (deleted)'
                            WHERE id = ?
                            """,
                            (qid,),
                        )
                        write_log(
                            sqlite_conn,
                            direction="SENT",
                            table_name=table_name,
                            record_uuid_value=uuid,
                            operation=operation,
                            sync_status="SKIPPED",
                            error_message="Tombstoned room — not recreating on Hostinger",
                        )
                        skipped += 1
                        ok += 1
                        continue
                    exists = sqlite_conn.execute(
                        "SELECT 1 FROM chiller_rooms WHERE id = ? LIMIT 1",
                        (pk_val,),
                    ).fetchone()
                    if exists is None:
                        sqlite_conn.execute(
                            """
                            UPDATE sync_queue
                            SET status = 'SUPERSEDED',
                                error_message = 'Skipped upsert: chiller room not in local DB'
                            WHERE id = ?
                            """,
                            (qid,),
                        )
                        write_log(
                            sqlite_conn,
                            direction="SENT",
                            table_name=table_name,
                            record_uuid_value=uuid,
                            operation=operation,
                            sync_status="SKIPPED",
                            error_message="Room missing locally — not recreating on Hostinger",
                        )
                        skipped += 1
                        ok += 1
                        continue
                payload = json.loads(row["data"] or "{}")
                if not isinstance(payload, dict):
                    raise RuntimeError("Queue data must be a JSON object")
                # orders: local qty -> Hostinger QTY
                if table_name == "orders" and "QTY" not in payload and "qty" in payload:
                    payload["QTY"] = payload.get("qty")
                upsert_row(mysql_conn, table_name, payload)
            sqlite_conn.execute(
                "UPDATE sync_queue SET status = 'SYNCED', error_message = NULL WHERE id = ?",
                (qid,),
            )
            write_log(
                sqlite_conn,
                direction="SENT",
                table_name=table_name,
                record_uuid_value=uuid,
                operation=operation,
                sync_status="SUCCESS",
            )
            ok += 1
        except Exception as exc:  # noqa: BLE001 — log and continue queue
            msg = str(exc)[:1000]
            sqlite_conn.execute(
                "UPDATE sync_queue SET status = 'FAILED', error_message = ? WHERE id = ?",
                (msg, qid),
            )
            write_log(
                sqlite_conn,
                direction="SENT",
                table_name=table_name,
                record_uuid_value=uuid,
                operation=operation,
                sync_status="FAILED",
                error_message=msg,
            )
            fail += 1
    return {"synced": ok, "failed": fail, "skipped": skipped, "processed": len(rows)}


def push_raw_chiller_data(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: Any,
    *,
    batch_size: int = 500,
    backfill: bool = False,
) -> dict[str, int]:
    """Single-direction push using sync_metadata watermark (not sync_queue)."""
    table = "raw_chiller_data"
    last_id, _ = get_metadata(sqlite_conn, table)

    if last_id == 0 and not backfill:
        # First run: skip historical dump unless backfill requested.
        row = sqlite_conn.execute(f'SELECT COALESCE(MAX(id), 0) AS m FROM "{table}"').fetchone()
        max_local = int(row["m"] or 0)
        set_metadata(sqlite_conn, table, max_local)
        write_log(
            sqlite_conn,
            direction="SENT",
            table_name=table,
            record_uuid_value=record_uuid(table, max_local),
            operation="WATERMARK",
            sync_status="SUCCESS",
            error_message=f"Initialized watermark at {max_local} (use --backfill-raw to push history)",
        )
        return {"synced": 0, "failed": 0, "skipped_history": max_local}

    rows = sqlite_conn.execute(
        f"""
        SELECT * FROM "{table}"
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
        """,
        (last_id, batch_size),
    ).fetchall()

    synced = 0
    failed = 0
    max_seen = last_id
    for row in rows:
        data = {k: row[k] for k in row.keys()}
        rid = int(data["id"])
        try:
            upsert_row(mysql_conn, table, data)
            write_log(
                sqlite_conn,
                direction="SENT",
                table_name=table,
                record_uuid_value=record_uuid(table, rid),
                operation="INSERT",
                sync_status="SUCCESS",
            )
            synced += 1
            max_seen = rid
        except Exception as exc:  # noqa: BLE001
            failed += 1
            write_log(
                sqlite_conn,
                direction="SENT",
                table_name=table,
                record_uuid_value=record_uuid(table, rid),
                operation="INSERT",
                sync_status="FAILED",
                error_message=str(exc)[:1000],
            )
            break

    if max_seen > last_id:
        set_metadata(sqlite_conn, table, max_seen)
    return {"synced": synced, "failed": failed, "last_change_id": max_seen}


def repair_raw_chiller_ids(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: Any,
    *,
    batch_size: int = 500,
) -> dict[str, int]:
    """Fix Hostinger rows where chiller_id was coerced to 0 from text names like 'Chiller 1'."""
    table = "raw_chiller_data"
    updated = 0
    failed = 0
    scanned = 0

    with mysql_conn.cursor() as cur:
        cur.execute("SELECT MIN(id) AS lo, MAX(id) AS hi, COUNT(*) AS n FROM `raw_chiller_data`")
        bounds = cur.fetchone() or {}
    lo = int(bounds.get("lo") or 0)
    hi = int(bounds.get("hi") or 0)
    remote_n = int(bounds.get("n") or 0)
    print(f"Hostinger raw rows={remote_n} id range={lo}..{hi}", flush=True)
    if remote_n == 0 or hi == 0:
        return {"updated": 0, "failed": 0, "scanned": 0, "remote_rows": 0}

    last_id = lo - 1
    print("Repairing chiller_id in batches (Hostinger id range only)...", flush=True)
    while last_id < hi:
        rows = sqlite_conn.execute(
            f"""
            SELECT id, chiller_id FROM "{table}"
            WHERE id > ? AND id <= ?
              AND chiller_id IS NOT NULL
              AND TRIM(CAST(chiller_id AS TEXT)) != ''
              AND TRIM(CAST(chiller_id AS TEXT)) != '0'
            ORDER BY id ASC
            LIMIT ?
            """,
            (last_id, hi, batch_size),
        ).fetchall()
        if not rows:
            break
        payload = [(str(r["chiller_id"]).strip(), int(r["id"])) for r in rows]
        last_id = payload[-1][1]
        scanned += len(payload)
        try:
            with mysql_conn.cursor() as cur:
                cur.executemany(
                    "UPDATE `raw_chiller_data` SET `chiller_id` = %s WHERE `id` = %s",
                    payload,
                )
            updated += len(payload)
            print(f"  scanned={scanned} updated={updated} last_id={last_id}", flush=True)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"  batch failed at id>{payload[0][1]}: {exc}", flush=True)
            return {
                "updated": updated,
                "failed": failed,
                "scanned": scanned,
                "stopped_at": last_id,
                "error": str(exc)[:500],
            }
    return {
        "updated": updated,
        "failed": failed,
        "scanned": scanned,
        "last_id": last_id,
        "remote_rows": remote_n,
    }


def reconcile_chiller_rooms(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: Any,
) -> dict[str, int]:
    """Push every live local room to Hostinger; purge tombstoned ids from Hostinger."""
    meta = TABLE_MAP.get("chiller_rooms")
    if not meta or not meta.get("reconcile"):
        return {"upserted": 0, "skipped": 1}

    from chiller_rooms_sync import get_tombstones

    tombstones = get_tombstones(sqlite_conn)
    cols = list(meta["columns"])
    local_rows = sqlite_conn.execute("SELECT * FROM chiller_rooms ORDER BY id").fetchall()
    upserted = 0
    purged = 0

    for rid in sorted(tombstones):
        try:
            delete_row(mysql_conn, "chiller_rooms", rid)
            purged += 1
            write_log(
                sqlite_conn,
                direction="SENT",
                table_name="chiller_rooms",
                record_uuid_value=record_uuid("chiller_rooms", rid),
                operation="TOMBSTONE_DELETE",
                sync_status="SUCCESS",
            )
        except Exception:
            pass

    for row in local_rows:
        data = {k: row[k] for k in row.keys()}
        rid = int(data["id"])
        if rid in tombstones:
            continue
        payload: dict[str, Any] = {}
        lower = {str(k).lower(): v for k, v in data.items()}
        for c in cols:
            if c in data:
                payload[c] = data[c]
            elif c.lower() in lower:
                payload[c] = lower[c.lower()]
            else:
                payload[c] = None
        name = str(payload.get("name") or "").strip()
        prefix = str(payload.get("table_prefix") or "").strip()
        with mysql_conn.cursor() as cur:
            if name:
                cur.execute(
                    "DELETE FROM `chiller_rooms` WHERE `name` = %s AND `id` <> %s",
                    (name, rid),
                )
            if prefix:
                cur.execute(
                    "DELETE FROM `chiller_rooms` WHERE `table_prefix` = %s AND `id` <> %s",
                    (prefix, rid),
                )
        upsert_row(mysql_conn, "chiller_rooms", payload)
        prefix = str(payload.get("table_prefix") or "").strip()
        if prefix:
            try:
                from chiller_subtables import ensure_mysql_chiller_subtables

                ensure_mysql_chiller_subtables(mysql_conn, prefix)
            except Exception:
                pass
        write_log(
            sqlite_conn,
            direction="SENT",
            table_name="chiller_rooms",
            record_uuid_value=record_uuid("chiller_rooms", rid),
            operation="RECONCILE",
            sync_status="SUCCESS",
        )
        upserted += 1

    return {"upserted": upserted, "purged_tombstones": purged, "local": upserted}
