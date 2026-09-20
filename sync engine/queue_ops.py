"""Enqueue local changes into sync_queue + write sync_log helpers."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any

from tables import TABLE_MAP, record_uuid


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def enqueue_change(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    operation: str,
    pk_value: Any,
    data: dict[str, Any] | None = None,
) -> int | None:
    """Queue one outbound change. Returns queue id or None if table is not queued."""
    meta = TABLE_MAP.get(table_name)
    if meta is None:
        return None
    if meta.get("use_queue", True) is False:
        return None
    if meta["direction"] not in ("both", "push"):
        return None

    op = (operation or "").strip().upper()
    if op not in ("INSERT", "UPDATE", "DELETE"):
        raise ValueError(f"Invalid operation: {operation}")

    uuid = record_uuid(table_name, pk_value)
    payload = None if data is None else json.dumps(data, default=str)

    # Collapse duplicate PENDING rows for the same record: keep latest intent.
    conn.execute(
        """
        UPDATE sync_queue
        SET status = 'SUPERSEDED'
        WHERE table_name = ?
          AND record_uuid = ?
          AND status = 'PENDING'
        """,
        (table_name, uuid),
    )

    # Intentional create of this id clears any old tombstone (new room).
    if table_name == "chiller_rooms" and op == "INSERT":
        try:
            stones = get_json_state(conn, "__chiller_rooms_tombstones__", default=[]) or []
            sid = int(pk_value)
            if sid in {int(x) for x in stones}:
                set_json_state(
                    conn,
                    "__chiller_rooms_tombstones__",
                    sorted(int(x) for x in stones if int(x) != sid),
                )
        except Exception:
            pass

    # Delete-wins: never let pull/reconcile restore this room id.
    if table_name == "chiller_rooms" and op == "DELETE":
        try:
            from chiller_rooms_sync import add_tombstone, cancel_outbound_room_upserts

            prefix = None
            if data and isinstance(data, dict):
                prefix = data.get("table_prefix") or data.get("table_Prefix")
            add_tombstone(conn, int(pk_value), prefix=str(prefix) if prefix else None)
            cancel_outbound_room_upserts(conn, int(pk_value))
        except Exception:
            pass

    cur = conn.execute(
        """
        INSERT INTO sync_queue (
            table_name, record_uuid, operation, data, created_at, status, retry_count
        ) VALUES (?, ?, ?, ?, ?, 'PENDING', 0)
        """,
        (table_name, uuid, op, payload, _now()),
    )
    return int(cur.lastrowid)


def write_log(
    conn: sqlite3.Connection,
    *,
    direction: str,
    table_name: str,
    record_uuid_value: str | None,
    operation: str | None,
    sync_status: str,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO sync_log (
            direction, table_name, record_uuid, operation, sync_status, synced_at, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            direction,
            table_name,
            record_uuid_value,
            operation,
            sync_status,
            _now(),
            error_message,
        ),
    )


def get_metadata(conn: sqlite3.Connection, table_name: str) -> tuple[int, str | None]:
    row = conn.execute(
        "SELECT last_change_id, last_sync_at FROM sync_metadata WHERE table_name = ?",
        (table_name,),
    ).fetchone()
    if not row:
        return 0, None
    return int(row["last_change_id"] or 0), row["last_sync_at"]


def set_metadata(conn: sqlite3.Connection, table_name: str, last_change_id: int) -> None:
    conn.execute(
        """
        INSERT INTO sync_metadata (table_name, last_change_id, last_sync_at)
        VALUES (?, ?, ?)
        ON CONFLICT(table_name) DO UPDATE SET
            last_change_id = excluded.last_change_id,
            last_sync_at = excluded.last_sync_at
        """,
        (table_name, int(last_change_id), _now()),
    )


def get_json_state(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = conn.execute(
        "SELECT last_sync_at FROM sync_metadata WHERE table_name = ?",
        (key,),
    ).fetchone()
    if not row or not row["last_sync_at"]:
        return default
    try:
        return json.loads(str(row["last_sync_at"]))
    except Exception:
        return default


def set_json_state(conn: sqlite3.Connection, key: str, value: Any) -> None:
    payload = json.dumps(value, default=str)
    conn.execute(
        """
        INSERT INTO sync_metadata (table_name, last_change_id, last_sync_at)
        VALUES (?, 0, ?)
        ON CONFLICT(table_name) DO UPDATE SET
            last_sync_at = excluded.last_sync_at
        """,
        (key, payload),
    )
