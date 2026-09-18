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
