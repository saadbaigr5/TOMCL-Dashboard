"""Safe enqueue helpers for the Flask app (never raises into callers)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_SYNC_DIR = Path(__file__).resolve().parent
if str(_SYNC_DIR) not in sys.path:
    sys.path.insert(0, str(_SYNC_DIR))


def enqueue_safe(
    *,
    table_name: str,
    operation: str,
    pk_value: Any,
    data: dict[str, Any] | None = None,
) -> None:
    """Best-effort queue write using the same SQLite DB as the dashboard."""
    try:
        from config import sqlite_path
        from queue_ops import enqueue_change
        from schema_sqlite import connect_sqlite, ensure_sync_tables

        conn = connect_sqlite(sqlite_path(), busy_ms=3000, timeout_s=5)
        try:
            ensure_sync_tables(conn)
            enqueue_change(
                conn,
                table_name=table_name,
                operation=operation,
                pk_value=pk_value,
                data=data,
            )
        finally:
            conn.close()
    except Exception:
        # Sync must never break local CRUD.
        return
