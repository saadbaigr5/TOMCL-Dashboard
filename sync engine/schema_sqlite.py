"""Ensure local SQLite sync_queue / sync_metadata / sync_log exist."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from tables import TABLE_MAP

SYNC_QUEUE_SQL = """
CREATE TABLE IF NOT EXISTS sync_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    record_uuid TEXT NOT NULL,
    operation TEXT NOT NULL,
    data TEXT,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_attempt TEXT,
    error_message TEXT
)
"""

SYNC_METADATA_SQL = """
CREATE TABLE IF NOT EXISTS sync_metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL UNIQUE,
    last_change_id INTEGER NOT NULL DEFAULT 0,
    last_sync_at TEXT
)
"""

SYNC_LOG_SQL = """
CREATE TABLE IF NOT EXISTS sync_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    direction TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_uuid TEXT,
    operation TEXT,
    sync_status TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    error_message TEXT
)
"""

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_sync_queue_status ON sync_queue(status, id)",
    "CREATE INDEX IF NOT EXISTS idx_sync_queue_record ON sync_queue(table_name, record_uuid, status)",
    "CREATE INDEX IF NOT EXISTS idx_sync_log_synced ON sync_log(synced_at)",
)


def connect_sqlite(db_path: Path, *, busy_ms: int = 30000, timeout_s: float = 60.0) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=timeout_s, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {busy_ms}")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    except sqlite3.OperationalError:
        pass
    return conn


def ensure_sync_tables(conn: sqlite3.Connection) -> None:
    conn.execute(SYNC_QUEUE_SQL)
    conn.execute(SYNC_METADATA_SQL)
    conn.execute(SYNC_LOG_SQL)
    for sql in _INDEXES:
        conn.execute(sql)
    names = list(TABLE_MAP.keys()) + ["__sync_changes__"]
    for table_name in names:
        conn.execute(
            """
            INSERT OR IGNORE INTO sync_metadata (table_name, last_change_id, last_sync_at)
            VALUES (?, 0, NULL)
            """,
            (table_name,),
        )
