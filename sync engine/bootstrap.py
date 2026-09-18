"""Bootstrap: seed sync_queue from existing local rows; init raw watermark."""

from __future__ import annotations

import sqlite3
from typing import Any

from queue_ops import enqueue_change, get_metadata, set_metadata
from tables import TABLE_MAP, push_tables


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def seed_queue_from_local(sqlite_conn: sqlite3.Connection, *, include_raw: bool = False) -> dict[str, int]:
    """Enqueue current local rows so the next push mirrors them to Hostinger."""
    counts: dict[str, int] = {}
    for table_name in push_tables():
        meta = TABLE_MAP[table_name]
        if meta.get("use_queue", True) is False:
            if include_raw and table_name == "raw_chiller_data":
                # Reset watermark so push_raw will upload from id 0.
                set_metadata(sqlite_conn, table_name, 0)
                counts[table_name] = -1  # means watermark reset
            continue
        pk = meta["pk"]
        try:
            rows = sqlite_conn.execute(f'SELECT * FROM "{table_name}"').fetchall()
        except sqlite3.OperationalError:
            counts[table_name] = 0
            continue
        # Drop stale outbound intents so bootstrap always ships a fresh full set.
        sqlite_conn.execute(
            """
            DELETE FROM sync_queue
            WHERE table_name = ?
              AND status IN ('PENDING', 'FAILED', 'SUPERSEDED')
            """,
            (table_name,),
        )
        n = 0
        for row in rows:
            data = _row_dict(row)
            # Normalize qty -> QTY for orders map
            if table_name == "orders" and "QTY" not in data and "qty" in data:
                data["QTY"] = data["qty"]
            pk_val = data.get(pk)
            if pk_val is None and table_name == "users":
                pk_val = data.get("user_ID") or data.get("user_id")
            if pk_val is None:
                continue
            if isinstance(pk_val, str):
                pk_val = pk_val.strip()
                if not pk_val:
                    continue
                data[pk] = pk_val
            enqueue_change(
                sqlite_conn,
                table_name=table_name,
                operation="INSERT",
                pk_value=pk_val,
                data=data,
            )
            n += 1
        counts[table_name] = n
    return counts


def init_raw_watermark_to_max(sqlite_conn: sqlite3.Connection) -> int:
    last, _ = get_metadata(sqlite_conn, "raw_chiller_data")
    if last > 0:
        return last
    row = sqlite_conn.execute(
        'SELECT COALESCE(MAX(id), 0) AS m FROM "raw_chiller_data"'
    ).fetchone()
    max_id = int(row["m"] or 0)
    set_metadata(sqlite_conn, "raw_chiller_data", max_id)
    return max_id
