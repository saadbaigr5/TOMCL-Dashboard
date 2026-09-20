"""Core sync cycle: push queue + raw watermark + pull both-way tables."""

from __future__ import annotations

from typing import Any

from config import load_config
from mysql_client import connect_mysql, ensure_mysql_tables
from chiller_rooms_sync import pull_chiller_rooms_mirror
from chiller_subtables import sync_all_chiller_subtables
from pull import pull_all
from push import process_queue, push_raw_chiller_data, reconcile_chiller_rooms
from schema_sqlite import connect_sqlite, ensure_sync_tables


def run_once(*, backfill_raw: bool = False) -> dict[str, Any]:
    cfg = load_config()
    sqlite_conn = connect_sqlite(cfg["sqlite_path"])
    try:
        ensure_sync_tables(sqlite_conn)
        mysql_conn = connect_mysql(cfg)
        try:
            ensure_mysql_tables(mysql_conn)
            # Delete-wins + Option A full subtable mirror:
            # 1) Queue (local deletes first)
            # 2) Pull rooms (create local tables / purge tombstones + DROP MySQL tables)
            # 3) Reconcile rooms (ensure MySQL registry + CREATE MySQL subtables)
            # 4) Sync subtable rows both ways
            queue_stats = process_queue(
                sqlite_conn, mysql_conn, limit=cfg["batch_size"]
            )
            pull_rooms = pull_chiller_rooms_mirror(sqlite_conn, mysql_conn)
            push_rooms = reconcile_chiller_rooms(sqlite_conn, mysql_conn)
            sub_stats = sync_all_chiller_subtables(
                sqlite_conn,
                mysql_conn,
                batch_size=max(100, int(cfg.get("raw_batch_size") or 500)),
            )
            raw_stats = push_raw_chiller_data(
                sqlite_conn,
                mysql_conn,
                batch_size=cfg["raw_batch_size"],
                backfill=backfill_raw,
            )
            pull_stats = pull_all(
                sqlite_conn, mysql_conn, batch_size=cfg["batch_size"]
            )
            return {
                "queue": queue_stats,
                "chiller_rooms_pull": pull_rooms,
                "chiller_rooms_push": push_rooms,
                "chiller_subtables": sub_stats,
                "raw": raw_stats,
                "pull": pull_stats,
            }
        finally:
            mysql_conn.close()
    finally:
        sqlite_conn.close()


def ping_mysql() -> dict[str, Any]:
    cfg = load_config()
    conn = connect_mysql(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE() AS db, NOW() AS server_time")
            row = cur.fetchone()
            cur.execute("SHOW TABLES")
            tables = [list(r.values())[0] for r in cur.fetchall()]
        return {"ok": True, "database": row["db"], "server_time": str(row["server_time"]), "tables": tables}
    finally:
        conn.close()
