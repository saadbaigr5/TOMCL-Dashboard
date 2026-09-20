#!/usr/bin/env python3
"""TOMCL sync engine runner — SQLite (local) <-> Hostinger MySQL.

Usage:
  python "run_sync.py" --ping
  python "run_sync.py" --once
  python "run_sync.py" --once --backfill-raw
  python "run_sync.py" --bootstrap
  python "run_sync.py" --bootstrap --backfill-raw
  python "run_sync.py" --loop
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))


def main() -> int:
    parser = argparse.ArgumentParser(description="TOMCL Hostinger sync engine")
    parser.add_argument("--ping", action="store_true", help="Test MySQL connection")
    parser.add_argument("--once", action="store_true", help="Run one sync cycle")
    parser.add_argument("--loop", action="store_true", help="Run forever")
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Enqueue existing local rows (small tables) for first push",
    )
    parser.add_argument(
        "--backfill-raw",
        action="store_true",
        help="Push historical raw_chiller_data (large) from id 0",
    )
    parser.add_argument(
        "--repair-chiller-ids",
        action="store_true",
        help="ALTER Hostinger chiller_id to VARCHAR and copy names from local SQLite",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="Override loop interval seconds",
    )
    args = parser.parse_args()

    if not any(
        [args.ping, args.once, args.loop, args.bootstrap, args.repair_chiller_ids]
    ):
        parser.print_help()
        return 1

    from config import load_config
    from schema_sqlite import connect_sqlite, ensure_sync_tables

    cfg = load_config()
    sqlite_conn = connect_sqlite(cfg["sqlite_path"])
    try:
        ensure_sync_tables(sqlite_conn)
    finally:
        sqlite_conn.close()
    print(f"SQLite: {cfg['sqlite_path']}")
    print(f"MySQL:  {cfg['mysql_user']}@{cfg['mysql_host']}/{cfg['mysql_database']}")

    if args.ping:
        from engine import ping_mysql

        result = ping_mysql()
        print(json.dumps(result, indent=2, default=str))
        return 0 if result.get("ok") else 2

    if args.repair_chiller_ids:
        from mysql_client import connect_mysql, ensure_mysql_tables
        from push import repair_raw_chiller_ids

        print("Connecting and ensuring chiller_id is VARCHAR...", flush=True)
        sqlite_conn = connect_sqlite(cfg["sqlite_path"])
        mysql = connect_mysql(cfg)
        try:
            ensure_sync_tables(sqlite_conn)
            ensure_mysql_tables(mysql)
            with mysql.cursor() as cur:
                cur.execute("SHOW COLUMNS FROM `raw_chiller_data` LIKE 'chiller_id'")
                col = cur.fetchone()
                print(f"Hostinger chiller_id column: {col}", flush=True)
            result = repair_raw_chiller_ids(
                sqlite_conn, mysql, batch_size=cfg["raw_batch_size"]
            )
            print("Repaired raw_chiller_data.chiller_id:")
            print(json.dumps(result, indent=2, default=str))
        finally:
            mysql.close()
            sqlite_conn.close()
        if not any([args.once, args.loop, args.bootstrap]):
            return 0

    if args.bootstrap:
        from bootstrap import seed_queue_from_local
        from mysql_client import connect_mysql, ensure_mysql_tables

        sqlite_conn = connect_sqlite(cfg["sqlite_path"])
        try:
            ensure_sync_tables(sqlite_conn)
            counts = seed_queue_from_local(
                sqlite_conn, include_raw=args.backfill_raw
            )
            print("Bootstrap enqueue counts:")
            print(json.dumps(counts, indent=2))
        finally:
            sqlite_conn.close()
        mysql = connect_mysql(cfg)
        try:
            ensure_mysql_tables(mysql)
            print("MySQL mirror tables ensured.")
        finally:
            mysql.close()

    if args.once or args.bootstrap:
        from engine import run_once

        summary = run_once(backfill_raw=args.backfill_raw)
        print("Sync cycle:")
        print(json.dumps(summary, indent=2, default=str))
        if args.once and not args.loop:
            return 0

    if args.loop:
        from engine import run_once

        interval = args.interval or cfg["interval_seconds"]
        print(f"Looping every {interval}s. Ctrl+C to stop.")
        while True:
            try:
                summary = run_once(backfill_raw=args.backfill_raw)
                q = summary.get("queue", {})
                r = summary.get("raw", {})
                print(
                    f"[{time.strftime('%H:%M:%S')}] "
                    f"queue synced={q.get('synced', 0)} failed={q.get('failed', 0)} | "
                    f"rooms pull +{summary.get('chiller_rooms_pull', {}).get('created', 0)}/"
                    f"-{summary.get('chiller_rooms_pull', {}).get('deleted_local', 0)} "
                    f"sub +{summary.get('chiller_subtables', {}).get('pushed_rows', 0)}/"
                    f"{summary.get('chiller_subtables', {}).get('pulled_rows', 0)} | "
                    f"raw synced={r.get('synced', 0)} failed={r.get('failed', 0)}"
                )
            except Exception as exc:  # noqa: BLE001
                print(f"[{time.strftime('%H:%M:%S')}] ERROR: {exc}")
            time.sleep(max(5, interval))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
