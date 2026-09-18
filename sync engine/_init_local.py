import time
from config import load_config
from schema_sqlite import connect_sqlite, ensure_sync_tables

cfg = load_config()
for i in range(10):
    try:
        conn = connect_sqlite(cfg["sqlite_path"], busy_ms=30000, timeout_s=60)
        ensure_sync_tables(conn)
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'sync_%' ORDER BY 1"
            ).fetchall()
        ]
        meta = [
            (r[0], r[1])
            for r in conn.execute(
                "SELECT table_name, last_change_id FROM sync_metadata ORDER BY table_name"
            ).fetchall()
        ]
        print("tables:", tables)
        print("metadata rows:", len(meta))
        for row in meta:
            print(" ", row)
        conn.close()
        print("sqlite_ok")
        break
    except Exception as exc:
        print(f"retry {i}: {exc}")
        time.sleep(2)
else:
    raise SystemExit("failed to init sync tables")
