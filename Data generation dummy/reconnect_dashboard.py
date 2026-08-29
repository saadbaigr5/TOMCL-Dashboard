"""Reconnect dashboard backend after chiller_rooms was emptied.

Why dashboard showed 0 chillers:
  - raw_chiller_data had ~200k rows, but chiller_rooms was EMPTY
  - Overview only lists rooms from chiller_rooms
  - metric tables were empty so analytics showed No Data

This script:
  1) Registers Chiller 1 / Chiller 2
  2) Fast-replays raw_chiller_data into *_temp / *_humidity / *_door_status

Run (close DB Browser first):
  python reconnect_dashboard.py
"""

from __future__ import annotations

import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PACKAGE_ROOT / "tomcl_python"))

from chiller_rooms import (  # noqa: E402
    connect,
    create_chiller_tables,
    format_duration,
    list_chiller_rooms,
    parse_timestamp,
    register_chiller_room,
    split_date_time,
    table_names,
)

NEEDED = ["Chiller 1", "Chiller 2"]


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def ensure_rooms() -> list[dict]:
    existing = {r["name"].strip().lower(): r for r in list_chiller_rooms()}
    for name in NEEDED:
        if name.lower() in existing:
            log(f"room ok: {name} (prefix={existing[name.lower()]['table_prefix']})")
            continue
        try:
            room = register_chiller_room(name)
            log(f"registered: {room['name']} prefix={room['table_prefix']} id={room['id']}")
        except ValueError as exc:
            log(f"skip register {name}: {exc}")
        except Exception as exc:
            log(f"ERROR registering {name}: {exc}")
    return list_chiller_rooms()


def match_keys(room: dict) -> list[str]:
    name = str(room.get("name") or "").strip()
    prefix = str(room.get("table_prefix") or "").strip()
    rid = str(room.get("id") or "").strip()
    keys = {name.lower(), prefix.lower(), rid.lower()}
    parts = name.split()
    if parts and parts[-1].isdigit():
        keys.add(parts[-1])
        keys.add(parts[-1].zfill(2))
    return [k for k in keys if k]


def distribute_fast(conn: sqlite3.Connection, room: dict) -> None:
    """In-memory change-only replay (no per-row SELECT) into metric tables."""
    name = room["name"]
    prefix = room["table_prefix"]
    create_chiller_tables(conn, prefix)
    names = table_names(prefix)
    keys = match_keys(room)
    placeholders = ", ".join(["?"] * len(keys))

    rows = conn.execute(
        f"""
        SELECT timestamp, door_status, door_unlocked_at, temperature_c, humidity_percent
        FROM raw_chiller_data
        WHERE lower(COALESCE(chiller_id, '')) IN ({placeholders})
        ORDER BY id ASC
        """,
        keys,
    ).fetchall()

    log(f"[{name}] replaying {len(rows):,} raw rows -> {prefix}_*")
    if not rows:
        log(f"[{name}] no matching raw rows (check chiller_id values)")
        return

    prev_temp: float | None = None
    prev_hum: float | None = None
    prev_door: str | None = None

    temp_batch: list[tuple] = []
    hum_batch: list[tuple] = []
    door_inserts = 0
    door_closes = 0
    t0 = time.monotonic()

    def flush_batches() -> None:
        nonlocal temp_batch, hum_batch
        for attempt in range(15):
            try:
                if temp_batch:
                    conn.executemany(
                        f'INSERT INTO "{names["temp"]}" (Date, time_stamp, temp) VALUES (?, ?, ?)',
                        temp_batch,
                    )
                if hum_batch:
                    conn.executemany(
                        f'INSERT INTO "{names["humidity"]}" (Date, time_stamp, humidity) VALUES (?, ?, ?)',
                        hum_batch,
                    )
                temp_batch = []
                hum_batch = []
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                time.sleep(0.35 * (attempt + 1))
        raise sqlite3.OperationalError("database stayed locked while flushing batches")

    def exec_retry(sql: str, params: tuple[Any, ...]) -> None:
        for attempt in range(15):
            try:
                conn.execute(sql, params)
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                time.sleep(0.35 * (attempt + 1))
        raise sqlite3.OperationalError("database stayed locked on door write")

    for i, r in enumerate(rows, start=1):
        ts = r["timestamp"] or datetime.now().isoformat(timespec="seconds")
        date_part, time_part = split_date_time(str(ts))

        temp = r["temperature_c"]
        if temp is not None:
            tval = float(temp)
            if prev_temp is None or tval != prev_temp:
                temp_batch.append((date_part, time_part, tval))
                prev_temp = tval

        hum = r["humidity_percent"]
        if hum is not None:
            hval = float(hum)
            if prev_hum is None or hval != prev_hum:
                hum_batch.append((date_part, time_part, hval))
                prev_hum = hval

        door_raw = (r["door_status"] or "").strip().lower()
        if door_raw in {"unlocked", "open"}:
            door = "unlocked"
        elif door_raw in {"locked", "closed"}:
            door = "locked"
        else:
            door = ""

        if door and door != prev_door:
            flush_batches()
            if door == "unlocked":
                unlocked_at = r["door_unlocked_at"] or time_part
                unlocked_date, unlocked_ts = split_date_time(str(unlocked_at))
                exec_retry(
                    f"""
                    INSERT INTO "{names["door_status"]}"
                    (Date, time_stamp_unlocked, time_stamp_locked, duration)
                    VALUES (?, ?, NULL, NULL)
                    """,
                    (unlocked_date, unlocked_ts),
                )
                door_inserts += 1
            else:
                open_row = None
                for attempt in range(15):
                    try:
                        open_row = conn.execute(
                            f"""
                            SELECT id, time_stamp_unlocked FROM "{names["door_status"]}"
                            WHERE time_stamp_locked IS NULL
                            ORDER BY id DESC LIMIT 1
                            """
                        ).fetchone()
                        break
                    except sqlite3.OperationalError as exc:
                        if "locked" not in str(exc).lower():
                            raise
                        time.sleep(0.35 * (attempt + 1))
                if open_row is not None:
                    unlocked_dt = parse_timestamp(open_row["time_stamp_unlocked"]) or parse_timestamp(time_part)
                    locked_dt = parse_timestamp(time_part) or datetime.now()
                    duration = None
                    if unlocked_dt is not None:
                        duration = format_duration((locked_dt - unlocked_dt).total_seconds())
                    exec_retry(
                        f"""
                        UPDATE "{names["door_status"]}"
                        SET time_stamp_locked = ?, duration = ?
                        WHERE id = ?
                        """,
                        (time_part, duration, open_row["id"]),
                    )
                    door_closes += 1
            prev_door = door

        if len(temp_batch) >= 4000 or len(hum_batch) >= 4000:
            flush_batches()

        if i % 50000 == 0:
            flush_batches()
            elapsed = max(time.monotonic() - t0, 0.001)
            log(f"  ... {i:,}/{len(rows):,} ({i / elapsed:,.0f} rows/s)")

    flush_batches()

    tc = conn.execute(f'SELECT COUNT(*) FROM "{names["temp"]}"').fetchone()[0]
    hc = conn.execute(f'SELECT COUNT(*) FROM "{names["humidity"]}"').fetchone()[0]
    dc = conn.execute(f'SELECT COUNT(*) FROM "{names["door_status"]}"').fetchone()[0]
    log(
        f"[{name}] metric counts temp={tc:,} humidity={hc:,} door={dc:,} "
        f"(door opens={door_inserts} closes={door_closes})"
    )


def main() -> int:
    log("reconnecting dashboard backend…")
    rooms = ensure_rooms()
    if not rooms:
        log("ERROR: still no rooms in chiller_rooms")
        return 1

    conn = connect(busy_ms=60000, timeout_s=120)
    try:
        for room in rooms:
            distribute_fast(conn, room)
    finally:
        conn.close()

    log("done - refresh http://127.0.0.1:5000/overview")
    for r in list_chiller_rooms():
        log(f"  - {r['name']} -> {r['table_prefix']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
