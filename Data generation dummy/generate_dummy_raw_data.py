"""Seed + live dummy generator for DB_TOMCL/DB_Tomcl.db → raw_chiller_data.

Phase 1 (ONE TIME ONLY) — Push 200,000 UNIQUE historical rows once:
  Timestamps evenly from 22 August → today (YTD).
  Sample-shaped fields; temp ↔ humidity inverse (colder → higher humidity).
  A marker file prevents this bulk seed from ever running again.

Phase 2 — Live loop (same idea as fetch_chiller_data.py):
  every 1s, build a new reading; INSERT only when values changed.

Usage:
  python generate_dummy_raw_data.py
  python generate_dummy_raw_data.py --rows 200000 --once
  python generate_dummy_raw_data.py --skip-seed
  python generate_dummy_raw_data.py --force-seed   # ignore marker (careful)
"""

from __future__ import annotations

import argparse
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = SCRIPT_DIR.parent
DB_PATH = PACKAGE_ROOT / "DB_TOMCL" / "DB_Tomcl.db"
# After a successful 200k seed, this file blocks any future bulk push
SEED_MARKER = SCRIPT_DIR / ".raw_chiller_seed_done"

sys.path.insert(0, str(PACKAGE_ROOT / "tomcl_python"))
from chiller_rooms import disperse_new_raw_rows, disperse_raw_row, list_chiller_rooms, write_lock_held  # noqa: E402

# Pakistan Standard Time (matches sample timestamps)
TZ = timezone(timedelta(hours=5))
# Historical window: 22 August → YTD (today)
SEED_START_MONTH = 8
SEED_START_DAY = 22

CREATE_RAW_SQL = """
CREATE TABLE IF NOT EXISTS raw_chiller_data (
    id INTEGER PRIMARY KEY,
    timestamp TEXT NOT NULL,
    chiller_status TEXT,
    defrost_active INTEGER,
    defrost_start TEXT,
    defrost_end TEXT,
    door_status TEXT,
    door_unlocked_at TEXT,
    door_unlocked_seconds INTEGER DEFAULT 0,
    temperature_c REAL,
    humidity_percent REAL,
    temperature_sensor_connected INTEGER,
    humidity_sensor_connected INTEGER,
    chiller_id TEXT
)
"""

COMPARE_FIELDS = (
    "timestamp",
    "chiller_status",
    "defrost_active",
    "defrost_start",
    "defrost_end",
    "door_status",
    "door_unlocked_at",
    "door_unlocked_seconds",
    "temperature_c",
    "humidity_percent",
    "temperature_sensor_connected",
    "humidity_sensor_connected",
)

INSERT_SQL = """
INSERT INTO raw_chiller_data (
    chiller_id,
    timestamp,
    chiller_status,
    defrost_active,
    defrost_start,
    defrost_end,
    door_status,
    door_unlocked_at,
    door_unlocked_seconds,
    temperature_c,
    humidity_percent,
    temperature_sensor_connected,
    humidity_sensor_connected
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

BATCH_SIZE = 5000
TEMP_MIN = -14.0
TEMP_MAX = 1.5
HUM_AT_WARM = 68.0
HUM_AT_COLD = 92.0


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=60, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA temp_store = MEMORY")
    return conn


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_RAW_SQL)
    cols = {str(r[1]).lower() for r in conn.execute("PRAGMA table_info(raw_chiller_data)")}
    if "chiller_id" not in cols:
        conn.execute("ALTER TABLE raw_chiller_data ADD COLUMN chiller_id TEXT")


def chiller_names(conn: sqlite3.Connection) -> list[str]:
    try:
        rows = conn.execute("SELECT name FROM chiller_rooms ORDER BY id").fetchall()
        names = [str(r[0]).strip() for r in rows if r[0]]
        if names:
            return names
    except sqlite3.Error:
        pass
    return ["Chiller 1", "Chiller 2"]


def fmt_ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    else:
        dt = dt.astimezone(TZ)
    return dt.isoformat(timespec="seconds")


def humidity_from_temp(temp_c: float, rng: random.Random) -> float:
    """Inverse relation: colder temperature → higher humidity (+ small noise)."""
    span = max(TEMP_MAX - TEMP_MIN, 0.001)
    t = max(TEMP_MIN, min(TEMP_MAX, temp_c))
    coldness = (TEMP_MAX - t) / span
    hum = HUM_AT_WARM + coldness * (HUM_AT_COLD - HUM_AT_WARM)
    hum += rng.uniform(-1.2, 1.2)
    return round(max(55.0, min(98.0, hum)), 1)


def history_window() -> tuple[datetime, datetime]:
    """22 August 00:00 → now (YTD), in +05:00."""
    end = datetime.now(TZ)
    start = datetime(end.year, SEED_START_MONTH, SEED_START_DAY, 0, 0, 0, tzinfo=TZ)
    if start > end:
        start = datetime(end.year - 1, SEED_START_MONTH, SEED_START_DAY, 0, 0, 0, tzinfo=TZ)
    return start, end


def make_unique_reading(
    *,
    when: datetime,
    chiller_id: str,
    rng: random.Random,
    seq: int,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if state is None:
        base = TEMP_MIN + (seq % 1000) / 1000.0 * (TEMP_MAX - TEMP_MIN)
        temp = round(base + rng.uniform(-0.35, 0.35), 2)
        chiller_on = rng.random() > 0.12
        door_unlocked = rng.random() < 0.14
        defrost = rng.random() < 0.06
        unlocked_secs = rng.randint(30, 480) if door_unlocked else 0
        unlocked_at_dt = when - timedelta(seconds=unlocked_secs) if door_unlocked else None
    else:
        temp = float(state["temperature_c"]) + rng.uniform(-0.25, 0.25)
        temp = round(max(TEMP_MIN, min(TEMP_MAX, temp)), 2)
        chiller_on = state["chiller_status"] == "on"
        if rng.random() < 0.03:
            chiller_on = not chiller_on
        door_unlocked = state["door_status"] == "unlocked"
        if rng.random() < 0.04:
            door_unlocked = not door_unlocked
        defrost = bool(state["defrost_active"])
        if rng.random() < 0.02:
            defrost = not defrost
        if door_unlocked:
            if state.get("door_unlocked_at"):
                try:
                    started = datetime.fromisoformat(str(state["door_unlocked_at"]))
                    unlocked_secs = max(1, int((when - started).total_seconds()))
                    unlocked_at_dt = started
                except ValueError:
                    unlocked_secs = int(state.get("door_unlocked_seconds") or 0) + 1
                    unlocked_at_dt = when - timedelta(seconds=unlocked_secs)
            else:
                unlocked_secs = 1
                unlocked_at_dt = when
        else:
            unlocked_secs = 0
            unlocked_at_dt = None

    temp = round(temp + (seq % 17) * 0.001, 3)
    hum = humidity_from_temp(temp, rng)

    defrost_start = None
    defrost_end = None
    if defrost:
        defrost_start = fmt_ts(when - timedelta(minutes=rng.randint(2, 35)))
    elif state and state.get("defrost_active") and not defrost:
        defrost_end = fmt_ts(when)

    return {
        "chiller_id": chiller_id,
        "timestamp": fmt_ts(when),
        "chiller_status": "on" if chiller_on else "off",
        "defrost_active": 1 if defrost else 0,
        "defrost_start": defrost_start,
        "defrost_end": defrost_end,
        "door_status": "unlocked" if door_unlocked else "locked",
        "door_unlocked_at": fmt_ts(unlocked_at_dt) if unlocked_at_dt else None,
        "door_unlocked_seconds": int(unlocked_secs),
        "temperature_c": temp,
        "humidity_percent": hum,
        "temperature_sensor_connected": 1,
        "humidity_sensor_connected": 1,
    }


def row_tuple(row: dict[str, Any]) -> tuple:
    return (
        row["chiller_id"],
        row["timestamp"],
        row["chiller_status"],
        row["defrost_active"],
        row["defrost_start"],
        row["defrost_end"],
        row["door_status"],
        row["door_unlocked_at"],
        row["door_unlocked_seconds"],
        row["temperature_c"],
        row["humidity_percent"],
        row["temperature_sensor_connected"],
        row["humidity_sensor_connected"],
    )


def raw_changed(current: dict[str, Any], previous: dict[str, Any] | None) -> bool:
    if previous is None:
        return True
    return any(current.get(f) != previous.get(f) for f in COMPARE_FIELDS)


def mark_seed_done(total: int, start: datetime, end: datetime) -> None:
    SEED_MARKER.write_text(
        f"seeded_at={datetime.now(TZ).isoformat()}\n"
        f"rows={total}\n"
        f"from={start.isoformat()}\n"
        f"to={end.isoformat()}\n",
        encoding="utf-8",
    )


def seed_unique(
    conn: sqlite3.Connection,
    total: int,
    names: list[str],
    *,
    force: bool = False,
) -> None:
    """Push exactly `total` unique rows ONCE (22 Aug → YTD). Never repeats after marker."""
    existing = int(conn.execute("SELECT COUNT(*) FROM raw_chiller_data").fetchone()[0])
    log(f"raw_chiller_data currently has {existing:,} rows")

    if SEED_MARKER.is_file() and not force:
        log(
            f"one-time 200k seed already done ({SEED_MARKER.name}) — "
            "skipping bulk push; live mode will continue"
        )
        return

    if force:
        log("--force-seed: ignoring one-time marker")

    start, end = history_window()
    span = (end - start).total_seconds()
    if span <= 0:
        raise ValueError("Invalid history window: start must be before now")

    need = total
    step = span / max(need - 1, 1)

    log(f"ONE-TIME seed: inserting {need:,} UNIQUE rows")
    log(f"date range: {fmt_ts(start)}  →  {fmt_ts(end)}  (22 Aug → YTD)")
    log("tip: close DB Browser on DB_Tomcl.db if inserts stall")
    log("pattern: colder temp → higher humidity (inverse)")

    rng = random.Random(20260822)
    written = 0
    t0 = time.monotonic()
    i = 0
    seen_keys: set[tuple[str, str]] = set()

    try:
        for r in conn.execute(
            "SELECT chiller_id, timestamp FROM raw_chiller_data ORDER BY id DESC LIMIT 8000"
        ):
            seen_keys.add((str(r[0] or ""), str(r[1] or "")))
    except sqlite3.Error:
        pass

    while i < need:
        batch: list[tuple] = []
        while len(batch) < BATCH_SIZE and i < need:
            when = start + timedelta(seconds=step * i)
            when = when + timedelta(microseconds=(i % 1000) * 1000)
            cid = names[i % len(names)]
            row = make_unique_reading(when=when, chiller_id=cid, rng=rng, seq=i)
            key = (row["chiller_id"], row["timestamp"])
            if key in seen_keys:
                when = when + timedelta(milliseconds=1 + (i % 97))
                row["timestamp"] = fmt_ts(when)
                key = (row["chiller_id"], row["timestamp"])
            seen_keys.add(key)
            batch.append(row_tuple(row))
            i += 1

        for attempt in range(12):
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.executemany(INSERT_SQL, batch)
                conn.execute("COMMIT")
                break
            except sqlite3.OperationalError as exc:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                    raise
                wait = 0.4 * (attempt + 1)
                log(f"  database locked — retry in {wait:.1f}s")
                time.sleep(wait)
        else:
            raise sqlite3.OperationalError(
                "database stayed locked. Close DB Browser and run again."
            )

        written += len(batch)
        if written % 50000 == 0 or written >= need:
            elapsed = max(time.monotonic() - t0, 0.001)
            log(f"  … {written:,}/{need:,} ({written / elapsed:,.0f} rows/s)")

    final = int(conn.execute("SELECT COUNT(*) FROM raw_chiller_data").fetchone()[0])
    mark_seed_done(need, start, end)
    log(f"seed complete — inserted {written:,} rows once; table now {final:,}")
    log(f"marker written: {SEED_MARKER.name} (bulk seed will not run again)")
    log("dispersing raw -> Chiller_X_temp / _humidity / _door_status ...")
    try:
        for s in disperse_new_raw_rows():
            if s.get("matched", 0):
                log(
                    f"  [{s['room']}] {s['matched']} rows -> {s['prefix']}_* "
                    f"temp+={s.get('temp', 0)} hum+={s.get('humidity', 0)}"
                )
    except Exception as exc:
        log(f"disperse after seed failed: {exc}")


def last_raw_for(conn: sqlite3.Connection, chiller_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        f"""
        SELECT chiller_id, {", ".join(COMPARE_FIELDS)}
        FROM raw_chiller_data
        WHERE lower(COALESCE(chiller_id, '')) = lower(?)
        ORDER BY id DESC
        LIMIT 1
        """,
        (chiller_id,),
    ).fetchone()
    return dict(row) if row else None


def live_loop(names: list[str], interval: float) -> None:
    log(f"live mode: poll every {interval:g}s — insert only when data changes (Ctrl+C to stop)")
    rng = random.Random()
    states: dict[str, dict[str, Any]] = {}
    seq = 0

    for name in names:
        conn = connect()
        try:
            prev = last_raw_for(conn, name)
        finally:
            conn.close()
        if prev:
            states[name] = prev

    while True:
        if write_lock_held():
            time.sleep(0.5)
            continue
        now = datetime.now(TZ)
        for name in names:
            seq += 1
            prev = states.get(name)
            if prev is None:
                conn = connect()
                try:
                    prev = last_raw_for(conn, name)
                finally:
                    conn.close()
            reading = make_unique_reading(
                when=now,
                chiller_id=name,
                rng=rng,
                seq=seq,
                state=prev,
            )
            if not raw_changed(reading, prev):
                reading["temperature_c"] = round(
                    float(reading["temperature_c"]) + rng.choice((-0.01, 0.01)),
                    3,
                )
                reading["humidity_percent"] = humidity_from_temp(
                    float(reading["temperature_c"]), rng
                )

            if raw_changed(reading, prev):
                for attempt in range(8):
                    if write_lock_held():
                        time.sleep(0.5)
                        break
                    conn = connect()
                    try:
                        cur = conn.execute(INSERT_SQL, row_tuple(reading))
                        new_id = int(cur.lastrowid or 0)
                        actions = disperse_raw_row(conn, reading)
                        log(
                            f"raw append id={new_id} chiller_id={name!r} "
                            f"temp={reading['temperature_c']}C "
                            f"hum={reading['humidity_percent']}% "
                            f"door={reading['door_status']} "
                            f"dispersed={actions or 'none'}"
                        )
                        states[name] = reading
                        break
                    except sqlite3.OperationalError as exc:
                        if "locked" not in str(exc).lower():
                            raise
                        time.sleep(0.25 * (attempt + 1))
                    finally:
                        conn.close()
                else:
                    log(f"DB locked — skipped push for {name!r}")
            else:
                log(f"raw unchanged for chiller_id={name!r}")

        time.sleep(max(interval, 0.5))


def main() -> int:
    parser = argparse.ArgumentParser(description="Dummy unique raw_chiller_data generator")
    parser.add_argument("--rows", type=int, default=200_000, help="One-time seed size")
    parser.add_argument("--interval", type=float, default=1.0, help="Live poll seconds")
    parser.add_argument("--skip-seed", action="store_true")
    parser.add_argument("--once", action="store_true", help="Seed only, no live loop")
    parser.add_argument(
        "--force-seed",
        action="store_true",
        help="Run bulk 200k seed again even if marker exists",
    )
    args = parser.parse_args()

    log(f"database {DB_PATH}")
    if not DB_PATH.parent.is_dir():
        log(f"ERROR: missing {DB_PATH.parent}")
        return 1

    conn = connect()
    names: list[str] = []
    try:
        ensure_table(conn)
        names = chiller_names(conn)
        log(f"chillers: {names}")

        if not args.skip_seed:
            seed_unique(conn, args.rows, names, force=args.force_seed)
        else:
            # Still catch up allocated tables from any undispersed raw
            log("catching up disperse from raw_chiller_data ...")
            try:
                for s in disperse_new_raw_rows():
                    if s.get("matched", 0):
                        log(
                            f"  [{s['room']}] {s['matched']} rows -> {s['prefix']}_* "
                            f"temp+={s.get('temp', 0)} hum+={s.get('humidity', 0)}"
                        )
            except Exception as exc:
                log(f"disperse catch-up failed: {exc}")

        if args.once:
            log("done (--once)")
            return 0
    except KeyboardInterrupt:
        log("stopped by user")
        return 0
    finally:
        conn.close()

    if not list_chiller_rooms():
        log("WARNING: no rooms in chiller_rooms — create Chiller 1/2 in Admin so disperse can work")

    try:
        live_loop(names, args.interval)
    except KeyboardInterrupt:
        log("stopped by user")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
