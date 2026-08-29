"""Poll simulator -> DB_Tomcl.db

Primary offline path (works even when API is down):
  Read raw_chiller_data by Chiller_id and fill each room's subtables.

When API is up:
  Also append new raw rows tagged with Chiller_id = room name.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR / "tomcl_python"))

from chiller_rooms import (  # noqa: E402
    apply_reading_to_room,
    connect as connect_tomcl,
    disperse_new_raw_rows,
    disperse_raw_row,
    ensure_schema,
    list_chiller_rooms,
    write_lock_held,
)

SOURCE_BASE = "https://chiller-room-simulator.vercel.app"
DASHBOARD_URL = f"{SOURCE_BASE}/api/dashboard"
READINGS_FALLBACK_URL = f"{SOURCE_BASE}/api/readings?limit=1"
POLL_SECONDS = 1
REQUEST_TIMEOUT = 4
DB_PATH = SCRIPT_DIR / "DB_TOMCL" / "DB_Tomcl.db"

CREATE_RAW_SQL = """
CREATE TABLE IF NOT EXISTS raw_chiller_data (
    id INTEGER PRIMARY KEY,
    chiller_id TEXT,
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
    humidity_sensor_connected INTEGER
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

BOOL_FIELDS = {
    "defrost_active",
    "temperature_sensor_connected",
    "humidity_sensor_connected",
}
INT_FIELDS = {"door_unlocked_seconds"}
FLOAT_FIELDS = {"temperature_c", "humidity_percent"}


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def fetch_json(url: str) -> Any:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "tomcl-chiller-fetcher"},
    )
    with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_snapshot() -> dict[str, Any]:
    last_error: Exception | None = None
    for url in (DASHBOARD_URL, READINGS_FALLBACK_URL):
        try:
            payload = fetch_json(url)
            if isinstance(payload, list):
                if not payload:
                    raise RuntimeError("Simulator returned no readings")
                return payload[0]
            if isinstance(payload, dict):
                return payload
            raise RuntimeError(f"Unexpected payload: {type(payload).__name__}")
        except Exception as exc:
            last_error = exc
            continue
    assert last_error is not None
    raise last_error


def to_bool_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on", "active", "connected"}:
            return 1
        if lowered in {"0", "false", "no", "off", "idle", "disconnected"}:
            return 0
    return 1 if value else 0


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def to_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)


def normalize_row(raw: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {}
    for field in COMPARE_FIELDS:
        value = raw.get(field)
        if field in BOOL_FIELDS:
            row[field] = to_bool_int(value)
        elif field in INT_FIELDS:
            row[field] = to_int(value)
        elif field in FLOAT_FIELDS:
            row[field] = to_float(value)
        else:
            row[field] = to_text(value)
    api_chiller = (
        raw.get("chiller_id")
        or raw.get("Chiller_id")
        or raw.get("chillerId")
        or raw.get("chiller_name")
        or raw.get("name")
    )
    row["chiller_id"] = to_text(api_chiller)
    if not row["timestamp"]:
        raise ValueError("Reading is missing timestamp")
    if row["door_unlocked_seconds"] is None:
        row["door_unlocked_seconds"] = 0
    return row


def connect_db() -> sqlite3.Connection:
    return connect_tomcl(busy_ms=3000, timeout_s=8)


def ensure_raw_table(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_RAW_SQL)
    cols = {str(r[1]).lower() for r in conn.execute("PRAGMA table_info(raw_chiller_data)")}
    if "chiller_id" not in cols:
        conn.execute("ALTER TABLE raw_chiller_data ADD COLUMN chiller_id TEXT")


def chiller_id_keys_for_room(room: dict[str, Any]) -> list[str]:
    name = str(room.get("name") or "").strip()
    prefix = str(room.get("table_prefix") or "").strip()
    rid = str(room.get("id") or "").strip()
    keys = {name, prefix, rid}
    m = re.search(r"(\d+)$", name)
    if m:
        keys.add(m.group(1))
        keys.add(m.group(1).zfill(2))
    return [k for k in keys if k]


def raw_rows_for_chiller(conn: sqlite3.Connection, room: dict[str, Any]) -> list[dict[str, Any]]:
    """All raw_chiller_data rows for this room, oldest first."""
    ensure_raw_table(conn)
    keys = chiller_id_keys_for_room(room)
    if not keys:
        return []
    placeholders = ", ".join(["?"] * len(keys))
    lowered = [k.lower() for k in keys]
    rows = conn.execute(
        f"""
        SELECT id, chiller_id, {", ".join(COMPARE_FIELDS)}
        FROM raw_chiller_data
        WHERE lower(COALESCE(chiller_id, '')) IN ({placeholders})
        ORDER BY id ASC
        """,
        lowered,
    ).fetchall()
    return [dict(r) for r in rows]


def last_raw_for_chiller(conn: sqlite3.Connection, room: dict[str, Any]) -> dict[str, Any] | None:
    rows = raw_rows_for_chiller(conn, room)
    return rows[-1] if rows else None


def raw_changed(current: dict[str, Any], previous: dict[str, Any] | None) -> bool:
    if previous is None:
        return True
    return any(current.get(f) != previous.get(f) for f in COMPARE_FIELDS)


def insert_raw(conn: sqlite3.Connection, row: dict[str, Any], chiller_id: str) -> int:
    ensure_raw_table(conn)
    columns = ["chiller_id", *COMPARE_FIELDS]
    placeholders = ", ".join(["?"] * len(columns))
    values = [chiller_id, *[row[field] for field in COMPARE_FIELDS]]
    cursor = conn.execute(
        f"INSERT INTO raw_chiller_data ({', '.join(columns)}) VALUES ({placeholders})",
        values,
    )
    return int(cursor.lastrowid)


def describe(row: dict[str, Any]) -> str:
    return (
        f"temp={row.get('temperature_c')}C hum={row.get('humidity_percent')}% "
        f"door={row.get('door_status')} chiller={row.get('chiller_status')}"
    )


def distribute_room_from_raw_history(conn: sqlite3.Connection, room: dict[str, Any]) -> dict[str, int]:
    """Replay every matching raw row into subtables (change-only inserts)."""
    counts = {"temp": 0, "humidity": 0, "door_unlocked": 0, "door_locked": 0, "rows": 0}
    rows = raw_rows_for_chiller(conn, room)
    counts["rows"] = len(rows)
    for raw in rows:
        actions = apply_reading_to_room(conn, room["table_prefix"], raw)
        for action in actions:
            counts[action] = counts.get(action, 0) + 1
    return counts


def distribute_from_existing_raw() -> None:
    """Disperse NEW raw rows into allocated tables by chiller_id match.

    Example: chiller_id 'Chiller 1' → Chiller_1_temp / Chiller_1_humidity / Chiller_1_door_status
    Uses a watermark so full history is not replayed every second.
    """
    rooms = list_chiller_rooms()
    if not rooms:
        log("no chiller rooms - create a room in Admin first")
        return

    try:
        summaries = disperse_new_raw_rows()
    except Exception as exc:
        log(f"disperse failed: {exc}")
        return

    for s in summaries:
        if s.get("matched", 0) == 0:
            continue
        log(
            f"[{s['room']}] dispersed {s['matched']} raw → {s['prefix']}_* | "
            f"temp+={s.get('temp', 0)} humidity+={s.get('humidity', 0)} "
            f"door_unlocked+={s.get('door_unlocked', 0)} door_locked+={s.get('door_locked', 0)}"
        )


def try_fetch_and_append_raw(rooms: list[dict[str, Any]]) -> bool:
    """Return True if API was reachable and raw was updated/checked."""
    try:
        live = normalize_row(fetch_snapshot())
    except Exception as exc:
        log(f"API unreachable ({SOURCE_BASE}): {exc}")
        return False

    api_chiller = live.get("chiller_id")
    conn = connect_db()
    try:
        ensure_raw_table(conn)
        for room in rooms:
            if api_chiller and str(api_chiller).strip().lower() not in {
                k.lower() for k in chiller_id_keys_for_room(room)
            }:
                continue
            chiller_id = room["name"]
            previous = last_raw_for_chiller(conn, room)
            tagged = dict(live)
            tagged["chiller_id"] = chiller_id
            if raw_changed(tagged, previous):
                new_id = insert_raw(conn, tagged, chiller_id)
                # Immediately disperse this row into Chiller_X_temp / humidity / door
                tagged_with_id = dict(tagged)
                actions = disperse_raw_row(conn, tagged_with_id, rooms)
                log(
                    f"raw append id={new_id} chiller_id={chiller_id!r} "
                    f"({describe(tagged)}) dispersed={actions or 'none'}"
                )
            else:
                log(f"raw unchanged for chiller_id={chiller_id!r}")
    finally:
        conn.close()
    return True


def poll_once() -> None:
    if write_lock_held():
        log("admin write lock held - skip")
        return

    rooms = list_chiller_rooms()
    if not rooms:
        log("no chiller rooms in DB_Tomcl.db - create a room in Admin first")
        return

    # 1) Optional live append into raw (skipped cleanly if source is down)
    try_fetch_and_append_raw(rooms)

    # 2) Always distribute from raw_chiller_data -> subtables by chiller_id match
    distribute_from_existing_raw()


def main() -> None:
    log(f"optional live source {DASHBOARD_URL}")
    log(f"database {DB_PATH}")
    log("disperse: raw.chiller_id 'Chiller 1' -> Chiller_1_temp / _humidity / _door_status")
    ensure_schema()
    # First pass immediately fills from existing raw even if API is down.
    try:
        poll_once()
    except Exception as exc:
        log(f"initial pass failed: {exc}")
    try:
        while True:
            time.sleep(POLL_SECONDS)
            try:
                poll_once()
            except Exception as exc:
                log(f"check failed: {exc}")
    except KeyboardInterrupt:
        log("stopped")


if __name__ == "__main__":
    main()
