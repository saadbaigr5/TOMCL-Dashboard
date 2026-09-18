"""Poll live chiller API -> DB_Tomcl.db

Source: http://192.168.0.50/api/data

When API is up:
  Append new raw rows (change-only) tagged with the matched room name
  (e.g. ROOM_01 -> "Chiller 1"), then disperse into room subtables.

When API is down:
  Still disperse any pending raw_chiller_data into allocated tables.
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
    chiller_match_keys,
    connect as connect_tomcl,
    device_api_url,
    disperse_new_raw_rows,
    disperse_raw_row,
    ensure_schema,
    list_chiller_rooms,
    parse_chiller_active_flag,
    set_chiller_active,
    write_lock_held,
)

# Fallback only if a room has no device_ip saved yet
DEFAULT_DATA_URL = "http://192.168.0.50/api/data"
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

# ESP32 uptime/counter timestamps change every poll — exclude from change detect.
CHANGE_FIELDS = (
    "chiller_status",
    "defrost_active",
    "door_status",
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


def fetch_snapshots(url: str) -> list[dict[str, Any]]:
    """Return one or more device readings from a room API URL."""
    payload = fetch_json(url)
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]
    if isinstance(payload, dict):
        # Some gateways wrap: {"data": {...}} or {"readings": [...]}
        for key in ("data", "reading", "payload"):
            inner = payload.get(key)
            if isinstance(inner, dict):
                return [inner]
            if isinstance(inner, list):
                return [p for p in inner if isinstance(p, dict)]
        for key in ("readings", "devices", "rooms"):
            inner = payload.get(key)
            if isinstance(inner, list):
                return [p for p in inner if isinstance(p, dict)]
        return [payload]
    raise RuntimeError(f"Unexpected payload: {type(payload).__name__}")


def room_data_url(room: dict[str, Any]) -> str | None:
    url = device_api_url(room.get("device_ip"))
    if url:
        return url
    return None


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


def _wall_clock_timestamp(raw_ts: Any) -> str:
    """Prefer ISO wall clock; ESP32 often sends uptime millis/seconds."""
    if raw_ts is None or raw_ts == "":
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(raw_ts, str):
        text = raw_ts.strip()
        # Already looks like a datetime
        if re.match(r"^\d{4}-\d{2}-\d{2}", text):
            return text
        try:
            raw_ts = float(text)
        except ValueError:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        num = float(raw_ts)
    except (TypeError, ValueError):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Unix seconds / millis
    if num >= 1_000_000_000_000:  # ms
        return datetime.fromtimestamp(num / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
    if num >= 1_000_000_000:  # seconds
        return datetime.fromtimestamp(num).strftime("%Y-%m-%d %H:%M:%S")
    # Uptime / counter — use local wall clock for dashboard readability
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_row(raw: dict[str, Any]) -> dict[str, Any]:
    """Map ESP32 / legacy simulator fields into raw_chiller_data columns."""
    # ESP32 gateway shape: temperature, humidity, door_open, chiller_on, defrost_on
    if "temperature" in raw and "temperature_c" not in raw:
        raw = dict(raw)
        raw["temperature_c"] = raw.get("temperature")
    if "humidity" in raw and "humidity_percent" not in raw:
        raw = dict(raw)
        raw.setdefault("humidity_percent", raw.get("humidity"))

    if "door_status" not in raw and "door_open" in raw:
        raw = dict(raw)
        raw["door_status"] = "unlocked" if to_bool_int(raw.get("door_open")) == 1 else "locked"

    if "chiller_status" not in raw and "chiller_on" in raw:
        raw = dict(raw)
        raw["chiller_status"] = "on" if to_bool_int(raw.get("chiller_on")) == 1 else "off"

    if "defrost_active" not in raw and "defrost_on" in raw:
        raw = dict(raw)
        raw["defrost_active"] = to_bool_int(raw.get("defrost_on"))

    row: dict[str, Any] = {}
    for field in COMPARE_FIELDS:
        value = raw.get(field)
        if field == "timestamp":
            row[field] = _wall_clock_timestamp(value)
            continue
        if field in BOOL_FIELDS:
            row[field] = to_bool_int(value)
        elif field in INT_FIELDS:
            row[field] = to_int(value)
        elif field in FLOAT_FIELDS:
            row[field] = to_float(value)
        else:
            row[field] = to_text(value)

    # ESP32 always reports temp/humidity fields when present on the wire
    if row["temperature_sensor_connected"] is None:
        row["temperature_sensor_connected"] = 1 if row.get("temperature_c") is not None else 0
    if row["humidity_sensor_connected"] is None:
        row["humidity_sensor_connected"] = 1 if row.get("humidity_percent") is not None else 0

    api_chiller = (
        raw.get("chiller_id")
        or raw.get("Chiller_id")
        or raw.get("chillerId")
        or raw.get("chiller_name")
        or raw.get("name")
        or raw.get("room_id")
        or raw.get("device_id")
    )
    row["chiller_id"] = to_text(api_chiller)
    row["_match_hints"] = [
        h
        for h in (
            raw.get("room_id"),
            raw.get("device_id"),
            raw.get("chiller_id"),
            raw.get("name"),
        )
        if h is not None and str(h).strip()
    ]
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


def _hint_tokens(hint: str) -> set[str]:
    text = str(hint).strip().lower()
    tokens = {text}
    # ROOM_01 / ESP32_ROOM_01 / Chiller_1 → numeric forms
    for m in re.finditer(r"(\d+)", text):
        num = m.group(1).lstrip("0") or "0"
        tokens.add(num)
        tokens.add(num.zfill(2))
        tokens.add(f"chiller {num}")
        tokens.add(f"chiller_{num}")
        tokens.add(f"room_{num}")
        tokens.add(f"room {num}")
        tokens.add(f"room_{num.zfill(2)}")
    return tokens


def match_rooms_for_reading(
    live: dict[str, Any], rooms: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Match API room_id/device_id to local chiller_rooms rows."""
    hints = [str(h) for h in (live.get("_match_hints") or [])]
    if live.get("chiller_id"):
        hints.append(str(live["chiller_id"]))
    hint_tokens: set[str] = set()
    for h in hints:
        hint_tokens |= _hint_tokens(h)

    matched: list[dict[str, Any]] = []
    for room in rooms:
        room_keys = {k.lower() for k in chiller_match_keys(room)}
        room_keys |= _hint_tokens(str(room.get("name") or ""))
        room_keys |= _hint_tokens(str(room.get("table_prefix") or ""))
        if hint_tokens & room_keys:
            matched.append(room)
    return matched


def last_raw_for_chiller(conn: sqlite3.Connection, room: dict[str, Any]) -> dict[str, Any] | None:
    ensure_raw_table(conn)
    keys = [k.lower() for k in chiller_match_keys(room)]
    if not keys:
        return None
    placeholders = ", ".join(["?"] * len(keys))
    row = conn.execute(
        f"""
        SELECT id, chiller_id, {", ".join(COMPARE_FIELDS)}
        FROM raw_chiller_data
        WHERE lower(COALESCE(chiller_id, '')) IN ({placeholders})
        ORDER BY id DESC
        LIMIT 1
        """,
        keys,
    ).fetchone()
    return dict(row) if row else None


def raw_changed(current: dict[str, Any], previous: dict[str, Any] | None) -> bool:
    if previous is None:
        return True
    return any(current.get(f) != previous.get(f) for f in CHANGE_FIELDS)


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
        f"door={row.get('door_status')} chiller={row.get('chiller_status')} "
        f"defrost={row.get('defrost_active')}"
    )


def distribute_from_existing_raw() -> None:
    """Disperse NEW raw rows into allocated tables by chiller_id match."""
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
    """Poll each room's device_ip (/api/data). Return True if any API was reachable."""
    any_ok = False
    targets: list[tuple[dict[str, Any], str]] = []
    for room in rooms:
        url = room_data_url(room)
        if url:
            targets.append((room, url))
        else:
            log(f"[{room.get('name')}] no chiller_IP — set IP in Chiller Rooms")

    if not targets:
        log(f"no room IPs configured — example fallback would be {DEFAULT_DATA_URL}")
        return False

    conn = connect_db()
    try:
        ensure_raw_table(conn)
        for room, url in targets:
            try:
                snapshots = fetch_snapshots(url)
            except Exception as exc:
                log(f"[{room.get('name')}] API unreachable ({url}): {exc}")
                continue
            any_ok = True
            if not snapshots:
                log(f"[{room.get('name')}] API returned no readings ({url})")
                continue
            for raw_snap in snapshots:
                live = normalize_row(raw_snap)
                # Prefer the room we polled; still allow multi-match if payload names another room
                matched_rooms = match_rooms_for_reading(live, rooms)
                if room not in matched_rooms:
                    matched_rooms = [room] + [r for r in matched_rooms if r.get("id") != room.get("id")]
                if not matched_rooms:
                    matched_rooms = [room]
                for target in matched_rooms:
                    # Only write into the room we are polling, unless payload clearly maps elsewhere
                    if target.get("id") != room.get("id"):
                        # Extra matches from ROOM_02 etc. — skip unless that room shares this poll
                        continue
                    chiller_id = target["name"]
                    previous = last_raw_for_chiller(conn, target)
                    tagged = {k: v for k, v in live.items() if not str(k).startswith("_")}
                    tagged["chiller_id"] = chiller_id
                    # Persist Active/Inactive into chiller_rooms.chiller_active
                    active_flag = parse_chiller_active_flag(tagged.get("chiller_status"))
                    if active_flag is None and "chiller_on" in raw_snap:
                        active_flag = to_bool_int(raw_snap.get("chiller_on")) == 1
                    if target.get("id") is not None and active_flag is not None:
                        label = set_chiller_active(int(target["id"]), active_flag)
                        if label:
                            log(f"[{chiller_id}] chiller_active={label}")
                    if raw_changed(tagged, previous):
                        new_id = insert_raw(conn, tagged, chiller_id)
                        actions = disperse_raw_row(conn, dict(tagged), rooms)
                        log(
                            f"raw append id={new_id} chiller_id={chiller_id!r} "
                            f"from {url} ({describe(tagged)}) dispersed={actions or 'none'}"
                        )
                    else:
                        log(f"raw unchanged for chiller_id={chiller_id!r} ({url})")
    finally:
        conn.close()
    return any_ok


def poll_once() -> None:
    if write_lock_held():
        log("admin write lock held - skip")
        return

    rooms = list_chiller_rooms()
    if not rooms:
        log("no chiller rooms in DB_Tomcl.db - create a room in Admin first")
        return

    try_fetch_and_append_raw(rooms)
    distribute_from_existing_raw()


def main() -> None:
    log("live source: per-room chiller_IP → http://IP/api/data")
    log(f"database {DB_PATH}")
    log("disperse: raw.chiller_id 'Chiller 1' -> Chiller_1_temp / _humidity / _door_status")
    ensure_schema()
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
