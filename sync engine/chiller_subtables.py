"""Option A: mirror per-chiller subtables + rows both ways (local <-> Hostinger).

Tables per prefix:
  {prefix}_temp
  {prefix}_humidity
  {prefix}_door_status
  {prefix}_defrost
"""

from __future__ import annotations

import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from mysql_client import _qi
from queue_ops import get_metadata, set_metadata, write_log
from tables import record_uuid

# Door/defrost rows are updated in place (lock/close). Watermark-by-id alone
# misses those updates, so we always re-reconcile recent + dirty rows.
_MUTABLE_KINDS = frozenset({"door_status", "defrost"})
_RECHECK_LIMIT = 200
_UPSERT_MAX_ATTEMPTS = 8
_UPSERT_RETRY_SLEEP_S = 0.35

_TOMCL = Path(__file__).resolve().parent.parent / "tomcl_python"
if str(_TOMCL) not in sys.path:
    sys.path.insert(0, str(_TOMCL))

# kind -> (columns in order, mysql DDL body without table name)
SUBTABLE_SPECS: dict[str, tuple[list[str], str]] = {
    "temp": (
        ["id", "Date", "time_stamp", "temp"],
        """
        `id` BIGINT NOT NULL AUTO_INCREMENT,
        `Date` VARCHAR(32) NOT NULL,
        `time_stamp` VARCHAR(64) NOT NULL,
        `temp` DOUBLE NOT NULL,
        PRIMARY KEY (`id`)
        """,
    ),
    "humidity": (
        ["id", "Date", "time_stamp", "humidity"],
        """
        `id` BIGINT NOT NULL AUTO_INCREMENT,
        `Date` VARCHAR(32) NOT NULL,
        `time_stamp` VARCHAR(64) NOT NULL,
        `humidity` DOUBLE NOT NULL,
        PRIMARY KEY (`id`)
        """,
    ),
    "door_status": (
        ["id", "Date", "time_stamp_unlocked", "time_stamp_locked", "duration"],
        """
        `id` BIGINT NOT NULL AUTO_INCREMENT,
        `Date` VARCHAR(32) NOT NULL,
        `time_stamp_unlocked` VARCHAR(64) NULL,
        `time_stamp_locked` VARCHAR(64) NULL,
        `duration` VARCHAR(64) NULL,
        PRIMARY KEY (`id`)
        """,
    ),
    "defrost": (
        ["id", "time_stamp_defrost_ON", "time_stamp_defrost_OFF", "duration"],
        """
        `id` BIGINT NOT NULL AUTO_INCREMENT,
        `time_stamp_defrost_ON` VARCHAR(64) NULL,
        `time_stamp_defrost_OFF` VARCHAR(64) NULL,
        `duration` VARCHAR(64) NULL,
        PRIMARY KEY (`id`)
        """,
    ),
}

KINDS = ("temp", "humidity", "door_status", "defrost")


def _safe_prefix(prefix: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_]+", "_", (prefix or "").strip())
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        raise ValueError("Invalid table_prefix")
    return text


def table_name_for(prefix: str, kind: str) -> str:
    p = _safe_prefix(prefix)
    if kind not in SUBTABLE_SPECS:
        raise ValueError(f"Unknown subtable kind: {kind}")
    return f"{p}_{kind}" if kind != "door_status" else f"{p}_door_status"


def meta_key(direction: str, table: str) -> str:
    return f"sub:{direction}:{table}"


def ensure_mysql_chiller_subtables(mysql_conn: Any, prefix: str) -> list[str]:
    """CREATE IF NOT EXISTS the four metric tables on Hostinger."""
    created: list[str] = []
    p = _safe_prefix(prefix)
    with mysql_conn.cursor() as cur:
        for kind, (_cols, ddl) in SUBTABLE_SPECS.items():
            name = table_name_for(p, kind)
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {_qi(name)} ("
                f"{ddl}"
                f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
            created.append(name)
    return created


def drop_mysql_chiller_subtables(mysql_conn: Any, prefix: str) -> list[str]:
    """DROP the four metric tables on Hostinger (delete room)."""
    dropped: list[str] = []
    p = _safe_prefix(prefix)
    with mysql_conn.cursor() as cur:
        for kind in KINDS:
            name = table_name_for(p, kind)
            cur.execute(f"DROP TABLE IF EXISTS {_qi(name)}")
            dropped.append(name)
    return dropped


def ensure_local_chiller_subtables(sqlite_conn: sqlite3.Connection, prefix: str) -> list[str]:
    from chiller_rooms import create_chiller_tables

    names = create_chiller_tables(sqlite_conn, _safe_prefix(prefix))
    return list(names.values())


def _sqlite_table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _mysql_table_exists(mysql_conn: Any, name: str) -> bool:
    with mysql_conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = %s
            LIMIT 1
            """,
            (name,),
        )
        return cur.fetchone() is not None


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    return {k: row[k] for k in row.keys()}


def _kind_from_table(table: str) -> str | None:
    for kind in KINDS:
        suffix = f"_{kind}" if kind != "door_status" else "_door_status"
        if table.endswith(suffix):
            return kind
    return None


def _format_time_of_day(value: timedelta) -> str:
    """MySQL TIME → HH:MM:SS text."""
    total = int(value.total_seconds()) % (24 * 3600)
    if total < 0:
        total += 24 * 3600
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _format_duration_td(value: timedelta) -> str:
    total = int(value.total_seconds())
    sign = "-" if total < 0 else ""
    total = abs(total)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{sign}{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{sign}{minutes}m {secs}s"
    return f"{sign}{secs}s"


def _to_db_value(col: str, value: Any) -> Any:
    """Coerce driver types so SQLite/MySQL TEXT binds never see timedelta/date/Decimal."""
    if value is None:
        return None
    if col == "id":
        try:
            return int(value)
        except (TypeError, ValueError):
            return value
    if isinstance(value, timedelta):
        if col.lower() == "duration":
            return _format_duration_td(value)
        return _format_time_of_day(value)
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (str, int, float)):
        return value
    return str(value)


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    return str(_to_db_value("x", value) if not isinstance(value, str) else value).strip()


def _row_payload(cols: list[str], data: dict[str, Any]) -> dict[str, Any]:
    lower = {str(k).lower(): v for k, v in data.items()}
    out: dict[str, Any] = {}
    for c in cols:
        if c in data:
            raw = data[c]
        elif c.lower() in lower:
            raw = lower[c.lower()]
        else:
            raw = None
        out[c] = _to_db_value(c, raw)
    return out


def _rows_equal(cols: list[str], local: dict[str, Any], remote: dict[str, Any] | None) -> bool:
    if remote is None:
        return False
    for c in cols:
        if c == "id":
            continue
        if _norm_text(local.get(c)) != _norm_text(remote.get(c)):
            return False
    return True


def _fetch_mysql_rows_by_ids(
    mysql_conn: Any, table: str, ids: list[int]
) -> dict[int, dict[str, Any]]:
    if not ids:
        return {}
    out: dict[int, dict[str, Any]] = {}
    # Chunk to keep IN clauses small
    for i in range(0, len(ids), 200):
        chunk = ids[i : i + 200]
        placeholders = ", ".join(["%s"] * len(chunk))
        with mysql_conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {_qi(table)} WHERE `id` IN ({placeholders})",
                chunk,
            )
            for row in cur.fetchall():
                data = _row_dict(row)
                try:
                    out[int(data["id"])] = data
                except (KeyError, TypeError, ValueError):
                    continue
    return out


def _upsert_mysql_row(mysql_conn: Any, table: str, cols: list[str], data: dict[str, Any]) -> None:
    mapped = _row_payload(cols, data)
    values = [mapped[c] for c in cols]
    col_sql = ", ".join(_qi(c) for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{_qi(c)}=VALUES({_qi(c)})" for c in cols if c != "id")
    if not updates:
        updates = "`id`=`id`"
    sql = (
        f"INSERT INTO {_qi(table)} ({col_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )
    with mysql_conn.cursor() as cur:
        cur.execute(sql, values)


def _upsert_sqlite_row(
    sqlite_conn: sqlite3.Connection, table: str, cols: list[str], data: dict[str, Any]
) -> None:
    mapped = _row_payload(cols, data)
    col_sql = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join("?" for _ in cols)
    sqlite_conn.execute(
        f'INSERT OR REPLACE INTO "{table}" ({col_sql}) VALUES ({placeholders})',
        [mapped[c] for c in cols],
    )


def _upsert_with_retries(
    *,
    direction: str,
    sqlite_conn: sqlite3.Connection,
    mysql_conn: Any,
    table: str,
    cols: list[str],
    data: dict[str, Any],
    to_mysql: bool,
) -> None:
    """Retry a single row upsert until success (or attempts exhausted)."""
    rid = int(data["id"])
    last_exc: Exception | None = None
    for attempt in range(1, _UPSERT_MAX_ATTEMPTS + 1):
        try:
            if to_mysql:
                _upsert_mysql_row(mysql_conn, table, cols, data)
            else:
                _upsert_sqlite_row(sqlite_conn, table, cols, data)
            write_log(
                sqlite_conn,
                direction=direction,
                table_name=table,
                record_uuid_value=record_uuid(table, rid),
                operation="UPSERT",
                sync_status="SUCCESS",
            )
            return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            write_log(
                sqlite_conn,
                direction=direction,
                table_name=table,
                record_uuid_value=record_uuid(table, rid),
                operation="UPSERT",
                sync_status="FAILED",
                error_message=f"attempt {attempt}/{_UPSERT_MAX_ATTEMPTS}: {exc}"[:1000],
            )
            if attempt < _UPSERT_MAX_ATTEMPTS:
                time.sleep(_UPSERT_RETRY_SLEEP_S * attempt)
    assert last_exc is not None
    raise last_exc


def _collect_push_candidates(
    sqlite_conn: sqlite3.Connection,
    table: str,
    *,
    last_id: int,
    batch_size: int,
    mutable: bool,
) -> list[dict[str, Any]]:
    """New ids past watermark, plus recent rows that may have been updated in place."""
    by_id: dict[int, dict[str, Any]] = {}
    new_rows = sqlite_conn.execute(
        f'''
        SELECT * FROM "{table}"
        WHERE id > ?
        ORDER BY id ASC
        LIMIT ?
        ''',
        (last_id, batch_size),
    ).fetchall()
    for row in new_rows:
        data = _row_dict(row)
        by_id[int(data["id"])] = data

    if mutable:
        recent = sqlite_conn.execute(
            f'''
            SELECT * FROM "{table}"
            ORDER BY id DESC
            LIMIT ?
            ''',
            (_RECHECK_LIMIT,),
        ).fetchall()
        for row in recent:
            data = _row_dict(row)
            by_id[int(data["id"])] = data

    return [by_id[i] for i in sorted(by_id)]


def push_subtable(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: Any,
    table: str,
    cols: list[str],
    *,
    batch_size: int = 500,
) -> dict[str, int]:
    if not _sqlite_table_exists(sqlite_conn, table):
        return {"synced": 0, "failed": 0}
    if not _mysql_table_exists(mysql_conn, table):
        kind = _kind_from_table(table)
        if kind:
            suffix = f"_{kind}" if kind != "door_status" else "_door_status"
            ensure_mysql_chiller_subtables(mysql_conn, table[: -len(suffix)])

    key = meta_key("push", table)
    last_id, _ = get_metadata(sqlite_conn, key)
    kind = _kind_from_table(table)
    mutable = kind in _MUTABLE_KINDS
    candidates = _collect_push_candidates(
        sqlite_conn,
        table,
        last_id=last_id,
        batch_size=batch_size,
        mutable=mutable,
    )
    remote_map = _fetch_mysql_rows_by_ids(
        mysql_conn, table, [int(r["id"]) for r in candidates]
    )

    synced = 0
    failed = 0
    max_seen = last_id
    for data in candidates:
        rid = int(data["id"])
        local_payload = _row_payload(cols, data)
        remote = remote_map.get(rid)
        # Skip only when remote already matches (still advance watermark for new ids).
        if _rows_equal(cols, local_payload, remote):
            if rid > max_seen:
                max_seen = rid
            continue
        try:
            _upsert_with_retries(
                direction="SENT",
                sqlite_conn=sqlite_conn,
                mysql_conn=mysql_conn,
                table=table,
                cols=cols,
                data=data,
                to_mysql=True,
            )
            synced += 1
            if rid > max_seen:
                max_seen = rid
            remote_map[rid] = local_payload
        except Exception:  # noqa: BLE001
            failed += 1
            # New ids must stay behind the watermark; recheck rows retry next cycle.
            if rid > last_id:
                break
            continue

    # Watermark only moves after successful (or already-matched) rows.
    # Failed upserts are retried next cycle (and within _upsert_with_retries).
    if max_seen > last_id:
        set_metadata(sqlite_conn, key, max_seen)
    return {"synced": synced, "failed": failed, "last_id": max_seen}


def pull_subtable(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: Any,
    table: str,
    cols: list[str],
    *,
    batch_size: int = 500,
) -> dict[str, int]:
    if not _mysql_table_exists(mysql_conn, table):
        return {"received": 0, "failed": 0}
    if not _sqlite_table_exists(sqlite_conn, table):
        kind = _kind_from_table(table)
        if kind:
            suffix = f"_{kind}" if kind != "door_status" else "_door_status"
            ensure_local_chiller_subtables(sqlite_conn, table[: -len(suffix)])

    key = meta_key("pull", table)
    last_id, _ = get_metadata(sqlite_conn, key)
    kind = _kind_from_table(table)
    mutable = kind in _MUTABLE_KINDS

    with mysql_conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {_qi(table)} WHERE `id` > %s ORDER BY `id` ASC LIMIT %s",
            (last_id, batch_size),
        )
        rows = [_row_dict(r) for r in cur.fetchall()]
        if mutable:
            cur.execute(
                f"SELECT * FROM {_qi(table)} ORDER BY `id` DESC LIMIT %s",
                (_RECHECK_LIMIT,),
            )
            by_id = {int(r["id"]): r for r in rows}
            for r in cur.fetchall():
                data = _row_dict(r)
                by_id[int(data["id"])] = data
            rows = [by_id[i] for i in sorted(by_id)]

    received = 0
    failed = 0
    max_seen = last_id
    for data in rows:
        rid = int(data["id"])
        # Prefer local door/defrost content when local is "more complete" (has lock/off).
        if mutable and _sqlite_table_exists(sqlite_conn, table):
            local = sqlite_conn.execute(
                f'SELECT * FROM "{table}" WHERE id = ? LIMIT 1', (rid,)
            ).fetchone()
            if local is not None:
                local_d = _row_dict(local)
                remote_payload = _row_payload(cols, data)
                local_payload = _row_payload(cols, local_d)
                if _local_more_complete(kind, local_payload, remote_payload):
                    if rid > max_seen:
                        max_seen = rid
                    continue
                if _rows_equal(cols, local_payload, remote_payload):
                    if rid > max_seen:
                        max_seen = rid
                    continue
        try:
            _upsert_with_retries(
                direction="RECEIVED",
                sqlite_conn=sqlite_conn,
                mysql_conn=mysql_conn,
                table=table,
                cols=cols,
                data=data,
                to_mysql=False,
            )
            received += 1
            if rid > max_seen:
                max_seen = rid
        except Exception:  # noqa: BLE001
            failed += 1
            break

    if max_seen > last_id:
        set_metadata(sqlite_conn, key, max_seen)
    return {"received": received, "failed": failed, "last_id": max_seen}


def _local_more_complete(
    kind: str | None, local: dict[str, Any], remote: dict[str, Any]
) -> bool:
    """Keep local lock/close when Hostinger still has the open-only snapshot."""
    if kind == "door_status":
        loc_locked = _norm_text(local.get("time_stamp_locked"))
        rem_locked = _norm_text(remote.get("time_stamp_locked"))
        if loc_locked and not rem_locked:
            return True
        loc_dur = _norm_text(local.get("duration"))
        rem_dur = _norm_text(remote.get("duration"))
        if loc_dur and not rem_dur:
            return True
    if kind == "defrost":
        loc_off = _norm_text(local.get("time_stamp_defrost_OFF"))
        rem_off = _norm_text(remote.get("time_stamp_defrost_OFF"))
        if loc_off and not rem_off:
            return True
        loc_dur = _norm_text(local.get("duration"))
        rem_dur = _norm_text(remote.get("duration"))
        if loc_dur and not rem_dur:
            return True
    return False


def clear_subtable_watermarks(sqlite_conn: sqlite3.Connection, prefix: str) -> None:
    p = _safe_prefix(prefix)
    for kind in KINDS:
        table = table_name_for(p, kind)
        for direction in ("push", "pull"):
            sqlite_conn.execute(
                "DELETE FROM sync_metadata WHERE table_name = ?",
                (meta_key(direction, table),),
            )


def sync_all_chiller_subtables(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: Any,
    *,
    batch_size: int = 500,
) -> dict[str, Any]:
    """Ensure tables exist for every live room, then push + pull row batches."""
    from chiller_rooms_sync import get_tombstones

    tombstones = get_tombstones(sqlite_conn)
    rooms = sqlite_conn.execute(
        "SELECT id, table_prefix FROM chiller_rooms ORDER BY id"
    ).fetchall()

    ensured: list[str] = []
    push_total = 0
    pull_total = 0
    failed = 0
    details: list[dict[str, Any]] = []

    for room in rooms:
        rid = int(room["id"] if not isinstance(room, sqlite3.Row) else room["id"])
        if rid in tombstones:
            continue
        prefix = str(
            room["table_prefix"] if not isinstance(room, sqlite3.Row) else room["table_prefix"]
        ).strip()
        if not prefix:
            continue
        try:
            ensure_local_chiller_subtables(sqlite_conn, prefix)
            mysql_names = ensure_mysql_chiller_subtables(mysql_conn, prefix)
            ensured.extend(mysql_names)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            details.append({"prefix": prefix, "error": str(exc)[:300]})
            continue

        for kind, (cols, _ddl) in SUBTABLE_SPECS.items():
            table = table_name_for(prefix, kind)
            pushed = push_subtable(
                sqlite_conn, mysql_conn, table, cols, batch_size=batch_size
            )
            pulled = pull_subtable(
                sqlite_conn, mysql_conn, table, cols, batch_size=batch_size
            )
            push_total += int(pushed.get("synced") or 0)
            pull_total += int(pulled.get("received") or 0)
            failed += int(pushed.get("failed") or 0) + int(pulled.get("failed") or 0)

    return {
        "rooms": len(rooms),
        "ensured_tables": len(set(ensured)),
        "pushed_rows": push_total,
        "pulled_rows": pull_total,
        "failed": failed,
        "details": details[:10],
    }
