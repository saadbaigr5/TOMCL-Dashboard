"""Pull Hostinger changes into local SQLite for both-way tables."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from mysql_client import fetch_all_rows, fetch_rows_after_pk, fetch_sync_changes
from queue_ops import get_metadata, set_metadata, write_log
from tables import TABLE_MAP, pull_tables, record_uuid

CLOUD_FEED_KEY = "__sync_changes__"


def _qi_sqlite(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _normalize_incoming(table_name: str, data: dict[str, Any]) -> dict[str, Any]:
    meta = TABLE_MAP[table_name]
    out: dict[str, Any] = {}
    lower = {str(k).lower(): v for k, v in data.items()}
    for col in meta["columns"]:
        if col in data:
            out[col] = data[col]
        elif col.lower() in lower:
            out[col] = lower[col.lower()]
    aliases = meta.get("sqlite_aliases") or {}
    for canonical, alts in aliases.items():
        if canonical in out:
            continue
        for alt in alts:
            if alt in data:
                out[canonical] = data[alt]
                break
            if alt.lower() in lower:
                out[canonical] = lower[alt.lower()]
                break
    return out


def _local_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({_qi_sqlite(table_name)})").fetchall()
    return {str(r[1]) for r in rows}


def apply_row_locally(
    sqlite_conn: sqlite3.Connection,
    table_name: str,
    data: dict[str, Any],
    *,
    operation: str = "UPSERT",
) -> str:
    meta = TABLE_MAP[table_name]
    row = _normalize_incoming(table_name, data)
    pk = meta["pk"]
    if operation != "DELETE" and pk not in row:
        raise ValueError(f"Missing PK {pk}")

    local_cols = _local_columns(sqlite_conn, table_name)
    mapped: dict[str, Any] = {}
    local_lower = {c.lower(): c for c in local_cols}
    for k, v in row.items():
        if k in local_cols:
            mapped[k] = v
        elif k.lower() in local_lower:
            mapped[local_lower[k.lower()]] = v

    pk_val = row.get(pk)
    if pk_val is None and operation == "DELETE":
        # Allow delete by uuid suffix only when payload empty.
        raise ValueError("DELETE requires PK in payload")

    uuid = record_uuid(table_name, pk_val)
    if operation == "DELETE":
        sqlite_conn.execute(
            f"DELETE FROM {_qi_sqlite(table_name)} WHERE {_qi_sqlite(pk)} = ?",
            (pk_val,),
        )
        return uuid

    if not mapped:
        raise ValueError("No matching columns for local apply")

    cols = list(mapped.keys())
    placeholders = ", ".join("?" for _ in cols)
    col_sql = ", ".join(_qi_sqlite(c) for c in cols)

    # destinations has no formal PRIMARY KEY locally — delete-then-insert.
    if table_name == "destinations" or pk == "destination":
        sqlite_conn.execute(
            f"DELETE FROM {_qi_sqlite(table_name)} WHERE {_qi_sqlite(pk)} = ?",
            (mapped[pk],),
        )
        sqlite_conn.execute(
            f"INSERT INTO {_qi_sqlite(table_name)} ({col_sql}) VALUES ({placeholders})",
            [mapped[c] for c in cols],
        )
        return uuid

    updates = ", ".join(
        f"{_qi_sqlite(c)}=excluded.{_qi_sqlite(c)}" for c in cols if c != pk
    )
    conflict = _qi_sqlite(pk)
    if updates:
        sql = (
            f"INSERT INTO {_qi_sqlite(table_name)} ({col_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict}) DO UPDATE SET {updates}"
        )
    else:
        sql = (
            f"INSERT INTO {_qi_sqlite(table_name)} ({col_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT({conflict}) DO NOTHING"
        )
    try:
        sqlite_conn.execute(sql, [mapped[c] for c in cols])
    except sqlite3.OperationalError:
        sqlite_conn.execute(
            f"INSERT OR REPLACE INTO {_qi_sqlite(table_name)} ({col_sql}) VALUES ({placeholders})",
            [mapped[c] for c in cols],
        )
    return uuid


def _is_numeric_pk(pk: str) -> bool:
    return pk.lower() in ("id", "user_id") or pk == "user_ID"


def pull_from_sync_changes(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: Any,
    *,
    batch_size: int = 100,
) -> dict[str, Any]:
    """Apply Hostinger sync_changes feed (updates/deletes) using cloud change_id watermark."""
    last_id, _ = get_metadata(sqlite_conn, CLOUD_FEED_KEY)
    rows = fetch_sync_changes(mysql_conn, last_id, batch_size)
    received = 0
    failed = 0
    skipped = 0
    max_seen = last_id
    allowed = set(pull_tables())

    for ch in rows:
        change_id = int(ch["change_id"])
        table_name = str(ch["table_name"])
        operation = str(ch["operation"] or "UPSERT").upper()
        try:
            if table_name not in allowed:
                skipped += 1
                max_seen = max(max_seen, change_id)
                continue
            payload: dict[str, Any] = {}
            raw = ch.get("data") or "{}"
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            if isinstance(raw, str) and raw.strip():
                payload = json.loads(raw)
            if not isinstance(payload, dict):
                payload = {}
            # Prefer payload; fall back to record_uuid suffix for deletes.
            if operation == "DELETE" and TABLE_MAP[table_name]["pk"] not in payload:
                uuid = str(ch.get("record_uuid") or "")
                prefix = f"{table_name}:"
                if uuid.startswith(prefix):
                    raw_pk = uuid[len(prefix) :]
                    pk = TABLE_MAP[table_name]["pk"]
                    if _is_numeric_pk(pk):
                        try:
                            payload[pk] = int(raw_pk)
                        except ValueError:
                            payload[pk] = raw_pk
                    else:
                        payload[pk] = raw_pk
            uuid = apply_row_locally(
                sqlite_conn, table_name, payload, operation=operation
            )
            write_log(
                sqlite_conn,
                direction="RECEIVED",
                table_name=table_name,
                record_uuid_value=uuid,
                operation=operation,
                sync_status="SUCCESS",
            )
            received += 1
            max_seen = max(max_seen, change_id)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            write_log(
                sqlite_conn,
                direction="RECEIVED",
                table_name=table_name,
                record_uuid_value=str(ch.get("record_uuid") or ""),
                operation=operation,
                sync_status="FAILED",
                error_message=str(exc)[:1000],
            )
            # Stop advancing watermark on failure so we retry this change.
            break

    if max_seen > last_id and failed == 0:
        set_metadata(sqlite_conn, CLOUD_FEED_KEY, max_seen)
    elif received == 0 and failed == 0:
        set_metadata(sqlite_conn, CLOUD_FEED_KEY, last_id)

    return {
        "received": received,
        "failed": failed,
        "skipped": skipped,
        "last_change_id": max_seen,
    }


def pull_table(
    sqlite_conn: sqlite3.Connection,
    mysql_conn: Any,
    table_name: str,
    *,
    batch_size: int = 100,
) -> dict[str, int]:
    meta = TABLE_MAP[table_name]
    if meta["direction"] not in ("both", "pull"):
        return {"received": 0, "failed": 0}

    last_id, _ = get_metadata(sqlite_conn, table_name)
    received = 0
    failed = 0
    max_seen = last_id

    if meta["pk"] in ("destination",) or not _is_numeric_pk(meta["pk"]):
        rows = fetch_all_rows(mysql_conn, table_name)
        for data in rows:
            try:
                uuid = apply_row_locally(sqlite_conn, table_name, data)
                write_log(
                    sqlite_conn,
                    direction="RECEIVED",
                    table_name=table_name,
                    record_uuid_value=uuid,
                    operation="UPSERT",
                    sync_status="SUCCESS",
                )
                received += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                write_log(
                    sqlite_conn,
                    direction="RECEIVED",
                    table_name=table_name,
                    record_uuid_value=None,
                    operation="UPSERT",
                    sync_status="FAILED",
                    error_message=str(exc)[:1000],
                )
        set_metadata(sqlite_conn, table_name, received if received else last_id)
        return {"received": received, "failed": failed}

    rows = fetch_rows_after_pk(mysql_conn, table_name, last_id, batch_size)
    for data in rows:
        try:
            row = _normalize_incoming(table_name, data)
            pk_val = row[meta["pk"]]
            uuid = apply_row_locally(sqlite_conn, table_name, row)
            write_log(
                sqlite_conn,
                direction="RECEIVED",
                table_name=table_name,
                record_uuid_value=uuid,
                operation="UPSERT",
                sync_status="SUCCESS",
            )
            received += 1
            try:
                max_seen = max(max_seen, int(pk_val))
            except (TypeError, ValueError):
                pass
        except Exception as exc:  # noqa: BLE001
            failed += 1
            write_log(
                sqlite_conn,
                direction="RECEIVED",
                table_name=table_name,
                record_uuid_value=None,
                operation="UPSERT",
                sync_status="FAILED",
                error_message=str(exc)[:1000],
            )

    if max_seen > last_id:
        set_metadata(sqlite_conn, table_name, max_seen)
    elif received == 0:
        set_metadata(sqlite_conn, table_name, last_id)

    return {"received": received, "failed": failed, "last_change_id": max_seen}


def pull_all(
    sqlite_conn: sqlite3.Connection, mysql_conn: Any, *, batch_size: int = 100
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "sync_changes": pull_from_sync_changes(
            sqlite_conn, mysql_conn, batch_size=batch_size
        )
    }
    for table_name in pull_tables():
        summary[table_name] = pull_table(
            sqlite_conn, mysql_conn, table_name, batch_size=batch_size
        )
    return summary
