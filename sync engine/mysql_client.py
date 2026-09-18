"""Hostinger MySQL connection + mirror table DDL / upsert / delete."""

from __future__ import annotations

from typing import Any

import pymysql
from pymysql.connections import Connection

from tables import TABLE_MAP


def connect_mysql(cfg: dict[str, Any]) -> Connection:
    return pymysql.connect(
        host=cfg["mysql_host"],
        port=cfg["mysql_port"],
        user=cfg["mysql_user"],
        password=cfg["mysql_password"],
        database=cfg["mysql_database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=20,
        read_timeout=120,
        write_timeout=120,
    )


def _qi(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _local_to_mysql_col(table_name: str, col: str) -> str:
    mapping = (TABLE_MAP[table_name].get("mysql_column_map") or {})
    return mapping.get(col, col)


def _mysql_to_local_row(table_name: str, data: dict[str, Any]) -> dict[str, Any]:
    """Map Hostinger column names back to local canonical names."""
    mapping = TABLE_MAP[table_name].get("mysql_column_map") or {}
    reverse = {v: k for k, v in mapping.items()}
    out: dict[str, Any] = {}
    for k, v in data.items():
        out[reverse.get(k, k)] = v
    return out


def _coerce_mysql_value(table_name: str, col: str, value: Any) -> Any:
    """Hostinger columns are often NOT NULL — avoid sending Python None."""
    if value is not None:
        return value
    meta = TABLE_MAP[table_name]
    col_type = str(meta["mysql_types"].get(col, "TEXT")).upper()
    if any(t in col_type for t in ("INT", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT")):
        return 0
    return ""


def ensure_mysql_tables(conn: Connection) -> None:
    with conn.cursor() as cur:
        for table_name, meta in TABLE_MAP.items():
            cols_sql = []
            for col in meta["columns"]:
                mysql_col = _local_to_mysql_col(table_name, col)
                col_type = meta["mysql_types"].get(col, "TEXT")
                cols_sql.append(f"{_qi(mysql_col)} {col_type}")
            pk = _local_to_mysql_col(table_name, meta["pk"])
            ddl = (
                f"CREATE TABLE IF NOT EXISTS {_qi(table_name)} ("
                + ", ".join(cols_sql)
                + f", PRIMARY KEY ({_qi(pk)})"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
            cur.execute(ddl)

        # destinations historically has no PK — upsert needs a unique key.
        try:
            cur.execute(
                "ALTER TABLE `destinations` "
                "ADD UNIQUE KEY `uq_destinations_destination` (`destination`)"
            )
        except Exception:
            pass

        # raw_chiller_data.chiller_id must be text (local stores "Chiller 1", not int).
        try:
            cur.execute(
                "ALTER TABLE `raw_chiller_data` "
                "MODIFY COLUMN `chiller_id` VARCHAR(64) NULL"
            )
        except Exception:
            pass

        try:
            cur.execute(
                "ALTER TABLE `chiller_rooms` ADD COLUMN `chiller_IP` VARCHAR(64) NULL"
            )
        except Exception:
            pass

        # Cloud change feed used for reliable both-way pull of updates/deletes.
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS `sync_changes` (
                `change_id` BIGINT NOT NULL AUTO_INCREMENT,
                `table_name` VARCHAR(100) NOT NULL,
                `record_uuid` VARCHAR(100) NOT NULL,
                `operation` VARCHAR(20) NOT NULL,
                `data` TEXT NOT NULL,
                `changed_at` DATETIME NOT NULL,
                PRIMARY KEY (`change_id`),
                KEY `idx_sync_changes_id` (`change_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )


def upsert_row(conn: Connection, table_name: str, data: dict[str, Any]) -> None:
    meta = TABLE_MAP[table_name]
    row: dict[str, Any] = {}
    for c in meta["columns"]:
        if c in data:
            row[c] = data[c]
        else:
            lower_map = {str(k).lower(): k for k in data}
            src = lower_map.get(c.lower())
            if src is not None:
                row[c] = data[src]
    if meta["pk"] not in row:
        raise ValueError(f"Missing PK {meta['pk']} for {table_name}")

    mysql_row = {
        _local_to_mysql_col(table_name, c): _coerce_mysql_value(table_name, c, row[c])
        for c in row
    }
    pk_mysql = _local_to_mysql_col(table_name, meta["pk"])
    col_names = list(mysql_row.keys())
    placeholders = ", ".join(["%s"] * len(col_names))
    col_sql = ", ".join(_qi(c) for c in col_names)
    updates = ", ".join(
        f"{_qi(c)}=VALUES({_qi(c)})" for c in col_names if c != pk_mysql
    )
    if not updates:
        updates = f"{_qi(pk_mysql)}={_qi(pk_mysql)}"
    sql = (
        f"INSERT INTO {_qi(table_name)} ({col_sql}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )
    with conn.cursor() as cur:
        cur.execute(sql, [mysql_row[c] for c in col_names])


def delete_row(conn: Connection, table_name: str, pk_value: Any) -> None:
    meta = TABLE_MAP[table_name]
    pk_mysql = _local_to_mysql_col(table_name, meta["pk"])
    sql = f"DELETE FROM {_qi(table_name)} WHERE {_qi(pk_mysql)} = %s"
    with conn.cursor() as cur:
        cur.execute(sql, (pk_value,))


def fetch_rows_after_pk(
    conn: Connection, table_name: str, after_id: int, limit: int
) -> list[dict[str, Any]]:
    meta = TABLE_MAP[table_name]
    pk = _local_to_mysql_col(table_name, meta["pk"])
    sql = (
        f"SELECT * FROM {_qi(table_name)} "
        f"WHERE {_qi(pk)} > %s "
        f"ORDER BY {_qi(pk)} ASC "
        f"LIMIT %s"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (after_id, limit))
        return [_mysql_to_local_row(table_name, dict(r)) for r in cur.fetchall()]


def fetch_all_rows(conn: Connection, table_name: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {_qi(table_name)}")
        return [_mysql_to_local_row(table_name, dict(r)) for r in cur.fetchall()]


def fetch_sync_changes(
    conn: Connection, after_change_id: int, limit: int
) -> list[dict[str, Any]]:
    sql = (
        "SELECT change_id, table_name, record_uuid, operation, data, changed_at "
        "FROM `sync_changes` "
        "WHERE change_id > %s "
        "ORDER BY change_id ASC "
        "LIMIT %s"
    )
    with conn.cursor() as cur:
        cur.execute(sql, (after_change_id, limit))
        return list(cur.fetchall())


def max_pk(conn: Connection, table_name: str) -> int:
    meta = TABLE_MAP[table_name]
    pk = _local_to_mysql_col(table_name, meta["pk"])
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX({_qi(pk)}) AS m FROM {_qi(table_name)}")
        row = cur.fetchone()
        if not row or row["m"] is None:
            return 0
        try:
            return int(row["m"])
        except (TypeError, ValueError):
            return 0
