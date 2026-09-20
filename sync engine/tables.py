"""Synced business tables and direction rules."""

from __future__ import annotations

from typing import Any

# direction: "both" | "push" (SQLite -> Hostinger only) | "pull"
TABLE_MAP: dict[str, dict[str, Any]] = {
    "chiller_rooms": {
        # Both ways: local dashboard <-> Hostinger web.
        # Dedicated sync mirrors creates/deletes; subtables live on local SQLite only.
        "direction": "both",
        "pk": "id",
        "columns": [
            "id",
            "name",
            "table_prefix",
            "created_at",
            "capacity",
            "Filled_QTY",
            "chiller_IP",
            "chiller_active",
        ],
        "mysql_types": {
            "id": "BIGINT",
            "name": "VARCHAR(255)",
            "table_prefix": "VARCHAR(255)",
            "created_at": "VARCHAR(64)",
            "capacity": "VARCHAR(64)",
            "Filled_QTY": "TEXT",
            "chiller_IP": "VARCHAR(64)",
            "chiller_active": "VARCHAR(32)",
        },
        # Upsert local→Hostinger every cycle; do NOT delete Hostinger-only rows here
        # (those are web creates — handled by pull mirror).
        "reconcile": True,
        "custom_pull": True,
    },
    "destinations": {
        "direction": "both",
        "pk": "destination",
        "columns": ["destination"],
        "mysql_types": {"destination": "VARCHAR(100)"},
    },
    "orders": {
        "direction": "both",
        "pk": "id",
        "columns": [
            "id",
            "order_id",
            "time_stamp",
            "party_name",
            "consignee_name",
            "time_stamp_stored",
            "dispatched_date",
            "status",
            "product_type",
            "pcs_type",
            "organs_type",
            "packing",
            "chiller_room",
            "QTY",
            "destination",
            "actual_dispatched_date",
        ],
        "mysql_types": {
            "id": "BIGINT",
            "order_id": "VARCHAR(64)",
            "time_stamp": "DATETIME",
            "party_name": "TEXT",
            "consignee_name": "TEXT",
            "time_stamp_stored": "DATETIME",
            "dispatched_date": "DATE",
            "status": "VARCHAR(64)",
            "product_type": "TEXT",
            "pcs_type": "TEXT",
            "organs_type": "TEXT",
            "packing": "TEXT",
            "chiller_room": "TEXT",
            "QTY": "TEXT",
            "destination": "TEXT",
            "actual_dispatched_date": "TEXT",
        },
        # Local SQLite may use qty vs QTY — normalized when reading rows.
        "sqlite_aliases": {"QTY": ["QTY", "qty"]},
    },
    "packing_types": {
        "direction": "both",
        "pk": "id",
        "columns": ["id", "packing"],
        "mysql_types": {"id": "BIGINT", "packing": "TEXT"},
    },
    "party_consignee": {
        "direction": "both",
        "pk": "id",
        "columns": ["id", "party_name", "consignee_name"],
        "mysql_types": {
            "id": "BIGINT",
            "party_name": "TEXT",
            "consignee_name": "TEXT",
        },
    },
    "pcs_types": {
        "direction": "both",
        "pk": "id",
        "columns": ["id", "pcs_type"],
        "mysql_types": {"id": "BIGINT", "pcs_type": "TEXT"},
    },
    "products": {
        "direction": "both",
        "pk": "id",
        "columns": ["id", "product_name"],
        "mysql_types": {"id": "BIGINT", "product_name": "TEXT"},
    },
    "raw_chiller_data": {
        "direction": "push",
        "pk": "id",
        "columns": [
            "id",
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
            "chiller_id",
        ],
        "mysql_types": {
            "id": "BIGINT",
            "timestamp": "VARCHAR(64)",
            "chiller_status": "TEXT",
            "defrost_active": "INT",
            "defrost_start": "VARCHAR(64)",
            "defrost_end": "VARCHAR(64)",
            "door_status": "TEXT",
            "door_unlocked_at": "VARCHAR(64)",
            "door_unlocked_seconds": "INT",
            "temperature_c": "DOUBLE",
            "humidity_percent": "DOUBLE",
            "temperature_sensor_connected": "INT",
            "humidity_sensor_connected": "INT",
            # Local SQLite stores room names ("Chiller 1"), not numeric ids.
            "chiller_id": "VARCHAR(64)",
        },
        # High volume: watermark push, not per-row sync_queue.
        "use_queue": False,
    },
    "users": {
        "direction": "both",
        "pk": "user_ID",
        "columns": ["user_ID", "User_name", "Department", "Password", "Name"],
        "mysql_types": {
            "user_ID": "BIGINT",
            "User_name": "VARCHAR(100)",
            "Department": "VARCHAR(100)",
            "Password": "VARCHAR(255)",
            "Name": "TEXT",
        },
        # Hostinger schema has historical typo "Pasword".
        "mysql_column_map": {"Password": "Pasword"},
    },
}


def both_way_tables() -> list[str]:
    return [n for n, m in TABLE_MAP.items() if m["direction"] == "both"]


def push_tables() -> list[str]:
    return [n for n, m in TABLE_MAP.items() if m["direction"] in ("both", "push")]


def pull_tables() -> list[str]:
    return [n for n, m in TABLE_MAP.items() if m["direction"] in ("both", "pull")]


def record_uuid(table_name: str, pk_value: Any) -> str:
    return f"{table_name}:{pk_value}"
