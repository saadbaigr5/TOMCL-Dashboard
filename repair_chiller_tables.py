"""Migrate rooms into DB_Tomcl.db and create missing metric tables.

Close DB Browser on DB_Tomcl.db before running.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "tomcl_python"))

from chiller_rooms import (  # noqa: E402
    DB_PATH,
    LEGACY_REGISTRY_PATH,
    ensure_all_room_tables,
    ensure_schema,
    list_chiller_rooms,
)

if __name__ == "__main__":
    print(f"Database: {DB_PATH}")
    print("Migrating / ensuring schema in DB_Tomcl.db only ...")
    ensure_schema()
    if LEGACY_REGISTRY_PATH.exists():
        print(f"NOTE: legacy file still present: {LEGACY_REGISTRY_PATH}")
    else:
        print("Legacy chiller_registry.db removed / not present.")
    rooms = list_chiller_rooms()
    print(f"Rooms in DB_Tomcl.db: {len(rooms)}")
    for room in rooms:
        print(f"  - {room['name']} -> {room['table_prefix']}_temp/_humidity/_door_status/_defrost")
    print("Creating missing tables ...")
    created = ensure_all_room_tables()
    for name in sorted(set(created)):
        print(f"  ensured {name}")
    print("Done. Refresh DB Browser on DB_Tomcl.db")
