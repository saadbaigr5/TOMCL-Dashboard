"""Load MySQL / sync settings from secrets/mysql_sync.env (gitignored)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
SECRETS_ENV = PACKAGE_ROOT / "secrets" / "mysql_sync.env"
DB_PATH = PACKAGE_ROOT / "DB_TOMCL" / "DB_Tomcl.db"


def _parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def sqlite_path() -> Path:
    """Local DB path — safe for enqueue without MySQL credentials."""
    return DB_PATH


def load_config(*, require_mysql: bool = True) -> dict[str, Any]:
    file_vals = _parse_env_file(SECRETS_ENV)

    def get(key: str, default: str = "") -> str:
        return (os.environ.get(key) or file_vals.get(key) or default).strip()

    password = get("MYSQL_PASSWORD")
    if require_mysql and not password:
        raise RuntimeError(
            f"Missing MYSQL_PASSWORD. Copy secrets/mysql_sync.env.example to "
            f"{SECRETS_ENV} and fill in Hostinger credentials."
        )

    return {
        "mysql_host": get("MYSQL_HOST", "127.0.0.1"),
        "mysql_port": int(get("MYSQL_PORT", "3306") or "3306"),
        "mysql_database": get("MYSQL_DATABASE"),
        "mysql_user": get("MYSQL_USER"),
        "mysql_password": password,
        "interval_seconds": int(get("SYNC_INTERVAL_SECONDS", "5") or "5"),
        "batch_size": int(get("SYNC_BATCH_SIZE", "100") or "100"),
        "raw_batch_size": int(get("RAW_PUSH_BATCH_SIZE", "500") or "500"),
        "sqlite_path": DB_PATH,
    }
