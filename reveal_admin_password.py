"""Reveal the admin password using the external Fernet key (for recovery only)."""

from __future__ import annotations

from pathlib import Path

from cryptography.fernet import Fernet

ROOT = Path(__file__).resolve().parent
KEY_PATH = ROOT / "secrets" / "fernet.key"
ENC_PATH = ROOT / "secrets" / "admin_password.enc"


def main() -> None:
    key = KEY_PATH.read_bytes().strip()
    token = ENC_PATH.read_bytes().strip()
    password = Fernet(key).decrypt(token).decode("utf-8")
    print("Admin password:")
    print(password)


if __name__ == "__main__":
    main()
