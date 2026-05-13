# -*- coding: utf-8 -*-
"""
Multi-user manager: encrypted API key storage + CRUD.
Each user's config is stored as an encrypted JSON file under data/{user_id}/config.json.
"""
import json
import os
import secrets
import shutil
import string
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _get_fernet() -> Fernet:
    key = os.getenv("MASTER_KEY", "")
    if not key:
        key_file = DATA_DIR / ".master_key"
        if key_file.exists():
            key = key_file.read_text().strip()
        else:
            key = Fernet.generate_key().decode()
            key_file.write_text(key)
    return Fernet(key.encode() if isinstance(key, str) else key)


def _user_dir(user_id: str) -> Path:
    return DATA_DIR / user_id


def _config_path(user_id: str) -> Path:
    return _user_dir(user_id) / "config.json"


def _gen_invite_code(length: int = 8) -> str:
    """Generate a random alphanumeric invite code (uppercase)."""
    chars = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(chars) for _ in range(length))


def create_slot(
    user_id: str,
    source_ip: str,
    port: int,
    public_ip: str = "",
) -> dict:
    """Admin creates a slot (no API key yet). User fills keys later."""
    f = _get_fernet()
    user_dir = _user_dir(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "user_id": user_id,
        "api_key": "",
        "secret_key": "",
        "source_ip": source_ip,
        "public_ip": public_ip,
        "port": port,
        "order_qty": "0.001",
        "invite_code": _gen_invite_code(),
    }
    encrypted = f.encrypt(json.dumps(config).encode())
    _config_path(user_id).write_bytes(encrypted)
    return config



def create_user(
    user_id: str,
    api_key: str,
    secret_key: str,
    source_ip: str,
    port: int,
) -> dict:
    """Create or update a user's encrypted config (full, admin use)."""
    f = _get_fernet()
    user_dir = _user_dir(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "user_id": user_id,
        "api_key": api_key,
        "secret_key": secret_key,
        "source_ip": source_ip,
        "port": port,
    }
    encrypted = f.encrypt(json.dumps(config).encode())
    _config_path(user_id).write_bytes(encrypted)
    return config


def get_user(user_id: str) -> Optional[dict]:
    """Read and decrypt a user's config. Returns None if not found."""
    path = _config_path(user_id)
    if not path.exists():
        return None
    f = _get_fernet()
    decrypted = f.decrypt(path.read_bytes())
    return json.loads(decrypted)


def list_users() -> list[dict]:
    """List all users (decrypted configs, without secret_key)."""
    users = []
    if not DATA_DIR.exists():
        return users
    for d in sorted(DATA_DIR.iterdir()):
        if d.is_dir() and (d / "config.json").exists():
            cfg = get_user(d.name)
            if cfg:
                safe = {k: v for k, v in cfg.items() if k != "secret_key"}
                has_key = bool(cfg.get("api_key", ""))
                safe["api_key"] = (cfg["api_key"][:8] + "...") if has_key else ""
                safe["key_configured"] = has_key
                safe["invite_code"] = cfg.get("invite_code", "")
                safe["bot_mode"] = "manual"
                users.append(safe)
    return users


def get_user_by_invite(code: str) -> Optional[dict]:
    """Look up a user by invite code. Returns config or None."""
    if not code or not DATA_DIR.exists():
        return None
    code_upper = code.strip().upper()
    for d in DATA_DIR.iterdir():
        if d.is_dir() and (d / "config.json").exists():
            cfg = get_user(d.name)
            if cfg and cfg.get("invite_code", "").upper() == code_upper:
                return cfg
    return None


def ensure_invite_codes():
    """Backfill invite_code for existing users that don't have one."""
    if not DATA_DIR.exists():
        return
    f = _get_fernet()
    for d in DATA_DIR.iterdir():
        if d.is_dir() and (d / "config.json").exists():
            cfg = get_user(d.name)
            if cfg and not cfg.get("invite_code"):
                cfg["invite_code"] = _gen_invite_code()
                encrypted = f.encrypt(json.dumps(cfg).encode())
                _config_path(d.name).write_bytes(encrypted)


def delete_user(user_id: str) -> bool:
    """Delete a user's data directory."""
    user_dir = _user_dir(user_id)
    if user_dir.exists():
        shutil.rmtree(user_dir)
        return True
    return False
