"""AES-GCM encryption for per-user vault tokens.

Threat model: an attacker who exfiltrates the SQLite DB but not the
key cannot decrypt vault tokens. Key precedence at startup:

  1. ``DUNECAT_HUB_SECRET_KEY`` env (base64-encoded 32 bytes) — the
     production path; ops sets this via systemd EnvironmentFile or
     similar.
  2. ``~/.dunecat/hub.key`` (derived from the hub DB's parent dir) —
     loaded if present; this is what a returning dev sees.
  3. Neither — generate a fresh key, write it with mode 0600 to the
     same path, and log a warning. **Dev convenience only.** In prod
     the env var should always be set so this branch never runs.

Malformed env values and unreadable key files are fatal; only
"nothing configured at all" triggers auto-generation.
"""

from __future__ import annotations

import base64
import logging
import os
import secrets
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from . import db

log = logging.getLogger("uvicorn.error")


class HubCryptoError(RuntimeError):
    pass


_aesgcm: AESGCM | None = None


def _key_path() -> Path:
    """Key file lives next to the DB by default. Override with
    ``DUNECAT_HUB_SECRET_KEY_FILE`` if you want it elsewhere."""
    raw = os.environ.get("DUNECAT_HUB_SECRET_KEY_FILE")
    if raw:
        return Path(raw).expanduser()
    return db.db_path().parent / "hub.key"


def _load_key_bytes() -> bytes:
    raw = os.environ.get("DUNECAT_HUB_SECRET_KEY")
    if raw:
        try:
            key = base64.b64decode(raw)
        except Exception as e:
            raise HubCryptoError(
                f"DUNECAT_HUB_SECRET_KEY is not valid base64: {e}"
            )
        if len(key) != 32:
            raise HubCryptoError(
                f"DUNECAT_HUB_SECRET_KEY must decode to 32 bytes; got {len(key)}"
            )
        log.info("hub: crypto key loaded from DUNECAT_HUB_SECRET_KEY env")
        return key

    path = _key_path()
    if path.exists():
        try:
            key = base64.b64decode(path.read_text().strip())
        except Exception as e:
            raise HubCryptoError(
                f"crypto key at {path} is not valid base64: {e}"
            )
        if len(key) != 32:
            raise HubCryptoError(
                f"crypto key at {path} must decode to 32 bytes; got {len(key)}"
            )
        log.info("hub: crypto key loaded from %s", path)
        return key

    # Auto-generate. Dev convenience — never executes in prod (env wins).
    path.parent.mkdir(parents=True, exist_ok=True)
    key = secrets.token_bytes(32)
    path.write_text(base64.b64encode(key).decode() + "\n")
    path.chmod(0o600)
    log.warning(
        "hub: no DUNECAT_HUB_SECRET_KEY set; generated a new key at %s. "
        "In production, set DUNECAT_HUB_SECRET_KEY via systemd "
        "EnvironmentFile (in a different location from the DB) instead.",
        path,
    )
    return key


def init_from_env() -> None:
    """Resolve the AES-GCM key (env → file → auto-generate) and install
    it as the process-wide cipher. Idempotent within a process; safe to
    call once at startup. Raises ``HubCryptoError`` on malformed
    configuration."""
    global _aesgcm
    key = _load_key_bytes()
    _aesgcm = AESGCM(key)


def encrypt(plaintext: bytes) -> tuple[bytes, bytes]:
    if _aesgcm is None:
        raise HubCryptoError("crypto not initialised; call init_from_env() first")
    nonce = secrets.token_bytes(12)
    return _aesgcm.encrypt(nonce, plaintext, None), nonce


def decrypt(ciphertext: bytes, nonce: bytes) -> bytes:
    if _aesgcm is None:
        raise HubCryptoError("crypto not initialised; call init_from_env() first")
    return _aesgcm.decrypt(nonce, ciphertext, None)
