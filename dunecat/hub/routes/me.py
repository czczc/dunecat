"""GET /api/me — the canonical "who am I" endpoint."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from .. import db
from ..auth.dep import current_user
from ..auth.session import User

router = APIRouter()


@router.get("/api/me")
def get_me(user: User = Depends(current_user)) -> dict:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT expires_at FROM vault_tokens WHERE user_id = ?",
            (user.id,),
        ).fetchone()
    return {
        "user_id": user.id,
        "oidc_sub": user.oidc_sub,
        "metacat_username": user.metacat_username,
        "vault_token_expires_at": row["expires_at"] if row else None,
    }
