"""Login / logout routes for the hub.

GET  /hub/login           — start a device flow, render the polling page
GET  /hub/login/poll      — poll vault, on success set the session cookie
POST /hub/logout          — drop the session row, clear the cookie

The polling page is plain HTML with a small inline ``setInterval``.
The SPA is *not* needed for this PoC; it consumes the cookie via the
existing fetch path once login is done.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Cookie, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from .. import crypto, db
from ..auth import flow as flow_mod
from ..auth import session as session_mod
from ..auth.session import COOKIE_NAME, DEVICE_FLOW_LIFETIME

log = logging.getLogger("uvicorn.error")
router = APIRouter()


_LOGIN_HTML = """\
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Sign in to dunecat</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            max-width: 38rem; margin: 4rem auto; padding: 0 1rem;
            color: #1f2328; line-height: 1.5; }}
    h1 {{ font-size: 1.4rem; }}
    a.cilogon {{ display: inline-block; padding: 0.6rem 1rem;
                 background: #0969da; color: white; text-decoration: none;
                 border-radius: 6px; font-weight: 600; cursor: pointer; }}
    code {{ background: #f6f8fa; padding: 0.1rem 0.3rem; border-radius: 3px;
            font-size: 0.95em; }}
    .status {{ margin-top: 2rem; padding: 1rem; background: #f6f8fa;
               border-radius: 6px; font-family: monospace; }}
    .ok    {{ color: #1a7f37; }}
    .err   {{ color: #cf222e; }}
    .hint  {{ color: #57606a; font-size: 0.9em; }}
  </style>
</head>
<body>
  <h1>Sign in to dunecat</h1>
  <p>1. Click the button to open CILogon <strong>in a new tab</strong> and complete the DUNE/Fermilab login.</p>
  <p>
    <a id="cilogon-link" class="cilogon" href="{auth_url}"
       target="_blank" rel="noopener noreferrer">
      Continue with CILogon &rarr;
    </a>
  </p>
  <p class="hint">User code (already filled into the URL): <code>{user_code}</code></p>
  <p>2. Keep this tab open. It will redirect you home once sign-in completes.</p>
  <div class="status" id="status">Waiting for CILogon...</div>
  <script>
    // Belt-and-suspenders: an explicit window.open inside a click handler
    // is honoured even when popup-blocker heuristics ignore target="_blank".
    document.getElementById("cilogon-link").addEventListener("click", (e) => {{
      e.preventDefault();
      window.open(e.currentTarget.href, "_blank", "noopener,noreferrer");
    }});

    const flowId = {flow_id_json};
    const statusEl = document.getElementById("status");
    async function tick() {{
      try {{
        const r = await fetch("/hub/login/poll?flow_id=" + encodeURIComponent(flowId),
                              {{credentials: "same-origin"}});
        const data = await r.json();
        if (data.status === "ok") {{
          statusEl.textContent = "Signed in. Redirecting...";
          statusEl.className = "status ok";
          window.location = "/";
          return;
        }}
        if (data.status === "pending") {{
          statusEl.textContent = "Waiting for CILogon...";
          setTimeout(tick, 3000);
          return;
        }}
        statusEl.textContent = "Error: " + (data.detail || "unknown");
        statusEl.className = "status err";
      }} catch (e) {{
        statusEl.textContent = "Network error; retrying...";
        setTimeout(tick, 3000);
      }}
    }}
    setTimeout(tick, 3000);
  </script>
</body>
</html>
"""


_HOME_HTML = """\
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>dunecat</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif;
            max-width: 38rem; margin: 4rem auto; padding: 0 1rem;
            color: #1f2328; line-height: 1.5; }}
    h1 {{ font-size: 1.4rem; }}
    code {{ background: #f6f8fa; padding: 0.1rem 0.3rem; border-radius: 3px;
            font-size: 0.95em; }}
    button {{ padding: 0.4rem 0.9rem; border: 1px solid #d0d7de;
              background: #f6f8fa; border-radius: 6px; cursor: pointer; }}
    button:hover {{ background: #eaeef2; }}
    .hint {{ color: #57606a; font-size: 0.9em; }}
  </style>
</head>
<body>
  <h1>dunecat</h1>
  <p>Signed in as <strong>{username}</strong>.</p>
  <p class="hint">Vault token expires: <code>{vault_expires_at}</code></p>
  <p><button id="logout">Sign out</button></p>
  <hr>
  <p class="hint">
    This is the dunecat-hub minimum-viable PoC (issue #26). The full
    catalog UI is a follow-up issue. Try
    <a href="/api/me">/api/me</a> for your raw identity JSON.
  </p>
  <script>
    document.getElementById("logout").addEventListener("click", async () => {{
      await fetch("/hub/logout", {{
        method: "POST", credentials: "same-origin",
      }});
      window.location = "/hub/login";
    }});
  </script>
</body>
</html>
"""


def _cookie_secure_for(request: Request) -> bool:
    """Mark the session cookie Secure only when the request itself came
    over HTTPS (works correctly behind a reverse proxy that sets
    X-Forwarded-Proto, which uvicorn's --proxy-headers respects)."""
    return request.url.scheme == "https"


def _new_flow_id() -> str:
    import secrets

    return secrets.token_urlsafe(24)


@router.get("/hub/login", response_class=HTMLResponse)
def login_page() -> HTMLResponse:
    start = flow_mod.start()
    flow_id = _new_flow_id()
    expires = (datetime.now(UTC) + DEVICE_FLOW_LIFETIME).isoformat()
    with db.connect() as conn:
        conn.execute(
            "INSERT INTO device_flows (id, poll_body, expires_at, status) "
            "VALUES (?, ?, ?, 'pending')",
            (flow_id, json.dumps(start.poll_body), expires),
        )
    html = _LOGIN_HTML.format(
        auth_url=start.auth_url,
        user_code=start.user_code or "(see URL)",
        flow_id_json=json.dumps(flow_id),
    )
    return HTMLResponse(html)


@router.get("/hub/login/poll")
def login_poll(flow_id: str, request: Request) -> Response:
    with db.connect() as conn:
        row = conn.execute(
            "SELECT poll_body, expires_at, status FROM device_flows WHERE id = ?",
            (flow_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="unknown flow_id")
    if row["status"] == "complete":
        # Idempotent: if the browser re-polls after success, just say ok again.
        return JSONResponse({"status": "ok"})
    if row["expires_at"] <= datetime.now(UTC).isoformat():
        return JSONResponse(
            {"status": "error", "detail": "flow expired; reload to start again"},
            status_code=410,
        )
    poll_body = json.loads(row["poll_body"])
    try:
        result = flow_mod.poll(poll_body)
    except Exception as e:
        log.warning("hub: vault poll failed: %s", e)
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=502)

    if result.outcome in ("pending", "slow_down"):
        return JSONResponse({"status": "pending"})

    # outcome == "complete"
    try:
        login = flow_mod.complete(result.auth or {})
    except Exception as e:
        log.warning("hub: completing login failed: %s", e)
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=502)

    # Persist the user, vault token, and session, atomically.
    ciphertext, nonce = crypto.encrypt(login.vault_token.encode())
    vault_expires = (
        datetime.now(UTC) + timedelta(seconds=login.vault_lease_seconds)
    ).isoformat()
    now = datetime.now(UTC).isoformat()
    with db.connect() as conn:
        conn.execute("BEGIN")
        try:
            user_id = session_mod.upsert_user(
                conn,
                oidc_sub=login.oidc_sub,
                metacat_username=login.metacat_username,
            )
            conn.execute(
                "INSERT OR REPLACE INTO vault_tokens "
                "(user_id, ciphertext, nonce, expires_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (user_id, ciphertext, nonce, vault_expires, now),
            )
            session_id = session_mod.create_session(conn, user_id=user_id)
            conn.execute(
                "UPDATE device_flows SET status = 'complete' WHERE id = ?",
                (flow_id,),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    resp = JSONResponse({"status": "ok"})
    resp.set_cookie(
        key=COOKIE_NAME,
        value=session_id,
        httponly=True,
        secure=_cookie_secure_for(request),
        samesite="lax",
        path="/",
        max_age=int(session_mod.SESSION_LIFETIME.total_seconds()),
    )
    return resp


@router.post("/hub/logout")
def logout(
    request: Request,
    dunecat_session: str | None = Cookie(default=None, alias=COOKIE_NAME),
) -> Response:
    if dunecat_session:
        with db.connect() as conn:
            session_mod.delete_session(conn, dunecat_session)
    resp = JSONResponse({"status": "ok"})
    resp.delete_cookie(key=COOKIE_NAME, path="/")
    return resp


@router.get("/", response_class=HTMLResponse)
def home(
    dunecat_session: str | None = Cookie(default=None, alias=COOKIE_NAME),
) -> Response:
    """Minimal home page. If signed in, render an identity + logout
    page. Otherwise, redirect to /hub/login. The full SPA is a
    separate follow-up issue."""
    if not dunecat_session:
        return RedirectResponse(url="/hub/login", status_code=303)
    with db.connect() as conn:
        user = session_mod.load_session(conn, dunecat_session)
        if user is None:
            return RedirectResponse(url="/hub/login", status_code=303)
        row = conn.execute(
            "SELECT expires_at FROM vault_tokens WHERE user_id = ?",
            (user.id,),
        ).fetchone()
    return HTMLResponse(
        _HOME_HTML.format(
            username=user.metacat_username,
            vault_expires_at=row["expires_at"] if row else "—",
        )
    )
