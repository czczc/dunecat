"""FastAPI app entrypoint for the multi-user hub.

Lifespan:
  * load .env (so DUNECAT_HUB_SECRET_KEY / DB path are visible)
  * initialise crypto (fatal if key missing/bad — fail-fast at startup)
  * create the schema
  * spawn a periodic GC task for expired sessions & device-flows

Routes are tiny:
  GET  /hub/login           render the device-flow page
  GET  /hub/login/poll      poll vault, set the cookie on success
  POST /hub/logout          drop the session
  GET  /api/me              identity for the cookie holder
  GET  /health              liveness, unauthenticated
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

load_dotenv()  # pull DUNECAT_HUB_SECRET_KEY etc. from .env before init

from . import crypto, db  # noqa: E402
from .auth import session as session_mod  # noqa: E402
from .routes.catalog import router as catalog_router  # noqa: E402
from .routes.config import router as config_router  # noqa: E402
from .routes.detectors import router as detectors_router  # noqa: E402
from .routes.login import router as login_router  # noqa: E402
from .routes.me import router as me_router  # noqa: E402

log = logging.getLogger("uvicorn.error")

_GC_INTERVAL_SECONDS = 300


async def _gc_loop() -> None:
    while True:
        try:
            await asyncio.sleep(_GC_INTERVAL_SECONDS)
            with db.connect() as conn:
                session_mod.gc_expired(conn)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # never let the GC kill the task
            log.warning("hub: gc tick failed: %s", e)


@asynccontextmanager
async def lifespan(_: FastAPI):
    crypto.init_from_env()  # fatal if missing — fail fast
    db.init_schema()
    log.info("hub: starting (db=%s)", db.db_path())
    gc = asyncio.create_task(_gc_loop())
    try:
        yield
    finally:
        gc.cancel()
        try:
            await gc
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="dunecat-hub",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

app.include_router(login_router)
app.include_router(me_router)
app.include_router(config_router)
app.include_router(detectors_router)
app.include_router(catalog_router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


# Static SPA bundle. Each known asset prefix is mounted explicitly
# (not the whole bundle at `/`) so a request to `/api/nonexistent`
# still gets a JSON 404 instead of silently being served `index.html`.
# A catch-all at the bottom handles SPA history-mode routes
# (`/datasets`, `/files/x:y`, etc.) by returning `index.html`.
_SPA_DIR = Path(__file__).resolve().parents[2] / "frontend" / "dist"
_SPA_INDEX = _SPA_DIR / "index.html"

if (_SPA_DIR / "assets").is_dir():
    app.mount(
        "/assets",
        StaticFiles(directory=_SPA_DIR / "assets"),
        name="spa-assets",
    )
if (_SPA_DIR / "logo").is_dir():
    app.mount(
        "/logo",
        StaticFiles(directory=_SPA_DIR / "logo"),
        name="spa-logo",
    )

if _SPA_INDEX.exists():
    log.info("hub: serving SPA bundle from %s", _SPA_DIR)

    @app.get("/{full_path:path}", include_in_schema=False)
    def spa_fallback(full_path: str) -> FileResponse:
        # Refuse paths under the API namespaces — those should have
        # been claimed by their registered routes above. If we got
        # here, the client asked for an endpoint that doesn't exist.
        if full_path.startswith(("api/", "hub/")):
            raise HTTPException(status_code=404, detail="Not Found")
        return FileResponse(_SPA_INDEX)
else:
    log.info(
        "hub: %s missing — SPA bundle not served. Run "
        "`npm --prefix frontend run build` to enable.",
        _SPA_INDEX,
    )
