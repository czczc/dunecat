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

from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()  # pull DUNECAT_HUB_SECRET_KEY etc. from .env before init

from . import crypto, db  # noqa: E402
from .auth import session as session_mod  # noqa: E402
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


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
