import os
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from metacat.webapi import AuthenticationError, MCWebAPIError

from .detectors import datasets_for_detector, load_detectors

app = FastAPI(title="dunecat", docs_url="/api/docs", openapi_url="/api/openapi.json")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)


@app.exception_handler(AuthenticationError)
async def _auth_error(_: Request, exc: AuthenticationError) -> JSONResponse:
    server = os.environ.get("METACAT_SERVER_URL", "<METACAT_SERVER_URL>")
    auth = os.environ.get("METACAT_AUTH_SERVER_URL", "<METACAT_AUTH_SERVER_URL>")
    return JSONResponse(
        status_code=401,
        content={
            "detail": (
                f"Token missing or expired ({exc}). Run: "
                f"metacat -s {server} -a {auth} auth login -m <method> <username>"
            )
        },
    )


@app.exception_handler(MCWebAPIError)
async def _metacat_error(_: Request, exc: MCWebAPIError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.get("/api/detectors")
def list_detectors() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for entry in load_detectors():
        datasets = datasets_for_detector(entry["namespaces"])
        out.append(
            {
                "id": entry["id"],
                "name": entry["name"],
                "namespaces": entry["namespaces"],
                "datasets_count": len(datasets),
                "files_count": sum(ds.get("file_count") or 0 for ds in datasets),
            }
        )
    return out
