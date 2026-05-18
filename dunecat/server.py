"""Background process manager for the backend (uvicorn) and frontend (Vite).

Two-terminal workflows are annoying to keep alive across reboots and tab
shuffles. This module exposes ``dunecat server {start,stop,restart,status,logs}``
which manages both processes via PID files in ``~/.dunecat/run`` and log
files in ``~/.dunecat/log``.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import typer

server_app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="Manage backend (uvicorn) and frontend (Vite) servers in the background.",
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUN_DIR = Path.home() / ".dunecat" / "run"
LOG_DIR = Path.home() / ".dunecat" / "log"

BACKEND_PORT_DEFAULT = 8000
FRONTEND_PORT_DEFAULT = 5173

_NAMES = ("backend", "frontend")


def _pid_file(name: str) -> Path:
    return RUN_DIR / f"{name}.pid"


def _log_file(name: str) -> Path:
    return LOG_DIR / f"{name}.log"


def _read_pid(name: str) -> int | None:
    f = _pid_file(name)
    if not f.exists():
        return None
    try:
        return int(f.read_text().strip())
    except ValueError:
        return None


def _is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, just not ours
    return True


def _find_js_runner() -> str:
    for cmd in ("npm", "bun"):
        if shutil.which(cmd):
            return cmd
    raise RuntimeError(
        "No JavaScript package manager found. "
        "Install one of: npm (Node.js) or bun."
    )


def _spawn_cmd(name: str, port: int) -> tuple[list[str], Path]:
    if name == "backend":
        return (
            ["uv", "run", "uvicorn", "dunecat.web:app", "--port", str(port)],
            PROJECT_ROOT,
        )
    if name == "frontend":
        js = _find_js_runner()
        return (
            [js, "run", "dev", "--", "--port", str(port), "--host", "127.0.0.1"],
            PROJECT_ROOT / "frontend",
        )
    raise ValueError(name)


def _start_one(name: str, port: int) -> None:
    existing = _read_pid(name)
    if existing is not None and _is_alive(existing):
        typer.echo(f"{name}: already running (pid {existing})")
        return
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    cmd, cwd = _spawn_cmd(name, port)
    log = _log_file(name)
    log_fp = open(log, "ab", buffering=0)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            stdout=log_fp,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    finally:
        log_fp.close()
    _pid_file(name).write_text(str(proc.pid))
    # Sanity check: did it survive the first half-second?
    time.sleep(0.5)
    if not _is_alive(proc.pid):
        _pid_file(name).unlink(missing_ok=True)
        typer.echo(
            f"{name}: exited immediately. Check logs: {log}", err=True
        )
        return
    typer.echo(f"{name}: started (pid {proc.pid}, port {port}, logs {log})")


def _stop_one(name: str) -> None:
    pid = _read_pid(name)
    if pid is None:
        typer.echo(f"{name}: not running")
        return
    if not _is_alive(pid):
        _pid_file(name).unlink(missing_ok=True)
        typer.echo(f"{name}: stale pidfile cleared")
        return
    try:
        pgid = os.getpgid(pid)
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        _pid_file(name).unlink(missing_ok=True)
        typer.echo(f"{name}: gone")
        return
    for _ in range(50):
        if not _is_alive(pid):
            break
        time.sleep(0.1)
    if _is_alive(pid):
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    _pid_file(name).unlink(missing_ok=True)
    typer.echo(f"{name}: stopped (pid {pid})")


def _status_one(name: str, ports: dict[str, int]) -> None:
    pid = _read_pid(name)
    port = ports[name]
    if pid is None:
        typer.echo(f"{name}: stopped")
        return
    if not _is_alive(pid):
        typer.echo(f"{name}: stale pidfile (pid {pid} not running)")
        return
    typer.echo(
        f"{name}: running (pid {pid}, port {port}, logs {_log_file(name)})"
    )


def _resolve_targets(service: str | None) -> list[str]:
    if service is None:
        return list(_NAMES)
    if service not in _NAMES:
        typer.echo(
            f"Unknown service '{service}'. Use 'backend', 'frontend', or omit.",
            err=True,
        )
        raise typer.Exit(2)
    return [service]


@server_app.command("start")
def server_start(
    service: str | None = typer.Argument(
        None, help="'backend', 'frontend', or omit for both."
    ),
    backend_port: int = typer.Option(BACKEND_PORT_DEFAULT, "--backend-port"),
    frontend_port: int = typer.Option(FRONTEND_PORT_DEFAULT, "--frontend-port"),
) -> None:
    """Start backend and/or frontend in the background."""
    ports = {"backend": backend_port, "frontend": frontend_port}
    for name in _resolve_targets(service):
        _start_one(name, ports[name])


@server_app.command("stop")
def server_stop(
    service: str | None = typer.Argument(
        None, help="'backend', 'frontend', or omit for both."
    ),
) -> None:
    """Stop backend and/or frontend (SIGTERM, then SIGKILL after 5 s)."""
    # Stop frontend first so the browser-side reconnect storm dies before
    # the API does.
    targets = _resolve_targets(service)
    for name in ("frontend", "backend"):
        if name in targets:
            _stop_one(name)


@server_app.command("restart")
def server_restart(
    service: str | None = typer.Argument(
        None, help="'backend', 'frontend', or omit for both."
    ),
    backend_port: int = typer.Option(BACKEND_PORT_DEFAULT, "--backend-port"),
    frontend_port: int = typer.Option(FRONTEND_PORT_DEFAULT, "--frontend-port"),
) -> None:
    """Stop then start."""
    ports = {"backend": backend_port, "frontend": frontend_port}
    targets = _resolve_targets(service)
    for name in ("frontend", "backend"):
        if name in targets:
            _stop_one(name)
    time.sleep(0.3)
    for name in _NAMES:
        if name in targets:
            _start_one(name, ports[name])


@server_app.command("status")
def server_status(
    backend_port: int = typer.Option(BACKEND_PORT_DEFAULT, "--backend-port"),
    frontend_port: int = typer.Option(FRONTEND_PORT_DEFAULT, "--frontend-port"),
) -> None:
    """Report whether each server is running."""
    ports = {"backend": backend_port, "frontend": frontend_port}
    for name in _NAMES:
        _status_one(name, ports)


@server_app.command("logs")
def server_logs(
    service: str | None = typer.Argument(
        None, help="'backend', 'frontend', or omit for both."
    ),
    follow: bool = typer.Option(
        True, "--follow/--no-follow", "-f/-F",
        help="Tail -F by default; --no-follow prints the tail and exits.",
    ),
    lines: int = typer.Option(50, "--lines", "-n", help="Show last N lines."),
) -> None:
    """Tail the log file(s). Ctrl-C detaches; the servers keep running."""
    targets = _resolve_targets(service)
    files: list[str] = []
    for name in targets:
        p = _log_file(name)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch(exist_ok=True)
        files.append(str(p))
    cmd = ["tail", "-n", str(lines)]
    if follow:
        cmd.append("-F")
    cmd.extend(files)
    try:
        subprocess.call(cmd)
    except KeyboardInterrupt:
        pass
