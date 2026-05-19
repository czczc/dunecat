<p align="center">
  <img src="frontend/public/logo/dunecat-logo.png" alt="dunecat" width="420">
</p>

A unified Web UI over DUNE's [metacat](https://github.com/fermitools/metacat), [conditions DB](https://condb2.readthedocs.io/en/latest/index.html), and [Rucio](https://rucio.cern.ch) — search files, inspect runs, and find replicas without juggling three tools.

<p align="center">
  <a href="https://www.phy.bnl.gov/~chao/animation/dunecat-intro.mp4">Watch the intro video</a>
</p>

## Quick start

Requirements: Python 3.12+ with [`uv`](https://docs.astral.sh/uv/), Node.js 20+,
and a DUNE metacat / FNAL services account.

```bash
git clone git@github.com:czczc/dunecat.git
cd dunecat
uv sync
cd frontend && npm install && cd ..  # or use bun
cp .env.example .env       # then fill in METACAT_USER + RUCIO_ACCOUNT
uv run dunecat login       # browser opens for OIDC; once per ~10 days
```

Start both servers in the background, then open the app:

```bash
uv run dunecat server start    # uvicorn :8000 + Vite :5173
open http://127.0.0.1:5173
```

Use `127.0.0.1`, not `localhost` (macOS prefers IPv6; uvicorn binds IPv4 only).

## Managing the servers

```bash
uv run dunecat server status                # are they up?
uv run dunecat server logs                  # tail -F both (Ctrl-C detaches)
uv run dunecat server restart backend       # after editing backend code
uv run dunecat server stop                  # shut down both
```

Add `backend` or `frontend` as the last arg to scope a command to one
service. Logs live at `~/.dunecat/log/{backend,frontend}.log`, PIDs at
`~/.dunecat/run/`. uvicorn is **not** run with `--reload` on macOS (heavy
file-watcher); restart it via the command above after backend changes.

## Details

- [`docs/auth.md`](docs/auth.md) — login variants, automatic token renewal,
  troubleshooting 401s. Read this if you hit any auth-related error in the
  UI.
- [`dunecat/README.md`](dunecat/README.md) — Python CLI that scripts the
  same operations from the terminal.

## Configuration files

- `dunecat/web/detectors.yaml` — detector → namespace map. Add an entry to
  surface a new detector; `chip: false` puts it behind the "More detectors…"
  dropdown instead of a top-level chip. Restart uvicorn for changes to take
  effect.
- `~/.dunecat/dunecat.db` — local SQLite cache (per-namespace dataset list,
  saved queries, condb run conditions, Rucio replicas with 1 h TTL). Safe
  to delete; rebuilt on next use.
- `~/.dunecat/rucio/etc/rucio.cfg` — see [`docs/auth.md`](docs/auth.md#rucio-config).

## Development

```bash
uv run pytest           # backend unit tests, no network
```

Unit tests mock `MetaCatClient` at the boundary. There are no integration
tests in CI; live verification is running the app against the production
server.
