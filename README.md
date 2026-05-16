<p align="center">
  <img src="frontend/public/logo/dunecat-logo.png" alt="dunecat" width="420">
</p>

A local web app for browsing the DUNE metacat file catalog: pick a detector,
narrow datasets by tier / file type, look up runs, and run raw MQL queries —
all from a browser, against the production metacat server.

A Python CLI ships alongside for scripting the same operations from the
terminal; see [`dunecat/README.md`](dunecat/README.md) for that.

## Requirements

- Python 3.12+ and [`uv`](https://docs.astral.sh/uv/)
- Node.js 20+ and `npm`
- A DUNE metacat account

## Install

```bash
git clone git@github.com:czczc/dunecat.git
cd dunecat
uv sync                 # backend deps + the dunecat CLI
cd frontend && npm install && cd ..   # frontend deps (first time only)
```

## Configure

```bash
cp .env.example .env
```

`.env` defaults to the production DUNE instance. Fill in your DUNE username
and preferred auth method:

```
METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_prod/app
METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
METACAT_USER=<your-username>
METACAT_AUTH_METHOD=password          # or x509 / token; see `metacat auth login --help`
```

Authenticate once to populate `~/.token_library` (reused by both the web app
and the CLI):

```bash
uv run dunecat login                  # uses .env defaults; --user/--method override
```

This wraps the upstream `metacat auth login` command using the URLs and
defaults from `.env`. Tokens expire after one week. When the app returns
`Token missing or expired`, re-run `uv run dunecat login`.

## Rucio (file replicas) — optional

The file detail page can look up Rucio replicas (RSE + PFN per file) on
demand. This needs a DUNE-scoped OIDC bearer token. Both `rucio-clients`
and `htgettoken` are project dependencies, so once `uv sync` has run
they're available at `.venv/bin/htgettoken` — nothing else to install.

Set the relevant variables in `.env`:

```
RUCIO_ACCOUNT=<your FNAL Rucio account, e.g. your username>
BEARER_TOKEN_FILE=/tmp/bt_u<your-uid>      # default htgettoken path on Linux/macOS
```

Mint a token (browser device-code flow the first time per ~10 days):

```bash
uv run htgettoken --vaulttokenttl=10d --vaultserver=htvaultprod.fnal.gov --issuer=dune
```

This produces:

- A bearer token at `/tmp/bt_u<uid>` — good ~3 hours. dunecat reads this
  fresh on each Rucio call.
- A vault refresh token at `/tmp/vt_u<uid>` — good 10 days. Lets future
  `htgettoken` invocations re-mint the bearer **without** another browser
  dance.

To keep the bearer fresh during a long uvicorn session, run htgettoken
every ~2 hours. Two convenient options on macOS:

```bash
# Quick: from another terminal whenever you see a 401 from /api/replicas
uv run htgettoken --vaultserver=htvaultprod.fnal.gov --issuer=dune

# Hands-off: a launchd / cron job
# In `crontab -e`, every 2 hours:
0 */2 * * * cd ~/Code/metacat && .venv/bin/htgettoken \
  --vaultserver=htvaultprod.fnal.gov --issuer=dune >/dev/null 2>&1
```

When the bearer is missing or expired, `/api/replicas` returns 401 with
the literal command above — the UI surfaces it verbatim so you know
exactly what to run.

The Rucio config (`~/.dunecat/rucio/etc/rucio.cfg`) is generated from
your `.env` values on first use. You don't need to maintain it
separately.

## Run the web app

Two terminals — `uvicorn` is **not** run with `--reload` (the macOS file-watcher
is CPU-heavy); restart manually after backend code changes.

```bash
# Terminal 1 — backend (FastAPI on :8000)
uv run uvicorn dunecat.web:app --port 8000

# Terminal 2 — frontend (Vite dev server on :5173)
cd frontend
npm run dev
```

Open <http://127.0.0.1:5173>. Use `127.0.0.1`, not `localhost` — macOS prefers
IPv6 for `localhost` while uvicorn binds IPv4 only.

The frontend proxies `/api/*` to the backend; switching detectors, dataset
metadata, file lineage, run lookups, and saved MQL queries all live in the UI.

## CLI

```bash
uv run dunecat dataset list 'hd-protodune-det-reco:*cosmic*'
```

Full command reference: [`dunecat/README.md`](dunecat/README.md).

## Development

```bash
uv run pytest           # backend unit tests, no network
```

Unit tests mock `MetaCatClient` at the boundary. There are no integration tests
in CI; live verification is running the app against the production server.

## Configuration files

- `dunecat/web/detectors.yaml` — detector → namespace map. Add a detector by
  appending an entry; restart uvicorn for changes to take effect.
- `~/.dunecat/dunecat.db` — local SQLite cache (per-namespace dataset list and
  saved queries, condb run conditions, Rucio replicas with 1 h TTL). Safe to
  delete; the app will rebuild on next use.
- `~/.dunecat/rucio/etc/rucio.cfg` — auto-generated from your `.env` values
  the first time the server makes a Rucio call. Delete to regenerate from
  current `.env`.
