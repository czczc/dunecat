# dunecat-hub

A multi-user web variant of dunecat, designed to run on a shared remote
server (e.g. behind the BNL VPN) instead of a single user's laptop.
Lives in `dunecat/hub/` alongside — and deliberately separate from —
the single-user local app at `dunecat/web/`.

This doc is a quick reference so future-you doesn't have to re-read
the implementation. For the original design walk-through see issue #26
and the spike at `.idea/spike/`.

## When to use which

| Audience            | Use `dunecat/web/`                  | Use `dunecat/hub/`                |
|---------------------|-------------------------------------|-----------------------------------|
| One user, laptop    | yes (`uv run dunecat server`)       | overkill                          |
| Multiple users      | no — single shared identity         | yes (`uv run dunecat hub`)        |
| Auth model          | local CLI + filesystem tokens       | per-user OIDC + DB                |
| Login surface       | `dunecat login` (terminal)          | `/hub/login` (browser)            |
| Re-login cadence    | every ~10 days (vault refresh)      | every ~28 days (vault lease)      |

The two apps share *only* the pure-function core in `dunecat/*.py`
(`client.py`, `datasets.py`, `files.py`, `filters.py`, etc.). They
never share state.

## Quick start (dev)

```bash
uv run dunecat hub
# open http://127.0.0.1:8001/
```

That's it. On first run the hub auto-generates an AES-GCM key at
`~/.dunecat/hub.key` (mode `0600`) and creates an empty SQLite DB at
`~/.dunecat/hub.sqlite`. Subsequent runs are silent.

`/` redirects to `/hub/login` when you're not signed in; sign in via
CILogon device flow; redirected to `/` which shows your identity and
a logout button.

## Login flow (what actually happens)

```
1. browser   ──GET /hub/login──▶                     hub backend
2.                              hub backend ──POST /v1/auth/oidc-dune/oidc/auth_url──▶ htvaultprod
3. hub stores poll context in `device_flows` table, renders HTML with
   a "Continue with CILogon" link (target=_blank + JS window.open).
4. user      ── opens link, completes CILogon flow ──▶ cilogon.org
5. polling tab in browser repeatedly hits /hub/login/poll
6. hub backend ──POST /v1/auth/oidc-dune/oidc/poll──▶ htvaultprod
   while vault returns authorization_pending.
7. user completes CILogon — vault flips state to complete; next poll
   returns auth response containing the vault token + metadata.credkey.
8. hub ──GET /v1/secret/oauth/creds/dune/<credkey>:default──▶ htvaultprod
   → fresh bearer JWT (iss=cilogon.org/dune, aud=wlcg.cern.ch/jwt/v1/any).
9. hub decodes JWT for `sub`, INSERTs or UPDATEs `users`, encrypts the
   vault token into `vault_tokens`, creates a `sessions` row, sets the
   HttpOnly cookie, returns {status: "ok"}.
10. polling tab redirects to `/`.
```

The bearer itself is never persisted on the hub. The *vault token* is
the long-lived per-user credential; bearers are minted from it on
demand at ~50ms each.

## How it differs from the local auth model

The local app shells out to two CLIs and persists their output on disk:

```
htgettoken      → /tmp/bt_u<uid>   (3h bearer)
metacat login   → ~/.token_library (metacat session, ~3h)
```

The hub does neither. **No `/tmp/bt_*`, no `~/.token_library`, no
subprocess.** Everything is HTTP-direct against vault, in-memory
except for the encrypted vault token in `hub.sqlite`. The two
codepaths produce bytewise-identical bearers (same `iss`/`aud`) — that
compatibility is what lets metacat accept either one — but they don't
share files or state.

| Concern                   | local (`dunecat/web/`)                      | hub (`dunecat/hub/`)                |
|---------------------------|---------------------------------------------|-------------------------------------|
| Who is "the user"         | whoever owns the venv                       | per-request, looked up by cookie    |
| Where the bearer lives    | `/tmp/bt_u<uid>`                            | nowhere on disk; in-memory only     |
| Where the refresh lives   | vault server (10d), htgettoken pulls bearer | DB (28d vault token, encrypted)     |
| Metacat session token     | `~/.token_library` file                     | not used — `MetaCatClient(token=…)` |
| Renewal trigger           | every metacat/Rucio call (5-min buffer)     | mint bearer per-request from vault  |
| First-login UX            | `dunecat login` in terminal                 | `/hub/login` in browser             |
| Re-login UX               | `dunecat login` again every ~10d            | sign-in button every ~28d           |
| Failure mode "no creds"   | terminal prompt to run `dunecat login`      | redirect to `/hub/login`            |

## Configuration

All read at startup (from environment, falling back to `.env`):

| Env var                      | Default                        | Purpose                                      |
|------------------------------|--------------------------------|----------------------------------------------|
| `DUNECAT_HUB_SECRET_KEY`     | (auto-generated, see below)    | AES-GCM key, base64 of 32 random bytes       |
| `DUNECAT_HUB_SECRET_KEY_FILE`| `~/.dunecat/hub.key`           | Path the key is loaded from / written to     |
| `DUNECAT_HUB_DB`             | `~/.dunecat/hub.sqlite`        | SQLite path                                  |
| `METACAT_SERVER_URL`         | (shared with local app)        | Will be used by future catalog routes        |
| `METACAT_AUTH_SERVER_URL`    | (shared with local app)        | Will be used by future catalog routes        |

Key resolution precedence at startup:

1. `DUNECAT_HUB_SECRET_KEY` env → use it (prod path).
2. `~/.dunecat/hub.key` → load if present (returning dev user).
3. Neither → generate 32 fresh bytes, write to the file with mode
   `0600`, log a `WARNING`. **Dev convenience only.**

In production, set `DUNECAT_HUB_SECRET_KEY` explicitly so a misplaced
key file can't silently break decryption.

## Where things live

| Path                         | What                                       |
|------------------------------|--------------------------------------------|
| `~/.dunecat/hub.sqlite`      | Per-user state: users, sessions, vault_tokens, device_flows |
| `~/.dunecat/hub.key`         | AES-GCM key (dev auto-gen path; not used when `DUNECAT_HUB_SECRET_KEY` is set) |
| `~/.dunecat/dunecat.db`      | Local app's DB. **The hub never reads or writes this.** |
| `/tmp/bt_u<uid>`             | Local app's bearer. **The hub never reads or writes this.** |
| `~/.token_library`           | Local app's metacat session. **The hub never reads or writes this.** |

## Schema

Four tables. All in `dunecat/hub/db.py`.

- `users(id, oidc_sub UNIQUE, metacat_username, created_at, last_seen_at)`
  — `oidc_sub` is the JWT `sub` claim (immutable UUID).
  `metacat_username` is vault's `metadata.credkey` (e.g. `chaoz`),
  used when the hub eventually calls metacat as that user.
- `sessions(id PK, user_id FK, created_at, last_seen_at, expires_at)`
  — 7-day sliding expiry. `id` is the opaque cookie value.
- `vault_tokens(user_id PK, ciphertext BLOB, nonce BLOB, expires_at, updated_at)`
  — encrypted with AES-GCM. Plaintext vault token never on disk.
- `device_flows(id PK, poll_body TEXT, expires_at, status)`
  — transient login flows. Garbage-collected every 5 min.

## Catalog routes (issue #27)

Ported, all `Depends(current_user)` + `metacat_for(user)`:

- **Datasets**: `/api/detectors`, `/api/detectors/counts`,
  `/api/datasets`, `/api/datasets/facets`, `/api/datasets/refresh`,
  `/api/dataset`
- **Files**: `/api/files`, `/api/files/count`, `/api/file`,
  `/api/run/{run}`
- **Queries**: `/api/query/run`, `/api/query/count`,
  `/api/query/validate`
- **Saved queries** (per-user, `UNIQUE(user_id, name)`):
  `GET /api/queries`, `POST /api/queries`,
  `PUT /api/queries/{id}`, `DELETE /api/queries/{id}`
- **Conditions DB** (auth-free upstream; we still require a session):
  `/api/runs/{detector}/{run}/conditions`,
  `/api/runs/{detector}/conditions`,
  `/api/detectors/{detector_id}/condb-columns`

Auth flow per request:

1. `Depends(current_user)` looks up the session via cookie.
2. `metacat_for(user)` decrypts the user's vault token →
   mints a fresh OIDC bearer (vault HTTP) → calls metacat's
   `login_token(username, bearer)` to get a session token →
   returns a `MetaCatClient` ready to use.
3. Each external call is wrapped in `with_timeout(...)` (60 s for
   metacat, 30 s for condb). On timeout: 504, the worker thread
   unwinds.

Per-request overhead is two FNAL round-trips (vault + metacat
login_token), roughly 100–150 ms. Per-session caching of the metacat
session token is a future optimisation, not built.

### Not (yet) ported

- `/api/replicas` — Rucio's `ReplicaClient` reads its bearer from a
  process-global file via `BEARER_TOKEN_FILE`. Adapting that to
  per-user is more involved (write per-user temp file; reset the
  cached client; guard against concurrent env mutation), so it lives
  in a follow-up. The SPA's file-detail page will surface "Replicas
  not available in hub mode yet" until then.

## SPA integration (in progress — issue #28)

Frontend boot sequence:

1. `main.js` calls `fetchConfig()` → `GET /api/config` (unauthenticated).
2. The response (`{"mode": "local" | "hub", "login_url"?: ...}`) lands
   in a module-scope object in `frontend/src/api.js`.
3. `createApp(App).use(router).mount('#app')` runs only after step 2.
4. Every subsequent `jsonFetch` reads `appConfig.mode`. In hub mode,
   a 401 from any endpoint redirects the browser to `login_url`. In
   local mode, 401s pass through to the existing error toasts.

`frontend/vite.config.js` reads `DUNECAT_PROXY_TARGET` so you can run
the dev SPA against either backend:

```bash
# against local app
npm --prefix frontend run dev

# against hub
DUNECAT_PROXY_TARGET=http://127.0.0.1:8001 npm --prefix frontend run dev
```

`<AppHeader>` shows a Sign Out button when `mode === "hub"`; it POSTs
`/hub/logout` and redirects to `login_url`.

## Static SPA serving (issue #28 — landed)

When `frontend/dist/index.html` exists, the hub serves the SPA itself
— no Vite needed for prod. Three mount points:

- `/assets/*` → built JS/CSS from Vite (`StaticFiles`)
- `/logo/*` → static images from `frontend/public/logo/`
- catch-all `GET /{full_path:path}` → returns `index.html` for any
  unmatched path, so Vue Router's HTML5 history mode works
  (`/datasets`, `/files/x:y`, etc.).

The catch-all explicitly refuses `api/...` and `hub/...` prefixes so a
typo'd API URL still returns JSON 404 instead of silently serving the
SPA shell.

`GET /` itself runs through `routes/login.py:home`:

- no session → 303 to `/hub/login`
- session + SPA bundle on disk → serve `frontend/dist/index.html`
- session + no SPA bundle → render the inline fallback (dev only)

Build the bundle for the hub with:

```bash
npm --prefix frontend run build
```

Then `uv run dunecat hub` serves it at `http://127.0.0.1:8001/`. No
Vite involved in prod. Dev iteration on the frontend still uses Vite:

```bash
DUNECAT_PROXY_TARGET=http://127.0.0.1:8001 npm --prefix frontend run dev
```

## What's *still* not yet built

- **`/api/replicas`** — Rucio per-user is awkward (`ReplicaClient`
  reads `BEARER_TOKEN_FILE` env var, which is process-global). Plan:
  write each user's bearer to a per-request temp file, reset the
  cached client, guard with a process-wide lock. Tracked as a
  follow-up; the SPA's file-detail "Replicas" panel will be empty in
  hub mode until then.
- **Per-session caching of the metacat session token.** Currently
  every request re-mints (vault → bearer → metacat login_token), ~150 ms.
  Cheap enough for v1; profile-driven if it bites.
- **Reverse proxy + TLS + systemd packaging** (Q10).
- **Backups, monitoring** (Q12).

## Production deployment notes (BNL)

Out of scope for this PoC, but for the eventual deployment:

- Uvicorn behind Caddy or nginx; TLS via BNL CIT (Let's Encrypt won't
  work behind VPN — see issue #4 in `.idea/next-steps-issues.md`).
- Run under systemd with `EnvironmentFile=/etc/dunecat-hub/env` that
  sets `DUNECAT_HUB_SECRET_KEY` and `DUNECAT_HUB_DB` explicitly.
- Back up the SQLite DB nightly to your usual ops target; back up the
  key file separately (e.g. password manager) so a backup tarball
  alone can't decrypt the DB.
- Outbound HTTPS from BNL to `htvaultprod.fnal.gov` and
  `metacat.fnal.gov` must work — verify with the issue #2 curl check.

## Pointers

- Source: `dunecat/hub/`
- Tests: `tests/hub/test_smoke.py` (mocked end-to-end)
- Local-only feasibility spike: `.idea/spike/vault_device_flow.py`
- Architectural decisions: issue #26 body, `.idea/next-steps-issues.md`
- htgettoken reference (vault HTTP endpoints): `.venv/lib/python3.12/site-packages/htgettoken/__init__.py`
