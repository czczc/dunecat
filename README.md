# dunecat

Locate DUNE data files via metacat queries. A thin Python library + CLI on top of
[`metacat-client`](https://fermitools.github.io/metacat/) tailored to the workflows of
finding datasets, inspecting metadata, and filtering files by run, run range, date, or
arbitrary metadata.

## Install

Requires Python 3.12+ and [`uv`](https://docs.astral.sh/uv/).

```bash
git clone git@github.com:czczc/dunecat.git
cd dunecat
uv sync
```

## Configure

Copy the template and fill in the server URLs:

```bash
cp .env.example .env
```

`.env` defaults to the production DUNE instance:

```
METACAT_SERVER_URL=https://metacat.fnal.gov:9443/dune_meta_prod/app
METACAT_AUTH_SERVER_URL=https://metacat.fnal.gov:8143/auth/dune
```

Authenticate once with the upstream `metacat` CLI to populate `~/.token_library`:

```bash
metacat -s $METACAT_SERVER_URL -a $METACAT_AUTH_SERVER_URL auth login -m <method> <username>
```

Tokens expire after one week. When `dunecat` returns exit code 2 with
`Token missing or expired`, re-run the command above.

## Commands

All commands write payload to stdout and progress/errors to stderr. List-shaped output
is one identifier per line by default; `--json` emits JSONL (one object per line) so the
stream stays memory-bounded. `dataset show` prints a `rich` table for humans and a
single-line JSON object with `--json`.

### List datasets

```bash
# fnmatch pattern as 'NAMESPACE:NAME_PATTERN'
uv run dunecat dataset list 'hd-protodune-det-reco:*cosmic*'

# restrict to a namespace, filter by dataset metadata (client-side)
uv run dunecat dataset list \
  --namespace hd-protodune-det-reco \
  --meta core.data_tier=full-reconstructed
```

### Inspect one dataset

```bash
uv run dunecat dataset show \
  'hd-protodune-det-reco:full-reconstructed__v09_91_02d01__standard_reco_stage2_calibration_protodunehd_keepup__cosmic_runchunk4_v1_official'
```

### List files in a dataset, filtered

```bash
DS='hd-protodune-det-reco:full-reconstructed__v09_91_02d01__standard_reco_stage2_calibration_protodunehd_keepup__cosmic_runchunk4_v1_official'

# explicit run numbers
uv run dunecat dataset files "$DS" --runs 27731,27732

# inclusive numeric range
uv run dunecat dataset files "$DS" --run-range 27000-28000

# generic metadata equality (repeatable; ANDed)
uv run dunecat dataset files "$DS" --meta dune.output_status=confirmed

# UTC date range against the run timestamp extracted from the filename
uv run dunecat dataset files "$DS" --date-range 2024-07-01:2024-07-31

# one representative file per UTC calendar date (post-filter)
uv run dunecat dataset files "$DS" --one-per-day

# include full file metadata in the output
uv run dunecat dataset files "$DS" --runs 27731 --json --with-metadata
```

The default filename timestamp regex is `(\d{8}T\d{6})` — the first
`YYYYMMDDTHHMMSS` match in the file's name portion of the DID, parsed as UTC. Override
with `--filename-time-regex PATTERN` and `--filename-time-format STRFTIME` for other
naming conventions.

To prevent client-side date filters from chewing through huge candidate sets, the
streaming pipeline aborts (exit 1) if more than `--date-range-max-candidates` files
arrive before the post-filter (default 10000). Narrow with `--runs`, `--run-range`,
`--namespace`, or `--meta` first.

### Distinct values for a metadata field

```bash
# what runs are in this dataset?
uv run dunecat dataset values "$DS" core.runs

# JSON array of distinct status values
uv run dunecat dataset values "$DS" dune.output_status --json
```

### Which datasets contain a file

Given a file DID (a single file's `namespace:name`), list the datasets it belongs to.
A file commonly lives in several datasets: an umbrella like `dune:all`, a per-batch
production set, and curated subsets.

```bash
uv run dunecat file datasets \
  'hd-protodune-det-reco:np04hd_raw_run027731_0003_dataflow1_datawriter_0_20240705T122251_reco_stage1_reco_stage2_20241004T202612_keepup.root'

# JSON array
uv run dunecat file datasets "$FILE_DID" --json
```

Unknown file DID → exit 1 with `File not found: <did>` on stderr.

### Raw MQL

When the structured filters don't cover what you need, pass raw MQL through:

```bash
uv run dunecat query "files from $DS where core.runs in (27731,27732) and dune.output_status='confirmed'"

# JSONL with full metadata
uv run dunecat query "files from $DS where core.runs in (27731)" --json --with-metadata
```

MQL syntax errors from the server surface with the server's full message on stderr and
exit 1.

## Development

```bash
uv run pytest          # backend unit tests, no network
```

Unit tests mock `MetaCatClient` at the boundary. There are no integration tests in CI;
the live verification path is running the CLI against the production server manually.

## Web UI (in progress)

A FastAPI backend (`dunecat/web/`) plus a Vite + Vue 3 frontend (`frontend/`) provide a
browser-based explorer for the same catalog. Single-user local app; reuses
`~/.token_library` for auth.

### Dev runbook

Two terminals — `uvicorn` is **not** run with `--reload` (the file-watcher is CPU-heavy
on macOS); restart manually after backend code changes.

```bash
# Terminal 1 — backend
uv run uvicorn dunecat.web:app --port 8000

# Terminal 2 — frontend
cd frontend
npm install              # first time only
npm run dev              # → http://localhost:5173
```

The frontend's Vite dev server proxies `/api/*` to `http://localhost:8000`.

### Endpoints (so far)

- `GET /api/detectors` — list of sub-detectors with live `datasets_count` and
  `files_count` per detector, sourced from `dunecat/web/detectors.yaml` and
  enriched from metacat.

Token-expired → 401 with the same `metacat auth login` instruction the CLI emits.
MQL / metacat server errors → 400 with the server's message in `detail`.
