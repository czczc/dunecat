#!/usr/bin/env bash
# Deploy the latest main to a prod hub server.
#
# Usage:   ./scripts/hub-deploy.sh
# Or pin the SPA base for sub-path deployments:
#   VITE_BASE=/<prefix>/ ./scripts/hub-deploy.sh
#
# Env knobs:
#   DUNECAT_REPO   repo checkout to update (default: /opt/dunecat)
#   VITE_BASE      passed to the Vite build (default: /). Trailing slash.
#                  Set this to the same prefix as `--root-path` in the
#                  systemd unit when serving under a URL sub-path. Per-host
#                  defaults live in scripts/hub-deploy.env (sourced below);
#                  this env var overrides them.
#
# Side effects: `git pull --ff-only`, `uv sync`, `npm install`,
# `npm run build`, `sudo systemctl restart dunecat-hub`.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Per-host deploy defaults (e.g. VITE_BASE for the twister sub-path). An
# explicit VITE_BASE=... on the command line still wins; the env file only
# fills it in when unset.
if [[ -f "$SCRIPT_DIR/hub-deploy.env" ]]; then
    # shellcheck source=scripts/hub-deploy.env
    source "$SCRIPT_DIR/hub-deploy.env"
fi

REPO="${DUNECAT_REPO:-/opt/dunecat}"

cd "$REPO"

echo "==> git pull"
git pull --ff-only

echo "==> uv sync"
uv sync

echo "==> npm install + build SPA  (base='${VITE_BASE:-/}')"
( cd frontend && npm install --silent && npm run build )

echo "==> restart dunecat-hub"
sudo systemctl restart dunecat-hub

echo "==> waiting for /health"
for _ in $(seq 1 10); do
    if curl -fsS http://127.0.0.1:8001/health >/dev/null 2>&1; then
        echo "    OK"
        break
    fi
    sleep 1
done

echo "==> systemctl status"
sudo systemctl status dunecat-hub --no-pager | sed -n '1,8p'
