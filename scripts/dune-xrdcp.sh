#!/usr/bin/env bash
#
# dune-xrdcp.sh — download a DUNE file via xrdcp, auto-(re)minting the bearer
#                 token whenever it is missing or about to expire.
#
# Usage:
#   dune-xrdcp.sh <xrootd-url> [dest-dir]
#
#   xrootd-url   root:// URL of the replica (required).
#   dest-dir     where to write it (defaults to the current directory).
#
# Token handling:
#   - Reuses $BEARER_TOKEN_FILE (default /tmp/bt_u<uid>) if it has > MIN_TTL left.
#   - Otherwise runs htgettoken. If your cached *vault* token is still valid this
#     is non-interactive; once that expires too, htgettoken opens a browser for SSO.
#
set -euo pipefail

# ---- config (override via env) ---------------------------------------------
VAULT="${VAULT:-htvaultprod.fnal.gov}"
ISSUER="${ISSUER:-dune}"
MIN_TTL="${MIN_TTL:-300}"                       # remint if fewer seconds remain
export BEARER_TOKEN_FILE="${BEARER_TOKEN_FILE:-/tmp/bt_u$(id -u)}"

usage() {
  sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

[ $# -ge 1 ] || usage 1
case "$1" in -h|--help) usage 0;; esac

URL="$1"
DEST="${2:-.}"

# ---- make brew tools visible (login profiles aren't sourced in cron/scripts)-
if [ -x /opt/homebrew/bin/brew ]; then
  eval "$(/opt/homebrew/bin/brew shellenv)"
fi

# Locate xrdcp and htgettoken (PATH first, then this repo's venv as fallback).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
XRDCP="$(command -v xrdcp || true)"
HTGETTOKEN="$(command -v htgettoken || true)"
[ -n "$HTGETTOKEN" ] || HTGETTOKEN="$REPO_ROOT/.venv/bin/htgettoken"

[ -n "$XRDCP" ]            || { echo "ERROR: xrdcp not found (brew install xrootd)"; exit 1; }
[ -x "$HTGETTOKEN" ]       || { echo "ERROR: htgettoken not found at $HTGETTOKEN"; exit 1; }

# ---- token helpers ----------------------------------------------------------
# Seconds of life left in the bearer token, or -1 if missing/unparseable.
token_seconds_left() {
  local f="$BEARER_TOKEN_FILE" payload json exp now
  [ -s "$f" ] || { echo -1; return; }
  payload="$(cut -d. -f2 "$f" | tr '_-' '/+')"
  case $(( ${#payload} % 4 )) in 2) payload+='==';; 3) payload+='=';; esac
  json="$(printf '%s' "$payload" | base64 --decode 2>/dev/null \
        || printf '%s' "$payload" | base64 -D 2>/dev/null)"
  exp="$(printf '%s' "$json" | tr ',' '\n' | sed -n 's/.*"exp":\([0-9]*\).*/\1/p' | head -1)"
  [ -n "$exp" ] || { echo -1; return; }
  now="$(date +%s)"
  echo $(( exp - now ))
}

token_ok() { [ "$(token_seconds_left)" -ge "$MIN_TTL" ]; }

mint_token() {
  echo ">> token missing/expiring — running htgettoken (browser may open for SSO)..."
  "$HTGETTOKEN" --nokerberos --nossh -a "$VAULT" -i "$ISSUER" --web-open-command=open
}

ensure_token() {
  token_ok || mint_token
  if ! token_ok; then
    echo "ERROR: still no valid token after htgettoken." >&2
    exit 1
  fi
  echo ">> token OK (~$(token_seconds_left)s left): $BEARER_TOKEN_FILE"
}

# ---- run --------------------------------------------------------------------
mkdir -p "$DEST"
ensure_token

echo ">> xrdcp $URL -> $DEST/"
if "$XRDCP" -f "$URL" "$DEST/"; then
  echo ">> done."
  exit 0
fi

# One retry: a long transfer can outlive the token. Remint and try once more.
echo ">> xrdcp failed — reminting token and retrying once..."
mint_token
ensure_token
"$XRDCP" -f "$URL" "$DEST/"
echo ">> done (after retry)."
