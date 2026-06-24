#!/usr/bin/env bash
#
# dune-xrdcp.sh — download a DUNE replica, auto-(re)minting the bearer token
#                 whenever it is missing or about to expire.
#
# Usage:
#   dune-xrdcp.sh <url> [dest-dir]
#
#   url        replica URL (required). Transfer tool is chosen by scheme:
#                root://         -> xrdcp  (e.g. CERN eospublic)
#                https:// davs:// -> curl  (WebDAV, e.g. FNAL dCache, CASTOR)
#              FNAL dCache's root://fndcadoor.fnal.gov:1094 door only offers
#              gsi (X.509 proxy) auth, so it is auto-rewritten to its
#              token-capable WebDAV door https://fndcadoor.fnal.gov:2880.
#   dest-dir   where to write it (defaults to the current directory).
#
# Token handling:
#   - Reuses $BEARER_TOKEN_FILE (WLCG discovery default:
#     $XDG_RUNTIME_DIR/bt_u<uid> on Linux, else /tmp/bt_u<uid>) if it has
#     more than MIN_TTL left.
#   - Otherwise runs htgettoken. If your cached *vault* token is still valid this
#     is non-interactive; once that expires too, htgettoken opens a browser for SSO.
#
set -euo pipefail

# ---- config (override via env) ---------------------------------------------
VAULT="${VAULT:-htvaultprod.fnal.gov}"
ISSUER="${ISSUER:-dune}"
MIN_TTL="${MIN_TTL:-300}"                       # remint if fewer seconds remain
# WLCG Bearer Token Discovery location: $XDG_RUNTIME_DIR/bt_u<uid> if that var
# is set (typical on Linux, e.g. /run/user/1000), else /tmp/bt_u<uid> (macOS).
# Exported so htgettoken writes and xrdcp reads the same path.
export BEARER_TOKEN_FILE="${BEARER_TOKEN_FILE:-${XDG_RUNTIME_DIR:-/tmp}/bt_u$(id -u)}"

usage() {
  # Print the leading comment block (after the shebang) up to the first
  # non-comment line, stripping the leading "# ".
  awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "${BASH_SOURCE[0]}"
  exit "${1:-0}"
}

[ $# -ge 1 ] || usage 1
case "$1" in -h|--help) usage 0;; esac

URL="$1"
DEST="${2:-.}"

# FNAL dCache xrootd door is gsi-only; use its WebDAV door (bearer-token capable).
if [[ "$URL" == root://fndcadoor.fnal.gov:1094/* ]]; then
  URL="https://fndcadoor.fnal.gov:2880/${URL#root://fndcadoor.fnal.gov:1094/}"
  echo ">> FNAL dCache root door is gsi-only; using WebDAV: $URL"
fi

# ---- make brew tools visible (login profiles aren't sourced in cron/scripts)-
if [ -x /opt/homebrew/bin/brew ]; then
  eval "$(/opt/homebrew/bin/brew shellenv)"
fi

# Locate tools (PATH first, then this repo's venv as fallback for htgettoken).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
XRDCP="$(command -v xrdcp || true)"
CURL="$(command -v curl || true)"
HTGETTOKEN="$(command -v htgettoken || true)"
[ -n "$HTGETTOKEN" ] || HTGETTOKEN="$REPO_ROOT/.venv/bin/htgettoken"
[ -x "$HTGETTOKEN" ] || { echo "ERROR: htgettoken not found at $HTGETTOKEN"; exit 1; }

# Require the tool that matches the URL scheme.
case "$URL" in
  root://*)         [ -n "$XRDCP" ] || { echo "ERROR: xrdcp not found (brew install xrootd)"; exit 1; } ;;
  https://*|davs://*) [ -n "$CURL" ]  || { echo "ERROR: curl not found"; exit 1; } ;;
  *) echo "ERROR: unsupported URL scheme: $URL"; exit 1 ;;
esac

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

# ---- pre-flight -------------------------------------------------------------
# FNAL dCache files can be tape-only (NEARLINE); a GET then 403s because reading
# them needs staging rights we don't have. Check locality up front via the REST
# frontend (:3880) and bail with a clear message instead of a cryptic 403.
check_dcache_online() {
  local api info loc qos
  api="${URL/:2880\//:3880/api/v1/namespace/}"   # WebDAV door -> frontend API
  info="$("$CURL" -s -H "Authorization: Bearer $(cat "$BEARER_TOKEN_FILE")" "${api}?locality=true&qos=true")"
  loc="$(printf '%s' "$info" | sed -n 's/.*"fileLocality"[^"]*"\([^"]*\)".*/\1/p')"
  qos="$(printf '%s' "$info" | sed -n 's/.*"currentQos"[^"]*"\([^"]*\)".*/\1/p')"
  case "$loc" in
    *ONLINE*) echo ">> dCache locality: $loc (qos=${qos:-?}) — on disk, proceeding." ;;
    "")       echo ">> WARNING: could not read dCache locality; attempting anyway." ;;
    *)        echo "ERROR: file is tape-resident (fileLocality=$loc, currentQos=${qos:-?})." >&2
              echo "       It must be staged to disk first. Request a Rucio rule to a disk" >&2
              echo "       RSE, or download a disk replica instead." >&2
              exit 3 ;;
  esac
}

# ---- transfer ---------------------------------------------------------------
# Download $URL into $DEST using the tool implied by the scheme. xrdcp reads the
# token via $BEARER_TOKEN_FILE; curl needs it as an Authorization header.
do_download() {
  case "$URL" in
    root://*)
      "$XRDCP" -f "$URL" "$DEST/" ;;
    https://*|davs://*)
      local url="${URL/#davs:/https:}" out
      out="$DEST/$(basename "${url%%\?*}")"
      "$CURL" -fL -C - -H "Authorization: Bearer $(cat "$BEARER_TOKEN_FILE")" -o "$out" "$url" ;;
  esac
}

# ---- run --------------------------------------------------------------------
mkdir -p "$DEST"
ensure_token

# Fail fast on tape-resident FNAL dCache files.
case "$URL" in
  https://fndcadoor.fnal.gov:2880/*) check_dcache_online ;;
esac

echo ">> downloading: $URL -> $DEST/"
if do_download; then
  echo ">> done."
  exit 0
fi

# One retry: a long transfer can outlive the token. Remint and try once more.
echo ">> download failed — reminting token and retrying once..."
mint_token
ensure_token
do_download
echo ">> done (after retry)."
