#!/usr/bin/env bash
# Inspect dunecat-hub logs.
#
# Usage:
#   ./scripts/hub-logs.sh                # last 1h of WARN/ERR + live tail
#   ./scripts/hub-logs.sh "30 min ago"   # narrow the lookback window
#   ./scripts/hub-logs.sh tail           # skip the summary, just live-tail

set -euo pipefail

ARG="${1:-1 hour ago}"

if [[ "$ARG" == "tail" ]]; then
    exec sudo journalctl -u dunecat-hub -f
fi

echo "==> service status"
sudo systemctl status dunecat-hub --no-pager | sed -n '1,8p'
echo

echo "==> errors / warnings since: $ARG"
sudo journalctl -u dunecat-hub --since "$ARG" --no-pager \
    | grep -E 'ERROR|WARN|Traceback|Exception|timed out|condb fetch failed|rucio' \
    | tail -50 \
    || echo "(none)"
echo

echo "==> live tail (Ctrl-C to exit)"
exec sudo journalctl -u dunecat-hub -f
