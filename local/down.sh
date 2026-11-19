#!/usr/bin/env bash
# Stop the local lab. Data is kept (Docker volumes) unless you pass --wipe.
# Usage: ./local/down.sh [--wipe]
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); cd "$HERE"
"$HERE/sim.sh" stop || true
if [ "${1:-}" = "--wipe" ]; then docker compose down -v; echo "stopped and data deleted"
else docker compose down; echo "stopped (data kept; --wipe to delete)"; fi
