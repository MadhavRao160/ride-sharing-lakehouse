#!/usr/bin/env bash
# Run a lab script against the local cluster.
# Usage: ./local/lab.sh <script.py> [args...]     e.g. ./local/lab.sh serde_demo.py
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/.." && pwd)
[ -x "$HERE/.venv/bin/python" ] || { echo "No Python venv yet: run ./local/up.sh first"; exit 1; }
SCRIPT="$1"; shift
cd "$ROOT/sim_host/lab"
KAFKA_MODE=local exec "$HERE/.venv/bin/python" -u "$SCRIPT" "$@"
