#!/usr/bin/env bash
# Stop the simulator (flushes pending messages first).
set -euo pipefail
sudo systemctl stop rs-sim 2>/dev/null || true
echo "service: $(systemctl is-active rs-sim || true)"
sudo journalctl -u rs-sim --no-pager -n 3 -o cat
