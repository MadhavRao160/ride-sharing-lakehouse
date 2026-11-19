#!/usr/bin/env bash
# Run the simulator on the Mac against the local cluster, in the background.
# Usage: ./local/sim.sh start [drivers=1000] [requests_per_sec=2] | stop | status
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/.." && pwd)
[ -x "$HERE/.venv/bin/python" ] || { echo "No Python venv yet: run ./local/up.sh first"; exit 1; }
PID="$HERE/.sim.pid"; LOG="$HERE/sim.log"
case "${1:-status}" in
  start)
    "$0" stop >/dev/null 2>&1 || true
    cd "$ROOT/sim_host/simulator"
    KAFKA_MODE=local DRIVERS="${2:-1000}" REQ_RATE="${3:-2}" DUP_RATE=0.01 LATE_RATE=0.005 \
      nohup "$HERE/.venv/bin/python" -u sim.py >"$LOG" 2>&1 &
    echo $! >"$PID"
    echo "started pid $(cat "$PID"): drivers=${2:-1000} requests/s=${3:-2}; first stats in ~25s"
    sleep 25; tail -n 6 "$LOG" ;;
  stop)
    if [ -f "$PID" ] && kill "$(cat "$PID")" 2>/dev/null; then echo "simulator stopped"; else echo "simulator not running"; fi
    rm -f "$PID" ;;
  status)
    if [ -f "$PID" ] && kill -0 "$(cat "$PID")" 2>/dev/null; then echo "running pid $(cat "$PID")"; tail -n 4 "$LOG"
    else echo "not running"; fi ;;
esac
