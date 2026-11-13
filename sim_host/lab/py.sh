#!/usr/bin/env bash
# Run a lab Python script on the EC2 with the simulator's venv and MSK settings.
# Usage (from laptop): ./infra/scripts/run-on-sim.sh lab/py.sh <script.py> [args...]
source /opt/rs/sim_host/env.sh
SCRIPT="$1"; shift
cd /opt/rs/sim_host/lab
exec /opt/rs/venv/bin/python -u "$SCRIPT" "$@"
