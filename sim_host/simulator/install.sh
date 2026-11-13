#!/usr/bin/env bash
# Python environment for the simulator. Safe to re-run.
set -euo pipefail
VENV=/opt/rs/venv
REQ=/opt/rs/sim_host/simulator/requirements.txt

python3 -m pip --version >/dev/null 2>&1 || sudo dnf install -y -q python3-pip
if [ ! -x "$VENV/bin/python" ]; then
  echo "+ creating venv $VENV"
  sudo mkdir -p "$VENV" && sudo chown ec2-user:ec2-user "$VENV"
  python3 -m venv "$VENV"
fi
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -r "$REQ"
"$VENV/bin/python" --version
"$VENV/bin/pip" list 2>/dev/null | grep -Ei 'confluent-kafka|fastavro|aws-msk-iam'
