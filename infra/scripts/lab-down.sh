#!/usr/bin/env bash
# End of a Kafka lab session: stop everything that bills by the hour.
#   simulator stop -> MSK delete (asks you to type rs-msk) -> EC2 stop + NAT pause
# Topics, messages and registered schemas go with MSK; lab-up.sh rebuilds them from the repo.
source "$(dirname "$0")/common.sh"
DIR="$(dirname "$0")"

echo "== Simulator"
"$DIR/run-on-sim.sh" simulator/stop.sh || echo "   (EC2 not reachable; skipping)"

echo "== MSK"
"$DIR/msk-delete.sh"

echo "== EC2 + NAT"
"$DIR/pause-all.sh"
