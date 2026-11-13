#!/usr/bin/env bash
# Start of a Kafka lab session: rebuild everything from the repo, in dependency order.
#   NAT + EC2 -> MSK (~20 min) -> topics -> schema registry -> schemas -> Kafka UI -> simulator
# Usage: lab-up.sh [drivers=5000] [requests_per_sec=4]
source "$(dirname "$0")/common.sh"
DIR="$(dirname "$0")"
DRIVERS="${1:-5000}"
REQ_RATE="${2:-4}"

"$DIR/resume-all.sh"
"$DIR/msk-create.sh"
"$DIR/run-on-sim.sh" kafka/apply_topics.sh
"$DIR/run-on-sim.sh" setup/install_schema_registry.sh
"$DIR/run-on-sim.sh" schemas/register_schemas.sh
"$DIR/run-on-sim.sh" setup/install_kafka_ui.sh
"$DIR/run-on-sim.sh" simulator/start.sh "$DRIVERS" "$REQ_RATE"

echo
echo "Lab is up. Open Kafka UI with: ./infra/scripts/kafka-ui.sh"
