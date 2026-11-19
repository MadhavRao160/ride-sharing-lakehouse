#!/usr/bin/env bash
# Create the topics in sim_host/kafka/topics.conf on the LOCAL cluster (same settings as MSK).
# Safe to re-run: existing topics get their config synced.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
CONF="$ROOT/sim_host/kafka/topics.conf"
KT="docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:19092"
KC="docker exec kafka1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka1:19092"
COMMON="unclean.leader.election.enable=false,message.timestamp.type=LogAppendTime,segment.ms=86400000"
EXISTING=$($KT --list)

grep -vE '^\s*(#|$)' "$CONF" | while read -r NAME PARTS RF RET MINISR; do
  ALL="retention.ms=$RET,min.insync.replicas=$MINISR,$COMMON"
  if grep -qx "$NAME" <<<"$EXISTING"; then
    echo "~ $NAME exists, syncing config"
    $KC --alter --entity-type topics --entity-name "$NAME" --add-config "$ALL" >/dev/null
  else
    echo "+ create $NAME (partitions=$PARTS rf=$RF)"
    ARGS=(); IFS=',' read -ra KV <<<"$ALL"; for kv in "${KV[@]}"; do ARGS+=(--config "$kv"); done
    $KT --create --topic "$NAME" --partitions "$PARTS" --replication-factor "$RF" "${ARGS[@]}"
  fi
done
echo; echo "== Current state"
grep -vE '^\s*(#|$)' "$CONF" | awk '{print $1}' | while read -r NAME; do
  $KT --describe --topic "$NAME" | awk 'NR==1' | sed -E 's/TopicId: [^[:space:]]*[[:space:]]*//'
done
