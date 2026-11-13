#!/usr/bin/env bash
# Make the MSK topics match topics.conf. Safe to re-run (declarative).
#  - missing topic          -> create
#  - existing topic         -> update retention, min.insync and COMMON_CONFIGS
#  - more partitions wanted -> add partitions (warns: key-to-partition mapping changes for new data)
#  - fewer partitions / different replication -> warn only, never destructive
source /opt/rs/sim_host/env.sh
CONF=/opt/rs/sim_host/kafka/topics.conf
K="--bootstrap-server $BS --command-config $CLIENT_PROPS"
COMMON_CONFIGS="unclean.leader.election.enable=false,message.timestamp.type=LogAppendTime,segment.ms=86400000"

EXISTING=$(kafka-topics.sh $K --list)

grep -vE '^\s*(#|$)' "$CONF" | while read -r NAME PARTS RF RET MINISR; do
  ALL="retention.ms=$RET,min.insync.replicas=$MINISR,$COMMON_CONFIGS"

  if ! grep -qx "$NAME" <<<"$EXISTING"; then
    echo "+ create $NAME (partitions=$PARTS rf=$RF)"
    CFG_ARGS=()
    IFS=',' read -ra KV <<<"$ALL"
    for kv in "${KV[@]}"; do CFG_ARGS+=(--config "$kv"); done
    kafka-topics.sh $K --create --topic "$NAME" --partitions "$PARTS" \
      --replication-factor "$RF" "${CFG_ARGS[@]}"
    continue
  fi

  echo "~ $NAME exists, syncing config"
  kafka-configs.sh $K --alter --entity-type topics --entity-name "$NAME" --add-config "$ALL"

  DESC=$(kafka-topics.sh $K --describe --topic "$NAME")
  CUR_PARTS=$(grep -oP 'PartitionCount:\s*\K\d+' <<<"$DESC")
  CUR_RF=$(grep -oP 'ReplicationFactor:\s*\K\d+' <<<"$DESC")
  if [ "$PARTS" -gt "$CUR_PARTS" ]; then
    echo "  ! raising partitions $CUR_PARTS -> $PARTS (key-to-partition mapping changes for new data)"
    kafka-topics.sh $K --alter --topic "$NAME" --partitions "$PARTS"
  elif [ "$PARTS" -lt "$CUR_PARTS" ]; then
    echo "  WARNING: conf wants $PARTS partitions but topic has $CUR_PARTS; Kafka can't reduce. Skipped."
  fi
  if [ "$RF" -ne "$CUR_RF" ]; then
    echo "  WARNING: conf wants rf=$RF but topic has rf=$CUR_RF; needs a reassignment. Skipped."
  fi
done

echo
echo "== Current state (topic-level settings)"
grep -vE '^\s*(#|$)' "$CONF" | awk '{print $1}' | while read -r NAME; do
  kafka-topics.sh $K --describe --topic "$NAME" | head -1 \
    | sed -E 's/TopicId: [^ \t]+[ \t]*//; s/message\.format\.version=[^,]*,?//'
done
