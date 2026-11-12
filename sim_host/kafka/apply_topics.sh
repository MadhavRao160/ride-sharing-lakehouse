#!/usr/bin/env bash
# Make the MSK topics match topics.conf. Safe to re-run (declarative).
# Every topic also gets unclean.leader.election.enable=false (zero data loss):
# MSK's default allows an out-of-sync replica to become leader, which can drop acked data.
#  - missing topic          -> create
#  - existing topic         -> update retention + min.insync.replicas
#  - more partitions wanted -> add partitions (warns: breaks key ordering for new data)
#  - fewer partitions / different replication -> warn only, never destructive
source /opt/rs/sim_host/env.sh
CONF=/opt/rs/sim_host/kafka/topics.conf
K="--bootstrap-server $BS --command-config $CLIENT_PROPS"

EXISTING=$(kafka-topics.sh $K --list)

grep -vE '^\s*(#|$)' "$CONF" | while read -r NAME PARTS RF RET MINISR; do
  if ! grep -qx "$NAME" <<<"$EXISTING"; then
    echo "+ create $NAME (partitions=$PARTS rf=$RF retention.ms=$RET min.insync=$MINISR)"
    kafka-topics.sh $K --create --topic "$NAME" --partitions "$PARTS" --replication-factor "$RF" \
      --config retention.ms="$RET" --config min.insync.replicas="$MINISR" \
      --config unclean.leader.election.enable=false
    continue
  fi

  echo "~ $NAME exists, syncing config"
  kafka-configs.sh $K --alter --entity-type topics --entity-name "$NAME" \
    --add-config retention.ms="$RET",min.insync.replicas="$MINISR",unclean.leader.election.enable=false

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
