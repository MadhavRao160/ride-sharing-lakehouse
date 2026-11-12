#!/usr/bin/env bash
# Delete the rs-msk cluster to stop its hourly cost.
# Loses: topics and their data (topics come back from topics.conf; data is test data).
# Asks you to type the cluster name before deleting.
source "$(dirname "$0")/common.sh"
NAME=rs-msk

ARN=$(aws kafka list-clusters-v2 --cluster-name-filter "$NAME" \
  --query 'ClusterInfoList[0].ClusterArn' --output text)
if [ "$ARN" = "None" ] || [ -z "$ARN" ]; then
  echo "$NAME not found. Nothing to do."; exit 0
fi

STATE=$(aws kafka describe-cluster-v2 --cluster-arn "$ARN" --query ClusterInfo.State --output text)
if [ "$STATE" = "DELETING" ]; then echo "$NAME is already DELETING."; exit 0; fi
if [ "$STATE" = "CREATING" ] || [ "$STATE" = "UPDATING" ]; then
  echo -n "$NAME is $STATE; MSK only deletes ACTIVE clusters. Waiting (check every 30s) "
  for _ in $(seq 1 60); do
    sleep 30; echo -n "."
    STATE=$(aws kafka describe-cluster-v2 --cluster-arn "$ARN" --query ClusterInfo.State --output text)
    [ "$STATE" = "ACTIVE" ] || [ "$STATE" = "FAILED" ] && break
  done
  echo " $STATE"
fi

VER=$(aws kafka describe-cluster-v2 --cluster-arn "$ARN" \
  --query ClusterInfo.Provisioned.CurrentBrokerSoftwareInfo.KafkaVersion --output text)
echo "About to DELETE $NAME (Kafka $VER) in account $ACCOUNT."
echo "All topics and messages in it are lost. msk-create.sh rebuilds it (~15-20 min)."
read -r -p "Type the cluster name to confirm: " CONFIRM
[ "$CONFIRM" = "$NAME" ] || { echo "Not confirmed. Nothing deleted."; exit 1; }

aws kafka delete-cluster --cluster-arn "$ARN" >/dev/null
echo "Delete started. MSK finishes it in the background (~10 min); billing stops when it's gone."
