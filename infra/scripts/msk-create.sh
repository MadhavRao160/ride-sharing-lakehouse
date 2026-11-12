#!/usr/bin/env bash
# Create the rs-msk cluster (MSK Provisioned) exactly as designed, if it doesn't exist.
# Spec as code: same values we picked in the console on 12 Nov.
# Takes ~15-20 min. Afterwards run apply_topics.sh and install_schema_registry.sh.
source "$(dirname "$0")/common.sh"

NAME=rs-msk
KAFKA_VERSION=3.9.x
BROKER_TYPE=kafka.t3.small        # KRaft not offered on t3.small, so ZooKeeper mode
BROKERS=3                         # 1 per zone
EBS_GB=10
SUBNETS='"subnet-06915a93c19d711d4","subnet-0373d61d3601072d0","subnet-035544dcb811859c1"'  # private4/5/6
SG=sg-063f3739be1828a2b           # rs-sg-msk

ARN=$(aws kafka list-clusters-v2 --cluster-name-filter "$NAME" \
  --query 'ClusterInfoList[0].ClusterArn' --output text)
if [ "$ARN" != "None" ] && [ -n "$ARN" ]; then
  STATE=$(aws kafka describe-cluster-v2 --cluster-arn "$ARN" --query ClusterInfo.State --output text)
  echo "$NAME already exists (state: $STATE)."
else
  echo "== Creating $NAME ($BROKERS x $BROKER_TYPE, Kafka $KAFKA_VERSION, IAM auth only)"
  ARN=$(aws kafka create-cluster \
    --cluster-name "$NAME" \
    --kafka-version "$KAFKA_VERSION" \
    --number-of-broker-nodes "$BROKERS" \
    --broker-node-group-info "{\"InstanceType\":\"$BROKER_TYPE\",\"ClientSubnets\":[$SUBNETS],\"SecurityGroups\":[\"$SG\"],\"StorageInfo\":{\"EbsStorageInfo\":{\"VolumeSize\":$EBS_GB}}}" \
    --client-authentication '{"Sasl":{"Iam":{"Enabled":true}},"Unauthenticated":{"Enabled":false}}' \
    --encryption-info '{"EncryptionInTransit":{"ClientBroker":"TLS","InCluster":true}}' \
    --enhanced-monitoring DEFAULT \
    --tags project=rs \
    --query ClusterArn --output text)
  echo "   $ARN"
fi

echo -n "== Waiting for ACTIVE (check every 30s) "
for _ in $(seq 1 60); do
  STATE=$(aws kafka describe-cluster-v2 --cluster-arn "$ARN" --query ClusterInfo.State --output text)
  [ "$STATE" = "ACTIVE" ] && { echo " ACTIVE"; exit 0; }
  [ "$STATE" = "FAILED" ] && { echo " FAILED"; exit 1; }
  echo -n "."; sleep 30
done
echo " still $STATE after 30 min; check the console"; exit 1
