#!/usr/bin/env bash
# Common settings for scripts that run ON the simulator EC2.
# Looks up the MSK broker list at run time, so it survives a cluster rebuild.
set -euo pipefail

export AWS_REGION=ap-southeast-2
export AWS_PAGER=""
export MSK_CLUSTER_NAME=rs-msk
export KAFKA_HOME="$HOME/kafka"
export PATH="$KAFKA_HOME/bin:$PATH"
export CLIENT_PROPS="/opt/rs/sim_host/kafka/client.properties"

CLUSTER_ARN=$(aws kafka list-clusters-v2 --region "$AWS_REGION" \
  --cluster-name-filter "$MSK_CLUSTER_NAME" \
  --query 'ClusterInfoList[0].ClusterArn' --output text)
if [ -z "$CLUSTER_ARN" ] || [ "$CLUSTER_ARN" = "None" ]; then
  echo "MSK cluster $MSK_CLUSTER_NAME not found"; exit 1
fi
BS=$(aws kafka get-bootstrap-brokers --region "$AWS_REGION" --cluster-arn "$CLUSTER_ARN" \
  --query BootstrapBrokerStringSaslIam --output text)
export BS
