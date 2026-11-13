#!/usr/bin/env bash
# Kafka UI (kafbat) on the simulator EC2, as a Docker container.
# Logs in to MSK with the EC2 role (IAM), decodes Avro via our Schema Registry.
# Not exposed to the network: open it from the laptop with infra/scripts/kafka-ui.sh (SSM tunnel).
source /opt/rs/sim_host/env.sh
IMAGE=ghcr.io/kafbat/kafka-ui:v1.2.0

if ! command -v docker >/dev/null 2>&1; then
  echo "+ installing Docker"
  sudo dnf install -y -q docker
fi
sudo systemctl enable -q --now docker

echo "+ pulling $IMAGE"
sudo docker pull -q "$IMAGE"

sudo docker rm -f kafka-ui >/dev/null 2>&1 || true
sudo docker run -d --name kafka-ui --restart unless-stopped \
  --network host --memory 768m \
  -e KAFKA_CLUSTERS_0_NAME=rs-msk \
  -e KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS="$BS" \
  -e KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL \
  -e KAFKA_CLUSTERS_0_PROPERTIES_SASL_MECHANISM=AWS_MSK_IAM \
  -e KAFKA_CLUSTERS_0_PROPERTIES_SASL_CLIENT_CALLBACK_HANDLER_CLASS=software.amazon.msk.auth.iam.IAMClientCallbackHandler \
  -e "KAFKA_CLUSTERS_0_PROPERTIES_SASL_JAAS_CONFIG=software.amazon.msk.auth.iam.IAMLoginModule required;" \
  -e KAFKA_CLUSTERS_0_SCHEMAREGISTRY=http://localhost:8081 \
  -e AWS_REGION="$AWS_REGION" \
  "$IMAGE" >/dev/null
echo "+ container started"

echo -n "waiting for Kafka UI on :8080 "
for _ in $(seq 1 40); do
  if curl -fsS http://localhost:8080/actuator/health 2>/dev/null | grep -q UP; then
    echo " up"
    sleep 5
    echo "cluster status: $(curl -fsS http://localhost:8080/api/clusters | grep -oE '"status":"[A-Z]+"' | head -1)"
    exit 0
  fi
  echo -n "."; sleep 3
done
echo " NOT up. Last log lines:"
sudo docker logs --tail 40 kafka-ui
exit 1
