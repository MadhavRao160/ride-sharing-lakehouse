#!/usr/bin/env bash
# Start the local Kafka lab: containers -> topics -> schemas -> Python venv.
# Usage: ./local/up.sh
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); ROOT=$(cd "$HERE/.." && pwd)
cd "$HERE"
docker compose up -d

echo "waiting for brokers..."
for i in $(seq 1 60); do
  docker exec kafka1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka1:19092 >/dev/null 2>&1 && break
  sleep 2; [ "$i" = 60 ] && { echo "brokers not ready after 2 min"; docker compose logs --tail 30 kafka1; exit 1; }
done
"$HERE/apply_topics.sh"

echo; echo "waiting for schema registry..."
for i in $(seq 1 60); do
  curl -fsS http://localhost:8081/subjects >/dev/null 2>&1 && break
  sleep 2; [ "$i" = 60 ] && { echo "registry not ready after 2 min"; docker compose logs --tail 30 schema-registry; exit 1; }
done
bash "$ROOT/sim_host/schemas/register_schemas.sh"

if [ ! -x "$HERE/.venv/bin/python" ]; then
  # confluent-kafka ships ready-made builds only for released Pythons; use 3.13/3.12/3.11, not 3.15
  PY=""
  for v in python3.13 python3.12 python3.11; do command -v "$v" >/dev/null && { PY=$v; break; }; done
  [ -n "$PY" ] || { echo "Need Python 3.11-3.13. Run: brew install python@3.12   then re-run ./local/up.sh"; exit 1; }
  echo; echo "creating Python venv with $PY (one time)..."
  "$PY" -m venv "$HERE/.venv"
  "$HERE/.venv/bin/pip" install -q --upgrade pip
  "$HERE/.venv/bin/pip" install -q --only-binary confluent-kafka 'confluent-kafka[avro]>=2.6,<3' \
    || { rm -rf "$HERE/.venv"; echo "pip install failed; venv removed"; exit 1; }
fi
echo; echo "UP. Kafka UI: http://localhost:8080   next: ./local/sim.sh start"
