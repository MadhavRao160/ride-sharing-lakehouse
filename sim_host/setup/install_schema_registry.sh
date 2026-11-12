#!/usr/bin/env bash
# Install Confluent Schema Registry (Confluent Platform 7.9 = Kafka 3.9) and run it
# as a systemd service that stores schemas in MSK via IAM. Safe to re-run.
source /opt/rs/sim_host/env.sh
CP_VER=7.9
IAM_JAR="$HOME/kafka/libs/aws-msk-iam-auth-all.jar"
TMPL=/opt/rs/sim_host/schema_registry/schema-registry.properties.tmpl
CONF=/etc/schema-registry/schema-registry.properties

[ -s "$IAM_JAR" ] || { echo "Missing $IAM_JAR - run setup/install_kafka_tools.sh first"; exit 1; }

# 1. Package (Confluent yum repo; only the platform repo, no client libraries needed)
if ! rpm -q confluent-schema-registry >/dev/null 2>&1; then
  echo "+ adding Confluent $CP_VER repo and installing confluent-schema-registry"
  sudo rpm --import "https://packages.confluent.io/rpm/$CP_VER/archive.key"
  sudo tee /etc/yum.repos.d/confluent.repo >/dev/null <<EOF
[Confluent]
name=Confluent repository
baseurl=https://packages.confluent.io/rpm/$CP_VER
gpgcheck=1
gpgkey=https://packages.confluent.io/rpm/$CP_VER/archive.key
enabled=1
EOF
  sudo dnf install -y -q confluent-schema-registry
else
  echo "= confluent-schema-registry installed ($(rpm -q confluent-schema-registry))"
fi

# 2. MSK IAM library on the registry's classpath
sudo install -m 0444 "$IAM_JAR" /usr/share/java/schema-registry/aws-msk-iam-auth-all.jar
echo "= IAM library on registry classpath"

# 3. Config from the repo template (broker list looked up live)
HOST=$(hostname -I | awk '{print $1}')
TMP=$(mktemp)
sed -e "s|__BS__|$BS|" -e "s|__HOST__|$HOST|" "$TMPL" > "$TMP"
if ! sudo cmp -s "$TMP" "$CONF"; then
  echo "+ writing $CONF"
  sudo install -m 0644 "$TMP" "$CONF"
  CHANGED=1
else
  echo "= config unchanged"
  CHANGED=0
fi
rm -f "$TMP"

# 4. Service: start on boot, restart if config changed
sudo systemctl enable -q confluent-schema-registry
if [ "$CHANGED" = 1 ] || ! systemctl is-active -q confluent-schema-registry; then
  echo "+ (re)starting service"
  sudo systemctl restart confluent-schema-registry
fi

# 5. Health check (first start creates the _schemas topic; allow ~60s)
echo -n "waiting for registry on :8081 "
for _ in $(seq 1 30); do
  if curl -fsS http://localhost:8081/subjects >/dev/null 2>&1; then
    echo " up"
    echo "subjects: $(curl -fsS http://localhost:8081/subjects)"
    echo "global compatibility: $(curl -fsS http://localhost:8081/config)"
    exit 0
  fi
  echo -n "."; sleep 3
done
echo " NOT up. Last log lines:"
sudo journalctl -u confluent-schema-registry --no-pager -n 40
exit 1
