#!/usr/bin/env bash
# Install what the simulator EC2 needs to talk to MSK. Safe to re-run.
# Java 17 + Kafka 3.9.1 CLI tools + MSK IAM login library.
set -euo pipefail
KAFKA_VER=3.9.1
SCALA_VER=2.13
IAM_VER=2.4.0

if ! java -version 2>&1 | grep -q '"17'; then
  echo "+ installing Java 17"
  sudo dnf install -y -q java-17-amazon-corretto-headless
else
  echo "= Java 17 present"
fi

if [ ! -x "$HOME/kafka/bin/kafka-topics.sh" ]; then
  echo "+ installing Kafka $KAFKA_VER tools"
  cd "$HOME"
  curl -fsSL -o kafka.tgz "https://archive.apache.org/dist/kafka/$KAFKA_VER/kafka_${SCALA_VER}-${KAFKA_VER}.tgz"
  tar -xzf kafka.tgz && rm -f kafka.tgz
  rm -rf kafka && mv "kafka_${SCALA_VER}-${KAFKA_VER}" kafka
else
  echo "= Kafka tools present"
fi

JAR="$HOME/kafka/libs/aws-msk-iam-auth-all.jar"
if [ ! -s "$JAR" ]; then
  echo "+ installing MSK IAM library $IAM_VER"
  curl -fsSL -o "$JAR" \
    "https://github.com/aws/aws-msk-iam-auth/releases/download/v$IAM_VER/aws-msk-iam-auth-$IAM_VER-all.jar"
else
  echo "= MSK IAM library present"
fi

java -version 2>&1 | head -1
"$HOME/kafka/bin/kafka-topics.sh" --version
ls -lh "$JAR" | awk '{print "IAM jar:", $5}'
