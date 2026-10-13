#!/usr/bin/env bash
# One-time: formats the Kafka KRaft storage directory. Run once before the
# first start-kafka-native.sh. Re-running wipes existing topic data.
set -e
cd "$(dirname "$0")/.."

JDK_DIR=$(ls -d local-infra/jdk-*/ | head -1)
KAFKA_DIR=$(ls -d local-infra/kafka_2.13-*/ | head -1)

export JAVA_HOME="$(cd "$JDK_DIR" && pwd)"
export PATH="$JAVA_HOME/bin:$PATH"

cd "$KAFKA_DIR"
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
echo "Formatting with cluster ID: $CLUSTER_ID"
bin/kafka-storage.sh format -t "$CLUSTER_ID" -c config/server.properties --standalone
