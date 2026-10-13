#!/usr/bin/env bash
# Starts the natively-installed Kafka broker (KRaft mode, no Docker, no Zookeeper).
# Assumes local-infra/jdk-*/ and local-infra/kafka_2.13-*/ already exist
# (see README.md "Run without Docker" section for the one-time setup).
set -e
cd "$(dirname "$0")/.."

JDK_DIR=$(ls -d local-infra/jdk-*/ | head -1)
KAFKA_DIR=$(ls -d local-infra/kafka_2.13-*/ | head -1)

export JAVA_HOME="$(cd "$JDK_DIR" && pwd)"
export PATH="$JAVA_HOME/bin:$PATH"

cd "$KAFKA_DIR"
echo "Starting Kafka broker (JAVA_HOME=$JAVA_HOME)..."
nohup bin/kafka-server-start.sh config/server.properties > ../kafka-broker.log 2>&1 &
echo "Kafka broker starting, pid $!. Logs: local-infra/kafka-broker.log"
