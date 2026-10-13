#!/usr/bin/env bash
# Reference ACL commands for a SECURED Kafka cluster (SASL + authorizer enabled).
# Local docker-compose Kafka runs PLAINTEXT with no authorizer, so these are
# no-ops there -- run these against staging/prod brokers where ACLs matter.
#
# Usage: BOOTSTRAP=broker1:9092 ./kafka-acls.sh

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
EVENTS_TOPIC="${KAFKA_TOPIC_EVENTS:-events.raw}"
DLQ_TOPIC="${KAFKA_TOPIC_DLQ:-events.dlq}"
GROUP="${KAFKA_CONSUMER_GROUP:-events-consumer-group}"

# Producer principal: can only WRITE to the raw events topic.
kafka-acls.sh --bootstrap-server "$BOOTSTRAP" \
  --add --allow-principal "User:events-producer" \
  --operation Write --operation Describe \
  --topic "$EVENTS_TOPIC"

# Consumer principal: can READ raw events, WRITE to the DLQ, and use the consumer group.
kafka-acls.sh --bootstrap-server "$BOOTSTRAP" \
  --add --allow-principal "User:events-consumer" \
  --operation Read --operation Describe \
  --topic "$EVENTS_TOPIC"

kafka-acls.sh --bootstrap-server "$BOOTSTRAP" \
  --add --allow-principal "User:events-consumer" \
  --operation Write --operation Describe \
  --topic "$DLQ_TOPIC"

kafka-acls.sh --bootstrap-server "$BOOTSTRAP" \
  --add --allow-principal "User:events-consumer" \
  --operation Read \
  --group "$GROUP"
