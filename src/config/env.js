require("dotenv").config();

function int(value, fallback) {
  const n = parseInt(value, 10);
  return Number.isNaN(n) ? fallback : n;
}

function bool(value, fallback) {
  if (value === undefined) return fallback;
  return value === "true" || value === "1";
}

module.exports = {
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    clientId: process.env.KAFKA_CLIENT_ID || "events-service",
    topicEvents: process.env.KAFKA_TOPIC_EVENTS || "events.raw",
    topicDlq: process.env.KAFKA_TOPIC_DLQ || "events.dlq",
    consumerGroup: process.env.KAFKA_CONSUMER_GROUP || "events-consumer-group",
    topicPartitions: int(process.env.KAFKA_TOPIC_PARTITIONS, 6),
    dlqPartitions: int(process.env.KAFKA_DLQ_PARTITIONS, 3),
    replicationFactor: int(process.env.KAFKA_TOPIC_REPLICATION, 1),
    retentionMs: int(process.env.KAFKA_RETENTION_MS, 7 * 24 * 60 * 60 * 1000),
    dlqRetentionMs: int(process.env.KAFKA_DLQ_RETENTION_MS, 14 * 24 * 60 * 60 * 1000),
    maxRetries: int(process.env.KAFKA_MAX_RETRIES, 3),
  },
  api: {
    port: int(process.env.API_PORT, 4000),
    authToken: process.env.API_AUTH_TOKEN || "changeme-dev-token",
  },
  consumerMetricsPort: int(process.env.CONSUMER_METRICS_PORT, 4001),
  storageEnabled: bool(process.env.STORAGE_ENABLED, false),
  clickhouse: {
    url: process.env.CLICKHOUSE_URL || "http://localhost:8123",
    database: process.env.CLICKHOUSE_DB || "default",
    user: process.env.CLICKHOUSE_USER || "default",
    password: process.env.CLICKHOUSE_PASSWORD || "",
    table: process.env.CLICKHOUSE_TABLE || "events",
    batchSize: int(process.env.CLICKHOUSE_BATCH_SIZE, 500),
    flushMs: int(process.env.CLICKHOUSE_FLUSH_MS, 2000),
  },
};
