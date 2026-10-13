const client = require("prom-client");

const register = new client.Registry();
client.collectDefaultMetrics({ register });

const eventsProducedTotal = new client.Counter({
  name: "events_produced_total",
  help: "Events successfully sent to Kafka",
  registers: [register],
});

const eventsProducedFailedTotal = new client.Counter({
  name: "events_produced_failed_total",
  help: "Events that failed to send to Kafka",
  registers: [register],
});

const eventsConsumedTotal = new client.Counter({
  name: "events_consumed_total",
  help: "Events successfully consumed and processed",
  registers: [register],
});

const eventsDlqTotal = new client.Counter({
  name: "events_dlq_total",
  help: "Events routed to the dead-letter topic",
  registers: [register],
});

const eventsInvalidPayloadTotal = new client.Counter({
  name: "events_invalid_payload_total",
  help: "Events rejected at the API due to schema validation failure",
  registers: [register],
});

const eventIngestLatencyMs = new client.Histogram({
  name: "event_ingest_latency_ms",
  help: "Time from API receipt to Kafka produce ack, in ms",
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2500],
  registers: [register],
});

const clickhouseInsertLatencyMs = new client.Histogram({
  name: "clickhouse_insert_latency_ms",
  help: "ClickHouse bulk insert latency, in ms",
  buckets: [10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
  registers: [register],
});

const clickhouseInsertErrorsTotal = new client.Counter({
  name: "clickhouse_insert_errors_total",
  help: "Failed ClickHouse bulk insert batches",
  registers: [register],
});

const consumerLag = new client.Gauge({
  name: "kafka_consumer_lag",
  help: "Approximate consumer lag (sum across partitions) for the events topic",
  registers: [register],
});

module.exports = {
  register,
  eventsProducedTotal,
  eventsProducedFailedTotal,
  eventsConsumedTotal,
  eventsDlqTotal,
  eventsInvalidPayloadTotal,
  eventIngestLatencyMs,
  clickhouseInsertLatencyMs,
  clickhouseInsertErrorsTotal,
  consumerLag,
};
