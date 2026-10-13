const kafka = require("./client");
const config = require("../config/env");
const logger = require("../utils/logger");
const metrics = require("../metrics/prometheus");

const producer = kafka.producer({
  allowAutoTopicCreation: false,
});

let connected = false;

async function connect() {
  if (connected) return;
  await producer.connect();
  connected = true;
  logger.info("Kafka producer connected");
}

async function disconnect() {
  if (!connected) return;
  await producer.disconnect();
  connected = false;
}

// events: array of validated event objects, each already has eventType/timestamp/payload/sourceId
async function sendEvents(events) {
  const startedAt = Date.now();
  const messages = events.map((event) => ({
    key: event.sourceId,
    value: JSON.stringify(event),
  }));

  try {
    await producer.send({
      topic: config.kafka.topicEvents,
      acks: -1,
      messages,
    });
    metrics.eventsProducedTotal.inc(events.length);
    metrics.eventIngestLatencyMs.observe(Date.now() - startedAt);
    return { success: true };
  } catch (err) {
    metrics.eventsProducedFailedTotal.inc(events.length);
    logger.error("Kafka produce failed", err.message);
    return { success: false, error: err.message };
  }
}

module.exports = { connect, disconnect, sendEvents, producer };
