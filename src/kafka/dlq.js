const kafka = require("./client");
const config = require("../config/env");
const logger = require("../utils/logger");
const metrics = require("../metrics/prometheus");

const producer = kafka.producer({ allowAutoTopicCreation: false });
let connected = false;

async function connect() {
  if (connected) return;
  await producer.connect();
  connected = true;
}

async function disconnect() {
  if (!connected) return;
  await producer.disconnect();
  connected = false;
}

// originalMessage: the raw Kafka message value (string) that failed processing
async function sendToDlq(originalMessage, reason, meta = {}) {
  const dlqRecord = {
    originalMessage,
    reason,
    failedAt: new Date().toISOString(),
    ...meta,
  };

  try {
    await producer.send({
      topic: config.kafka.topicDlq,
      messages: [{ value: JSON.stringify(dlqRecord) }],
    });
    metrics.eventsDlqTotal.inc();
  } catch (err) {
    logger.error("Failed to publish to DLQ (message dropped)", err.message);
  }
}

module.exports = { connect, disconnect, sendToDlq };
