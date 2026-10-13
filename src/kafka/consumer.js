const kafka = require("./client");
const config = require("../config/env");
const logger = require("../utils/logger");
const metrics = require("../metrics/prometheus");
const dlq = require("./dlq");

const consumer = kafka.consumer({ groupId: config.kafka.consumerGroup });

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// processFn(event) -> called per message. Throw to trigger retry/DLQ.
// If omitted, defaults to a no-op logger (Version 1 behaviour: queue only).
async function start(processFn) {
  const handler = processFn || (async (event) => {
    logger.info("Consumed event (no-op processor):", event.eventType, event.sourceId);
  });

  await consumer.connect();
  await dlq.connect();
  await consumer.subscribe({ topic: config.kafka.topicEvents, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message, partition }) => {
      const raw = message.value ? message.value.toString() : "";
      let event;

      try {
        event = JSON.parse(raw);
      } catch (err) {
        await dlq.sendToDlq(raw, "invalid_json", { partition });
        return;
      }

      let attempt = 0;
      let lastError;

      while (attempt <= config.kafka.maxRetries) {
        try {
          await handler(event);
          metrics.eventsConsumedTotal.inc();
          return;
        } catch (err) {
          lastError = err;
          attempt += 1;
          if (attempt <= config.kafka.maxRetries) {
            await sleep(2 ** attempt * 100); // exponential backoff
          }
        }
      }

      logger.error(
        `Processing failed after ${config.kafka.maxRetries} retries, routing to DLQ`,
        lastError && lastError.message
      );
      await dlq.sendToDlq(raw, lastError ? lastError.message : "unknown_error", { partition });
    },
  });

  logger.info("Kafka consumer running, group:", config.kafka.consumerGroup);
}

async function stop() {
  await consumer.disconnect();
  await dlq.disconnect();
}

module.exports = { start, stop, consumer };
