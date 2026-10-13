// Creates the event topics with partitions/retention configured.
// Run: npm run topics:create
const kafka = require("../client");
const config = require("../../config/env");
const logger = require("../../utils/logger");

async function createTopics() {
  const admin = kafka.admin();
  await admin.connect();

  try {
    const existing = await admin.listTopics();

    const topics = [
      {
        topic: config.kafka.topicEvents,
        numPartitions: config.kafka.topicPartitions,
        replicationFactor: config.kafka.replicationFactor,
        configEntries: [
          { name: "retention.ms", value: String(config.kafka.retentionMs) },
        ],
      },
      {
        topic: config.kafka.topicDlq,
        numPartitions: config.kafka.dlqPartitions,
        replicationFactor: config.kafka.replicationFactor,
        configEntries: [
          { name: "retention.ms", value: String(config.kafka.dlqRetentionMs) },
        ],
      },
    ].filter((t) => !existing.includes(t.topic));

    if (topics.length === 0) {
      logger.info("All topics already exist, nothing to create.");
    } else {
      await admin.createTopics({ topics, waitForLeaders: true });
      logger.info(
        "Created topics:",
        topics.map((t) => `${t.topic} (partitions=${t.numPartitions})`).join(", ")
      );
    }
  } finally {
    await admin.disconnect();
  }
}

if (require.main === module) {
  createTopics().catch((err) => {
    logger.error("Failed to create topics", err);
    process.exit(1);
  });
}

module.exports = createTopics;
