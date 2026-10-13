const kafka = require("./client");
const config = require("../config/env");
const logger = require("../utils/logger");
const metrics = require("../metrics/prometheus");

const POLL_MS = 15000;
let timer = null;

async function reportOnce(admin) {
  const topicOffsets = await admin.fetchTopicOffsets(config.kafka.topicEvents);
  const groupOffsets = await admin.fetchOffsets({
    groupId: config.kafka.consumerGroup,
    topics: [config.kafka.topicEvents],
  });

  const committedByPartition = new Map(
    groupOffsets[0].partitions.map((p) => [p.partition, Number(p.offset)])
  );

  let totalLag = 0;
  for (const { partition, offset } of topicOffsets) {
    const committed = committedByPartition.get(partition) ?? 0;
    totalLag += Math.max(0, Number(offset) - committed);
  }

  metrics.consumerLag.set(totalLag);
}

function start() {
  const admin = kafka.admin();
  admin
    .connect()
    .then(() => {
      timer = setInterval(() => {
        reportOnce(admin).catch((err) => logger.warn("Lag reporter poll failed", err.message));
      }, POLL_MS);
    })
    .catch((err) => logger.warn("Lag reporter admin connect failed", err.message));

  return async () => {
    if (timer) clearInterval(timer);
    await admin.disconnect().catch(() => {});
  };
}

module.exports = { start };
