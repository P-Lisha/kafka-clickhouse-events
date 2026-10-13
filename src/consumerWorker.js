const http = require("http");
const config = require("./config/env");
const logger = require("./utils/logger");
const kafkaConsumer = require("./kafka/consumer");
const lagReporter = require("./kafka/lagReporter");
const metrics = require("./metrics/prometheus");

function startMetricsServer() {
  const port = config.consumerMetricsPort;
  const server = http.createServer(async (req, res) => {
    if (req.url === "/metrics") {
      res.setHeader("Content-Type", metrics.register.contentType);
      res.end(await metrics.register.metrics());
      return;
    }
    res.statusCode = 404;
    res.end();
  });
  server.listen(port, () => logger.info(`Consumer metrics listening on port ${port}`));
  return server;
}

async function main() {
  const metricsServer = startMetricsServer();
  const stopLagReporter = lagReporter.start();

  let processFn;

  if (config.storageEnabled) {
    const writer = require("./clickhouse/writer");
    logger.info("Storage enabled: consumer will write events to ClickHouse");
    processFn = async (event) => {
      await writer.write(event);
    };
  } else {
    logger.info("Storage disabled: consumer running in queue-only (Version 1) mode");
    processFn = undefined; // consumer.start() falls back to a no-op logger
  }

  await kafkaConsumer.start(processFn);

  const shutdown = async () => {
    logger.info("Shutting down consumer...");
    if (config.storageEnabled) {
      const writer = require("./clickhouse/writer");
      await writer.flushRemaining();
    }
    await kafkaConsumer.stop();
    await stopLagReporter();
    metricsServer.close();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  logger.error("Consumer worker crashed", err);
  process.exit(1);
});
