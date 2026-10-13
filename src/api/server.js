const express = require("express");
const config = require("../config/env");
const logger = require("../utils/logger");
const metrics = require("../metrics/prometheus");
const requireAuth = require("./middleware/auth");
const eventsRouter = require("./routes/events");
const producer = require("../kafka/producer");

function createApp() {
  const app = express();
  app.use(express.json({ limit: "2mb" }));

  app.get("/health", (req, res) => res.json({ status: "ok" }));

  app.get("/metrics", async (req, res) => {
    res.set("Content-Type", metrics.register.contentType);
    res.end(await metrics.register.metrics());
  });

  app.use("/api/events", requireAuth, eventsRouter);

  app.use((err, req, res, next) => {
    logger.error("Unhandled API error", err);
    res.status(500).json({ error: "internal_error" });
  });

  return app;
}

async function main() {
  await producer.connect();
  const app = createApp();

  const server = app.listen(config.api.port, () => {
    logger.info(`Ingestion API listening on port ${config.api.port}`);
  });

  const shutdown = async () => {
    logger.info("Shutting down API...");
    server.close();
    await producer.disconnect();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

if (require.main === module) {
  main().catch((err) => {
    logger.error("Failed to start API", err);
    process.exit(1);
  });
}

module.exports = { createApp };
