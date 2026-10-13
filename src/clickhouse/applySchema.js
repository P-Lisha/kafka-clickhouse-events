// Applies schema.sql to ClickHouse. Run: npm run clickhouse:schema
const fs = require("fs");
const path = require("path");
const client = require("./client");
const logger = require("../utils/logger");

async function applySchema() {
  const sql = fs.readFileSync(path.join(__dirname, "schema.sql"), "utf8");
  await client.command({ query: sql });
  logger.info("ClickHouse schema applied.");
}

if (require.main === module) {
  applySchema()
    .then(() => process.exit(0))
    .catch((err) => {
      logger.error("Failed to apply ClickHouse schema", err);
      process.exit(1);
    });
}

module.exports = applySchema;
