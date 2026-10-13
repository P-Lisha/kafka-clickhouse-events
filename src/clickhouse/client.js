const { createClient } = require("@clickhouse/client");
const config = require("../config/env");

const client = createClient({
  url: config.clickhouse.url,
  database: config.clickhouse.database,
  username: config.clickhouse.user,
  password: config.clickhouse.password,
});

module.exports = client;
