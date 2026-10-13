const crypto = require("crypto");
const client = require("./client");
const config = require("../config/env");
const logger = require("../utils/logger");
const metrics = require("../metrics/prometheus");
const dlq = require("../kafka/dlq");

const MAX_DEDUP_CACHE = 50000;
const seenKeys = new Map(); // dedup_key -> true, insertion-ordered for FIFO eviction

function dedupKeyFor(event) {
  return crypto
    .createHash("sha256")
    .update(`${event.sourceId}|${event.eventType}|${event.timestamp}`)
    .digest("hex");
}

function rememberKey(key) {
  if (seenKeys.size >= MAX_DEDUP_CACHE) {
    const oldest = seenKeys.keys().next().value;
    seenKeys.delete(oldest);
  }
  seenKeys.set(key, true);
}

let buffer = [];
let flushTimer = null;

function toRow(event, dedupKey) {
  const eventTime = new Date(event.timestamp);
  return {
    event_date: eventTime.toISOString().slice(0, 10),
    event_time: eventTime.toISOString().slice(0, 19).replace("T", " "),
    event_type: event.eventType,
    source_id: event.sourceId,
    payload: JSON.stringify(event.payload),
    dedup_key: dedupKey,
  };
}

async function flush() {
  if (buffer.length === 0) return;
  const rows = buffer;
  buffer = [];

  const startedAt = Date.now();
  try {
    await client.insert({
      table: config.clickhouse.table,
      values: rows,
      format: "JSONEachRow",
    });
    metrics.clickhouseInsertLatencyMs.observe(Date.now() - startedAt);
  } catch (err) {
    metrics.clickhouseInsertErrorsTotal.inc();
    logger.error("ClickHouse insert failed, routing batch to DLQ", err.message);
    await Promise.all(
      rows.map((row) => dlq.sendToDlq(JSON.stringify(row), `clickhouse_insert_failed: ${err.message}`))
    );
  }
}

function scheduleFlush() {
  if (flushTimer) return;
  flushTimer = setTimeout(async () => {
    flushTimer = null;
    await flush();
  }, config.clickhouse.flushMs);
}

// Call per consumed event. Buffers and flushes in batches; skips duplicates.
async function write(event) {
  const dedupKey = dedupKeyFor(event);
  if (seenKeys.has(dedupKey)) {
    return { deduped: true };
  }
  rememberKey(dedupKey);

  buffer.push(toRow(event, dedupKey));

  if (buffer.length >= config.clickhouse.batchSize) {
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    await flush();
  } else {
    scheduleFlush();
  }

  return { deduped: false };
}

async function flushRemaining() {
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = null;
  }
  await flush();
}

module.exports = { write, flushRemaining };
