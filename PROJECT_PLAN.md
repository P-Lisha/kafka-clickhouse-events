# Event Ingestion Service — Kafka (+ ClickHouse) — Project Plan

Standalone service, separate from `server/` (Task Manager) and `client/`.
Lives in its own folder so it can be deployed/scaled independently.

---

## 0. Folder structure (to be created)

```
kafka-clickhouse-events/
├── PROJECT_PLAN.md            <- this file
├── package.json
├── .env.example
├── docker-compose.yml          # local Kafka + ClickHouse + Zookeeper for dev
├── src/
│   ├── config/
│   │   └── env.js
│   ├── api/
│   │   ├── server.js            # Express app bootstrap
│   │   ├── routes/events.js     # POST /api/events
│   │   ├── middleware/auth.js   # token check
│   │   └── validators/event.js  # schema validation (zod/joi)
│   ├── kafka/
│   │   ├── admin/createTopics.js   # topic + ACL provisioning script
│   │   ├── producer.js
│   │   ├── consumer.js             # v1: logs/forwards only
│   │   └── dlq.js                  # dead-letter publish helper
│   ├── clickhouse/              # v2 only
│   │   ├── client.js
│   │   ├── schema.sql
│   │   └── writer.js            # batched insert + idempotency
│   ├── metrics/
│   │   └── prometheus.js        # /metrics endpoint
│   └── utils/logger.js
├── scripts/
│   ├── create-topics.sh
│   └── load-test.js              # k6 or autocannon script
└── tests/
    ├── api.test.js
    ├── producer.test.js
    └── consumer.test.js
```

---

## 1. Milestones (sequenced)

### M0 — Scaffolding (0.5 day)
- Init `package.json`, ESLint, `.env.example`.
- `docker-compose.yml`: Kafka (Bitnami/Confluent image), Zookeeper, ClickHouse — for **local dev only**; prod Kafka/ClickHouse assumed to be provisioned separately.

### M1 — Kafka topic & ACL provisioning (0.5 day)
- Script (`scripts/create-topics.sh` using `kafka-topics.sh` or KafkaJS admin client) to create:
  - `events.raw` — partitions: 6 (tune to expected throughput), replication: 3 (prod) / 1 (dev), retention: `7d` (`retention.ms=604800000`).
  - `events.dlq` — partitions: 3, retention: 14d (longer, for debugging).
- ACLs: separate `producer` principal (Write on `events.raw`) and `consumer` principal (Read on `events.raw`, Write on `events.dlq`, part of a consumer group `events-consumer-group`).

### M2 — Ingestion API (1–1.5 days)
- `POST /api/events` — accepts single object or array of events.
- Validation schema (zod):
  - `eventType: string` (required)
  - `timestamp: ISO string` (optional → defaults to `new Date().toISOString()` server-side)
  - `payload: object` (required)
  - `sourceId: string` (required)
- Auth middleware: `Authorization: Bearer <token>` checked against a static token/env var for v1 (swap for JWT/API-key table later if needed).
- On valid payload → produce to `events.raw` (key = `sourceId` for partition affinity so same source's events land in order on one partition).
- Response: `202 Accepted` with per-event accept/reject status for batch requests.

### M3 — Kafka Producer hardening (0.5 day)
- KafkaJS producer, `acks: -1` (all replicas), retries with backoff.
- Track success/fail counters in-memory → exposed via Prometheus.

### M4 — Kafka Consumer (v1: no storage) (1 day)
- KafkaJS consumer, group `events-consumer-group`.
- Per-message: re-validate shape → process (v1 = log/forward placeholder) → commit offset.
- On processing error: publish original message + error reason to `events.dlq`, then commit (don't block the partition).
- Retry policy: N in-process retries with backoff before DLQ routing.

### M5 — Monitoring (0.5 day)
- `prom-client`: counters (`events_produced_total`, `events_produced_failed_total`, `events_consumed_total`, `events_dlq_total`), histogram (`event_ingest_latency_ms`), consumer lag gauge (poll via KafkaJS admin `fetchOffsets` vs committed offsets, or `kafka-consumer-groups.sh --describe`).
- `GET /metrics` endpoint.

**→ V1 checkpoint: ingestion API + Kafka queue + DLQ + metrics, no persistence.**

---

### M6 — ClickHouse schema (0.5 day)
- `clickhouse/schema.sql`: `events` table (MergeTree, partition by month, order by `(event_time, event_type, source_id)`) as specified.
- Add a `dedup_key` column (e.g. hash of `sourceId+timestamp+eventType` or a client-supplied idempotency key) for de-dup support — consider `ReplacingMergeTree` or app-level "seen key" cache (Redis/local LRU) instead of plain MergeTree if exact-once matters.

### M7 — Consumer → ClickHouse writer (1–1.5 days)
- Buffer consumed messages in memory (e.g. flush every 500 rows or every 2s, whichever first) → bulk `INSERT INTO events ... VALUES` via `@clickhouse/client`.
- Idempotency: check `dedup_key` against a short-lived cache before buffering; skip already-seen keys.
- On insert failure: route the batch to `events.dlq` with the ClickHouse error attached; do not lose the Kafka offset commit ordering (commit only after a successful flush, or use manual offset management per batch).

### M8 — Extended monitoring (0.5 day)
- Add: `clickhouse_insert_latency_ms` histogram, `clickhouse_insert_errors_total`, `events_invalid_payload_total`.
- Sample queries doc (`docs/queries.md`): events/min by type, p95 latency, top sourceIds, etc.

### M9 — Load testing (0.5–1 day)
- `scripts/load-test.js` (k6/autocannon) targeting an agreed X events/sec.
- Record: ingestion API p95/p99, consumer lag under load, ClickHouse insert latency, DLQ rate.

**→ V2 checkpoint: Kafka + ClickHouse storage, idempotent writes, full metrics, load-tested.**

---

## 2. Key technical decisions to confirm with you

1. **Kafka client library**: KafkaJS (pure JS, easier local dev) vs node-rdkafka (native, faster, trickier on Windows). → recommend **KafkaJS** given Windows dev environment.
2. **Auth for v1**: static bearer token via env var, or do you already have a user/API-key system to reuse from `server/`?
3. **Throughput target (X events/sec)** for load testing — needed to size partitions/replicas properly.
4. **Deployment target**: local Docker Compose only, or do you have managed Kafka (Confluent Cloud/MSK) and ClickHouse Cloud already provisioned?

---

## 3. Suggested order of work (next actionable steps)

1. `npm init` + install `kafkajs`, `express`, `zod`, `prom-client`, `dotenv`.
2. Write `docker-compose.yml` (Kafka+Zookeeper+ClickHouse) so you can dev locally without external infra.
3. Build M1 (topic/ACL script) → M2 (API) → M3 (producer) → M4 (consumer+DLQ) → M5 (metrics) = ship V1.
4. Then M6–M9 for V2.

Once you confirm the 4 decisions above, I'll scaffold the actual code (package.json, docker-compose, topic script, API, producer/consumer) inside this folder.
