# Event Ingestion Service — Kafka (+ ClickHouse)

Standalone service. Two processes:

- **API** (`src/api/server.js`) — accepts events over HTTP, validates, produces to Kafka.
- **Consumer worker** (`src/consumerWorker.js`) — reads from Kafka; Version 1 just
  logs/no-ops, Version 2 (`STORAGE_ENABLED=true`) writes batches to ClickHouse.

See [PROJECT_PLAN.md](./PROJECT_PLAN.md) for the full milestone breakdown.

---

## Setup

```bash
cd kafka-clickhouse-events
npm install
cp .env.example .env
```

### Local infra — option A: Docker

```bash
docker compose up -d
```

### Local infra — option B: no Docker (native Kafka + ClickHouse in WSL)

Used when Docker Desktop isn't available. One-time setup:

```bash
# 1. Portable JDK 21 (Kafka needs a JVM) -- download+extract into local-infra/
mkdir -p local-infra && cd local-infra
curl -L -o jdk.zip "https://api.adoptium.net/v3/binary/latest/21/ga/windows/x64/jdk/hotspot/normal/eclipse"
powershell -Command "Expand-Archive -Path jdk.zip -DestinationPath . -Force"

# 2. Kafka binary (KRaft mode, no Zookeeper needed from Kafka 4.x onward)
curl -L -o kafka.tgz "https://dlcdn.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz"
tar -xzf kafka.tgz
cd ..

# 3. Point Kafka's log.dirs at a local folder (default is /tmp) and format storage
sed -i "s|^log.dirs=.*|log.dirs=$(pwd)/local-infra/kafka-data|" local-infra/kafka_2.13-4.3.1/config/server.properties
bash scripts/format-kafka-storage.sh   # one-time; re-running wipes topic data

# 4. ClickHouse has no official native Windows server build -- run it inside WSL instead.
wsl --install -d Ubuntu-24.04
wsl -d Ubuntu-24.04 -u root -- apt-get update
wsl -d Ubuntu-24.04 -u root -- apt-get install -y apt-transport-https ca-certificates dirmngr gnupg
wsl -d Ubuntu-24.04 -u root -- curl -fsSL -o /usr/share/keyrings/clickhouse-keyring.gpg \
  "https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key"
wsl -d Ubuntu-24.04 -u root -- bash -c "mv /usr/share/keyrings/clickhouse-keyring.gpg /tmp/ck.asc && gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg /tmp/ck.asc"
wsl -d Ubuntu-24.04 -u root -- bash -c "echo 'deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=amd64] https://packages.clickhouse.com/deb stable main' > /etc/apt/sources.list.d/clickhouse.list"
wsl -d Ubuntu-24.04 -u root -- apt-get update
wsl -d Ubuntu-24.04 -u root -- apt-get install -y clickhouse-server clickhouse-client
# Bind to 0.0.0.0 so Windows can reach it, and set a password (default is empty):
wsl -d Ubuntu-24.04 -u root -- bash -c "echo '<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>' > /etc/clickhouse-server/config.d/listen.xml"
wsl -d Ubuntu-24.04 -u root -- bash -c "echo '<clickhouse><users><default><password>clickhouse123</password></default></users></clickhouse>' > /etc/clickhouse-server/users.d/default-password.xml"
```

Every time you want to run the stack without Docker:

```bash
bash scripts/start-kafka-native.sh      # starts the broker on localhost:9092
bash scripts/start-clickhouse-wsl.sh    # starts ClickHouse on localhost:8123 (+ keeps the WSL VM warm)
```

`start-clickhouse-wsl.sh` also launches a `sleep 999999` keep-alive inside the WSL
distro — WSL2 normally shuts its VM down ~8s after the last attached process exits,
which would otherwise restart (and briefly disconnect) `clickhouse-server` between
commands.

### Create Kafka topics

```bash
npm run topics:create
```

Creates `events.raw` (6 partitions, 7-day retention) and `events.dlq` (3 partitions,
14-day retention). ACLs require a secured cluster (SASL + authorizer) — see
`scripts/kafka-acls.sh` for the reference commands to run against staging/prod.

### Run Version 1 (queue only)

```bash
# .env: STORAGE_ENABLED=false
npm run start:api        # terminal 1
npm run start:consumer   # terminal 2
```

```bash
curl -X POST http://localhost:4000/api/events \
  -H "Authorization: Bearer changeme-dev-token" \
  -H "Content-Type: application/json" \
  -d '{"eventType":"order.created","payload":{"amount":10},"sourceId":"svc-1"}'
```

Batch:

```bash
curl -X POST http://localhost:4000/api/events \
  -H "Authorization: Bearer changeme-dev-token" \
  -H "Content-Type: application/json" \
  -d '[{"eventType":"order.created","payload":{},"sourceId":"svc-1"},
       {"eventType":"order.updated","payload":{},"sourceId":"svc-2"}]'
```

### Run Version 2 (+ ClickHouse storage)

```bash
npm run clickhouse:schema   # creates the events table
# .env: STORAGE_ENABLED=true
npm run start:api
npm run start:consumer
```

Query examples: [docs/queries.md](./docs/queries.md).

### Metrics

```
GET http://localhost:4000/metrics
```

Prometheus-format: produce success/fail, consume count, DLQ count, invalid-payload
count, ingest latency histogram, and (Version 2) ClickHouse insert latency/errors.

### Load test

```bash
TARGET_RPS=200 DURATION_SEC=30 node scripts/load-test.js
```

### Tests

```bash
npm test
```

---

## Folder structure

```
kafka-clickhouse-events/
├── docker-compose.yml       # local Kafka + ClickHouse (option A)
├── local-infra/              # portable JDK + Kafka binary (option B, gitignored)
├── src/
│   ├── config/env.js
│   ├── api/                 # Express ingestion API
│   ├── kafka/                # producer, consumer, DLQ, topic admin
│   ├── clickhouse/            # schema, client, batched writer
│   ├── metrics/prometheus.js
│   └── consumerWorker.js     # consumer process entrypoint
├── scripts/                  # ACL reference, load test, native start/format scripts
├── docs/queries.md
└── tests/
```
