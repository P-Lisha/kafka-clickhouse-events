# Sample ClickHouse queries

```sql
-- Events per minute, last hour, by type
SELECT
    toStartOfMinute(event_time) AS minute,
    event_type,
    count() AS cnt
FROM events
WHERE event_time >= now() - INTERVAL 1 HOUR
GROUP BY minute, event_type
ORDER BY minute;

-- Top source IDs by volume today
SELECT source_id, count() AS cnt
FROM events
WHERE event_date = today()
GROUP BY source_id
ORDER BY cnt DESC
LIMIT 20;

-- Extract a field from the JSON payload
SELECT
    event_type,
    JSONExtractString(payload, 'status') AS status,
    count() AS cnt
FROM events
WHERE event_date = today()
GROUP BY event_type, status;

-- p95 ingest-to-storage delay (ingest_time - event_time), last 24h
SELECT quantile(0.95)(dateDiff('second', event_time, ingest_time)) AS p95_delay_seconds
FROM events
WHERE event_time >= now() - INTERVAL 1 DAY;
```
