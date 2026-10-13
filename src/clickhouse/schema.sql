CREATE TABLE IF NOT EXISTS events (
    event_date Date,
    event_time DateTime,
    event_type String,
    source_id String,
    payload String,          -- JSON stored as String; use JSONExtract* in queries
    dedup_key String,        -- hash of sourceId+timestamp+eventType, used for idempotency
    ingest_time DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_time, event_type, source_id);
