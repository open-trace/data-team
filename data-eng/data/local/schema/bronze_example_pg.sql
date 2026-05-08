-- Example bronze schema for local PostgreSQL (partition of the bronze database).
-- Replace with your actual bronze database DDL simplified for Postgres.
-- Run this in your local DB (e.g. datateam_local) before or after syncing data.

CREATE TABLE IF NOT EXISTS bronze_events (
    id BIGSERIAL PRIMARY KEY,
    event_ts TIMESTAMPTZ,
    source TEXT,
    payload_json JSONB,
    ingested_at TIMESTAMPTZ
);

-- Optional: index for common filters (e.g. partition by date)
-- CREATE INDEX IF NOT EXISTS idx_bronze_events_event_ts ON bronze_events (event_ts);
