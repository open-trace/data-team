-- Example schema for local partition of the bronze database.
-- Replace with your actual bronze database DDL (simplified from BigQuery if needed).
-- SQLite-compatible; for BigQuery-specific types, map to SQLite equivalents locally.

CREATE TABLE IF NOT EXISTS bronze_events (
    id INTEGER PRIMARY KEY,
    event_ts TEXT,
    source TEXT,
    payload_json TEXT,
    ingested_at TEXT
);
