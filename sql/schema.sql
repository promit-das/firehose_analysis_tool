DROP TABLE IF EXISTS metric_beacon_cadence;

CREATE TABLE IF NOT EXISTS runs (
  run_id VARCHAR PRIMARY KEY,
  app_id VARCHAR NOT NULL,
  source_filename VARCHAR NOT NULL,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  status VARCHAR NOT NULL,
  total_lines BIGINT NOT NULL DEFAULT 0,
  valid_lines BIGINT NOT NULL DEFAULT 0,
  invalid_lines BIGINT NOT NULL DEFAULT 0,
  duplicate_lines BIGINT NOT NULL DEFAULT 0,
  config_json JSON,
  error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS ingest_errors (
  run_id VARCHAR NOT NULL,
  line_no BIGINT NOT NULL,
  error_code VARCHAR NOT NULL,
  error_message VARCHAR NOT NULL,
  raw_line VARCHAR,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events_raw (
  app_id VARCHAR NOT NULL,
  run_id VARCHAR NOT NULL,
  record_uid VARCHAR NOT NULL,
  tenant_id VARCHAR,
  event_type VARCHAR,
  record_ts_ms BIGINT,
  payload_json JSON,
  ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (app_id, run_id, record_uid)
);

CREATE TABLE IF NOT EXISTS events_flat (
  app_id VARCHAR NOT NULL,
  run_id VARCHAR NOT NULL,
  record_uid VARCHAR NOT NULL,
  tenant_id VARCHAR,
  event_type VARCHAR,
  beacon_mac VARCHAR,
  event_ts_ms BIGINT,
  record_ts_ms BIGINT,
  location_id VARCHAR,
  payload_json JSON,
  PRIMARY KEY (app_id, run_id, record_uid)
);

CREATE TABLE IF NOT EXISTS metric_event_counts (
  app_id VARCHAR NOT NULL,
  run_id VARCHAR NOT NULL,
  tenant_id VARCHAR,
  event_type VARCHAR,
  beacon_mac VARCHAR,
  window_start_ms BIGINT,
  window_end_ms BIGINT,
  event_count BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS metric_updates_per_minute (
  app_id VARCHAR NOT NULL,
  run_id VARCHAR NOT NULL,
  tenant_id VARCHAR,
  beacon_mac VARCHAR,
  minute_ts TIMESTAMP NOT NULL,
  updates_count BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS metric_delivery_delay (
  app_id VARCHAR NOT NULL,
  run_id VARCHAR NOT NULL,
  tenant_id VARCHAR,
  event_type VARCHAR,
  beacon_mac VARCHAR,
  sample_count BIGINT NOT NULL,
  p50_delay_ms DOUBLE,
  p95_delay_ms DOUBLE,
  avg_delay_ms DOUBLE,
  max_delay_ms BIGINT,
  breach_count BIGINT NOT NULL,
  negative_delay_count BIGINT NOT NULL,
  threshold_ms BIGINT NOT NULL
);
