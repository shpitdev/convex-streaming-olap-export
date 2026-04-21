CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{CONTROL_SCHEMA}}.{{CHECKPOINT_TABLE}} (
  source_id STRING NOT NULL,
  phase STRING NOT NULL,
  snapshot_ts BIGINT,
  snapshot_cursor STRING,
  delta_cursor BIGINT,
  schema_hash STRING,
  run_id STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
USING DELTA;
