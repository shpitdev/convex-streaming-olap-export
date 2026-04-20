CREATE OR REPLACE VIEW {{CATALOG}}.{{CONTROL_SCHEMA}}.connector_checkpoint_latest AS
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY source_id
      ORDER BY updated_at DESC, run_id DESC
    ) AS row_num
  FROM {{CATALOG}}.{{CONTROL_SCHEMA}}.{{CHECKPOINT_TABLE}}
)
SELECT
  source_id,
  phase,
  snapshot_ts,
  snapshot_cursor,
  delta_cursor,
  schema_hash,
  run_id,
  updated_at
FROM ranked
WHERE row_num = 1;
