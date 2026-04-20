-- Replace the {{...}} placeholders per table/component pair.
--
-- Expected bronze columns:
--   document_id
--   sequence_num
--   is_deleted
--   schema_fingerprint
--   source_component
--   source_table
--   raw_document_json
--   <flattened business columns...>

CREATE OR REFRESH STREAMING TABLE {{TARGET_TABLE}};

CREATE FLOW {{FLOW_NAME}} AS
AUTO CDC INTO {{TARGET_TABLE}}
FROM STREAM({{SOURCE_TABLE}})
KEYS (document_id)
APPLY AS DELETE WHEN is_deleted
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (
  is_deleted,
  sequence_num,
  schema_fingerprint,
  source_component,
  source_table,
  raw_document_json,
  ingested_at,
  run_id,
  source_id
)
STORED AS SCD TYPE 1;
