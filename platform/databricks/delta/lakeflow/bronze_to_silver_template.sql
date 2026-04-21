-- Replace the {{...}} placeholders per table/component pair.
--
-- Expected bronze columns:
--   _cdc_document_id
--   _cdc_sequence_num
--   _cdc_is_deleted
--   _cdc_schema_fingerprint
--   _cdc_source_component
--   _cdc_source_table
--   _cdc_raw_document_json
--   <flattened business columns...>

CREATE OR REFRESH STREAMING TABLE {{TARGET_TABLE}};

CREATE FLOW {{FLOW_NAME}} AS
AUTO CDC INTO {{TARGET_TABLE}}
FROM STREAM({{SOURCE_TABLE}})
KEYS (_cdc_document_id)
APPLY AS DELETE WHEN _cdc_is_deleted
SEQUENCE BY _cdc_sequence_num
COLUMNS * EXCEPT (
  _cdc_is_deleted,
  _cdc_sequence_num,
  _cdc_schema_fingerprint,
  _cdc_source_component,
  _cdc_source_table,
  _cdc_raw_document_json,
  _cdc_ingested_at,
  _cdc_run_id,
  _cdc_source_id,
  _cdc_creation_time
)
STORED AS SCD TYPE 1;
