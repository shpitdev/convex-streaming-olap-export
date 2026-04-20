# Convex CDC Job

`convex_cdc_job.py` is the Databricks job entrypoint for the Databricks-native
target family.

It mirrors the current Rust source/checkpoint behavior:

- fetch `json_schemas`
- resume or start `list_snapshot`
- hand off to `document_deltas`
- append bronze CDC rows
- advance the checkpoint only after the bronze write succeeds

## Required environment

- `CONVEX_DEPLOYMENT_URL`
- `CONVEX_DEPLOY_KEY`

## Optional environment

- `CONVEX_SOURCE_ID`: defaults to the deployment URL
- `CONVEX_TABLE_NAME`: limit extraction to one table for smoke/debug runs
- `DATABRICKS_CATALOG`
- `DATABRICKS_CONTROL_SCHEMA`: defaults to `control`
- `DATABRICKS_BRONZE_SCHEMA`: defaults to `bronze`
- `DATABRICKS_CHECKPOINT_TABLE`: defaults to `connector_checkpoint`
