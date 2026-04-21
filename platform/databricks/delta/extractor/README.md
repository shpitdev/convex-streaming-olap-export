# Convex CDC Job

`convex_cdc_job.py` is the Databricks job entrypoint for the Databricks Delta
target family.

It mirrors the current Rust source/checkpoint behavior:

- fetch `json_schemas`
- resume or start `list_snapshot`
- hand off to `document_deltas`
- append bronze CDC rows
- advance the checkpoint only after the bronze write succeeds

## Required environment

- `CONVEX_DEPLOYMENT_URL`
- one of:
  - `CONVEX_DEPLOY_KEY`
  - `CONVEX_DEPLOY_KEY_SECRET_SCOPE` and `CONVEX_DEPLOY_KEY_SECRET_KEY`

## Optional environment

- `CONVEX_SOURCE_ID`: defaults to the deployment URL
- `CONVEX_TABLE_NAME`: limit extraction to one table for smoke/debug runs
- `DATABRICKS_CATALOG`
- `DATABRICKS_CONTROL_SCHEMA`: defaults to `control`
- `DATABRICKS_BRONZE_SCHEMA`: defaults to `bronze`
- `DATABRICKS_CHECKPOINT_TABLE`: defaults to `connector_checkpoint`

In the bundled Databricks Delta path, the job receives the secret scope/key
names as task parameters and resolves the actual deploy key with
`dbutils.secrets.get(...)` at runtime.
