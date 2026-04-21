# Sources

This repo is a generic Convex sync engine. Source-specific defaults live here
so one checkout can manage multiple Convex deployments without renaming the
repo or duplicating the platform logic.

Each source directory should contain an `env.sh` that exports guarded defaults
using `: "${VAR:=value}"`.

Recommended contents:

- `CONVEX_SYNC_SOURCE`
- `CONVEX_SYNC_SOURCE_SLUG`
- `CONVEX_SYNC_SOURCE_SQL`
- `CONVEX_SOURCE_ID`
- `DATABRICKS_S3_SCHEMA`
- `DATABRICKS_DELTA_SECRET_SCOPE`
- `DATABRICKS_DELTA_SECRET_KEY`
- `DATABRICKS_DELTA_CATALOG`
- `DATABRICKS_DELTA_CONTROL_SCHEMA`
- `DATABRICKS_DELTA_BRONZE_SCHEMA`
- `DATABRICKS_DELTA_SILVER_SCHEMA`
- `DATABRICKS_DELTA_CHECKPOINT_TABLE`

Scripts load `sources/${CONVEX_SYNC_SOURCE:-meshix-api}/env.sh` automatically.
Explicit environment variables still win because the source files only set
defaults.
