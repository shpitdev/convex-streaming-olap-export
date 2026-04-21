#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 3 ]]; then
  echo "usage: $0 <profile> <target> <warehouse_id>" >&2
  exit 1
fi

profile="$1"
target="$2"
warehouse_id="$3"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

preserve_or_unset() {
  local var_name="$1"
  local was_set="$2"
  local prior_value="$3"
  if [[ "$was_set" == "1" ]]; then
    printf -v "$var_name" '%s' "$prior_value"
    export "$var_name"
  else
    unset "$var_name" || true
  fi
}

had_control_schema=0
prior_control_schema=""
if [[ -n "${DATABRICKS_DELTA_CONTROL_SCHEMA+x}" ]]; then
  had_control_schema=1
  prior_control_schema="${DATABRICKS_DELTA_CONTROL_SCHEMA}"
fi

had_bronze_schema=0
prior_bronze_schema=""
if [[ -n "${DATABRICKS_DELTA_BRONZE_SCHEMA+x}" ]]; then
  had_bronze_schema=1
  prior_bronze_schema="${DATABRICKS_DELTA_BRONZE_SCHEMA}"
fi

had_silver_schema=0
prior_silver_schema=""
if [[ -n "${DATABRICKS_DELTA_SILVER_SCHEMA+x}" ]]; then
  had_silver_schema=1
  prior_silver_schema="${DATABRICKS_DELTA_SILVER_SCHEMA}"
fi

had_source_id=0
prior_source_id=""
if [[ -n "${CONVEX_SOURCE_ID+x}" ]]; then
  had_source_id=1
  prior_source_id="${CONVEX_SOURCE_ID}"
fi

# shellcheck source=/dev/null
source "$repo_root/scripts/load-source-config.sh"
load_convex_sync_source_config "$repo_root"
preserve_or_unset DATABRICKS_DELTA_CONTROL_SCHEMA "$had_control_schema" "$prior_control_schema"
preserve_or_unset DATABRICKS_DELTA_BRONZE_SCHEMA "$had_bronze_schema" "$prior_bronze_schema"
preserve_or_unset DATABRICKS_DELTA_SILVER_SCHEMA "$had_silver_schema" "$prior_silver_schema"
preserve_or_unset CONVEX_SOURCE_ID "$had_source_id" "$prior_source_id"
timestamp="$(date +%Y%m%d%H%M%S)"

catalog="${DATABRICKS_DELTA_CATALOG:-workspace}"
source_slug="${CONVEX_SYNC_SOURCE_SLUG:-default}"
source_slug_sql="${CONVEX_SYNC_SOURCE_SQL:-${source_slug//-/_}}"
control_schema="${DATABRICKS_DELTA_CONTROL_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_smoke_${timestamp}_control}"
bronze_schema="${DATABRICKS_DELTA_BRONZE_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_smoke_${timestamp}_bronze}"
silver_schema="${DATABRICKS_DELTA_SILVER_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_smoke_${timestamp}_silver}"
checkpoint_table="${DATABRICKS_DELTA_CHECKPOINT_TABLE:-connector_checkpoint}"
table_name="${CONVEX_TABLE_NAME:-jobs}"
source_id="${CONVEX_SOURCE_ID:-${source_slug}-delta-smoke}"

export DATABRICKS_DELTA_CATALOG="$catalog"
export DATABRICKS_DELTA_CONTROL_SCHEMA="$control_schema"
export DATABRICKS_DELTA_BRONZE_SCHEMA="$bronze_schema"
export DATABRICKS_DELTA_SILVER_SCHEMA="$silver_schema"
export DATABRICKS_DELTA_CHECKPOINT_TABLE="$checkpoint_table"
export CONVEX_SOURCE_ID="$source_id"
export CONVEX_TABLE_NAME="$table_name"
export DATABRICKS_DELTA_DEPLOYMENT_SLUG="${DATABRICKS_DELTA_DEPLOYMENT_SLUG:-${source_slug}-smoke}"
export DATABRICKS_DELTA_JOB_NAME="${DATABRICKS_DELTA_JOB_NAME:-convex-sync-kit-${source_slug}-smoke-delta-extract}"

"$repo_root/scripts/bootstrap-databricks-delta.sh" "$profile" "$warehouse_id"
"$repo_root/scripts/deploy-databricks-delta.sh" "$profile" "$target"
"$repo_root/scripts/run-databricks-delta-job.sh" "$profile" "$target"

verify_dir="$(mktemp -d)"
cat > "$verify_dir/001_checkpoint.sql" <<EOF
SELECT source_id, phase, updated_at
FROM \`${catalog}\`.\`${control_schema}\`.connector_checkpoint_latest
ORDER BY updated_at DESC
LIMIT 5;
EOF
cat > "$verify_dir/002_bronze_tables.sql" <<EOF
SHOW TABLES IN \`${catalog}\`.\`${bronze_schema}\`;
EOF

"$repo_root/scripts/apply-databricks-sql-dir.sh" "$profile" "$warehouse_id" "$verify_dir"

echo "smoke complete"
echo "catalog=$catalog"
echo "control_schema=$control_schema"
echo "bronze_schema=$bronze_schema"
echo "silver_schema=$silver_schema"
echo "checkpoint_table=$checkpoint_table"
echo "table_name=$table_name"
