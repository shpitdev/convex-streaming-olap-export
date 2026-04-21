#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "usage: $0 <profile> <target>" >&2
  exit 1
fi

profile="$1"
target="$2"
bundle_engine="${DATABRICKS_BUNDLE_ENGINE:-direct}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bundle_root="$repo_root/platform/databricks/delta"

# shellcheck source=/dev/null
source "$repo_root/scripts/load-source-config.sh"
load_convex_sync_source_config "$repo_root"

read_env_file_value() {
  local key="$1"
  local env_file="$repo_root/.env"
  if [[ ! -f "$env_file" ]]; then
    return 1
  fi
  local line
  line="$(grep -E "^${key}=" "$env_file" | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    return 1
  fi
  printf '%s' "${line#*=}"
}

deployment_url="${CONVEX_DEPLOYMENT_URL:-$(read_env_file_value CONVEX_DEPLOYMENT_URL || true)}"
if [[ -z "$deployment_url" ]]; then
  echo "CONVEX_DEPLOYMENT_URL is required" >&2
  exit 1
fi

source_id="${CONVEX_SOURCE_ID:-$deployment_url}"
source_slug="${CONVEX_SYNC_SOURCE_SLUG:-default}"
source_slug_sql="${CONVEX_SYNC_SOURCE_SQL:-${source_slug//-/_}}"
deployment_slug="${DATABRICKS_DELTA_DEPLOYMENT_SLUG:-${source_slug}-${target}}"
job_name="${DATABRICKS_DELTA_JOB_NAME:-convex-sync-kit-${deployment_slug}-delta-extract}"
pipeline_name="${DATABRICKS_DELTA_AUTOCDC_PIPELINE_NAME:-convex-sync-kit-${deployment_slug}-autocdc}"
pipeline_file="${DATABRICKS_DELTA_AUTOCDC_PIPELINE_FILE:-${deployment_slug}-bronze-to-silver.sql}"
generated_file="$bundle_root/generated/$pipeline_file"

"$repo_root/scripts/render-databricks-delta-pipeline.sh" "$profile" "$generated_file" >/dev/null

catalog="${DATABRICKS_DELTA_CATALOG:-workspace}"
table_name="${CONVEX_TABLE_NAME:-}"
secret_scope="${DATABRICKS_DELTA_SECRET_SCOPE:-convex-sync-kit}"
secret_key="${DATABRICKS_DELTA_SECRET_KEY:-convex-deploy-key}"
control_schema="${DATABRICKS_DELTA_CONTROL_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_control}"
bronze_schema="${DATABRICKS_DELTA_BRONZE_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_bronze}"
silver_schema="${DATABRICKS_DELTA_SILVER_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_silver}"
checkpoint_table="${DATABRICKS_DELTA_CHECKPOINT_TABLE:-connector_checkpoint}"

"$repo_root/scripts/ensure-databricks-delta-secret.sh" "$profile" "$secret_scope" "$secret_key"

bundle_args=(
  --var "convex_deployment_url=$deployment_url"
  --var "source_slug=$source_slug"
  --var "job_name=$job_name"
  --var "convex_deploy_key_secret_scope=$secret_scope"
  --var "convex_deploy_key_secret_key=$secret_key"
  --var "source_id=$source_id"
  --var "table_name=$table_name"
  --var "catalog=$catalog"
  --var "control_schema=$control_schema"
  --var "bronze_schema=$bronze_schema"
  --var "silver_schema=$silver_schema"
  --var "checkpoint_table=$checkpoint_table"
  --var "autocdc_pipeline_name=$pipeline_name"
  --var "autocdc_pipeline_file=$pipeline_file"
  --var "deployment_slug=$deployment_slug"
)

(
  cd "$bundle_root"
  DATABRICKS_BUNDLE_ENGINE="$bundle_engine" databricks bundle validate -p "$profile" -t "$target" "${bundle_args[@]}"
  DATABRICKS_BUNDLE_ENGINE="$bundle_engine" databricks bundle deploy -p "$profile" -t "$target" "${bundle_args[@]}"
)
