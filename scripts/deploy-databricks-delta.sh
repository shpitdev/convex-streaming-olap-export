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
deploy_key="${CONVEX_DEPLOY_KEY:-$(read_env_file_value CONVEX_DEPLOY_KEY || true)}"

if [[ -z "$deployment_url" || -z "$deploy_key" ]]; then
  echo "CONVEX_DEPLOYMENT_URL and CONVEX_DEPLOY_KEY are required" >&2
  exit 1
fi

source_id="${CONVEX_SOURCE_ID:-$deployment_url}"
table_name="${CONVEX_TABLE_NAME:-}"
catalog="${DATABRICKS_DELTA_CATALOG:-workspace}"
control_schema="${DATABRICKS_DELTA_CONTROL_SCHEMA:-convex_streaming_olap_export_control}"
bronze_schema="${DATABRICKS_DELTA_BRONZE_SCHEMA:-convex_streaming_olap_export_bronze}"
checkpoint_table="${DATABRICKS_DELTA_CHECKPOINT_TABLE:-connector_checkpoint}"

bundle_args=(
  --var "convex_deployment_url=$deployment_url"
  --var "convex_deploy_key=$deploy_key"
  --var "source_id=$source_id"
  --var "table_name=$table_name"
  --var "catalog=$catalog"
  --var "control_schema=$control_schema"
  --var "bronze_schema=$bronze_schema"
  --var "checkpoint_table=$checkpoint_table"
)

(
  cd "$bundle_root"
  databricks bundle validate -p "$profile" -t "$target" "${bundle_args[@]}"
  DATABRICKS_BUNDLE_ENGINE="$bundle_engine" databricks bundle deploy -p "$profile" -t "$target" "${bundle_args[@]}"
)
