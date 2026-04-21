#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "usage: $0 <profile> <warehouse_id>" >&2
  exit 1
fi

profile="$1"
warehouse_id="$2"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$repo_root/scripts/load-source-config.sh"
load_convex_sync_source_config "$repo_root"

bootstrap_src="$repo_root/platform/databricks/delta/sql/bootstrap"
render_dir="$(mktemp -d)"

catalog="${DATABRICKS_DELTA_CATALOG:-workspace}"
source_slug="${CONVEX_SYNC_SOURCE_SLUG:-default}"
source_slug_sql="${CONVEX_SYNC_SOURCE_SQL:-${source_slug//-/_}}"
control_schema="${DATABRICKS_DELTA_CONTROL_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_control}"
bronze_schema="${DATABRICKS_DELTA_BRONZE_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_bronze}"
silver_schema="${DATABRICKS_DELTA_SILVER_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_silver}"
checkpoint_table="${DATABRICKS_DELTA_CHECKPOINT_TABLE:-connector_checkpoint}"

"$repo_root/scripts/render-databricks-delta-bootstrap.sh" \
  "$bootstrap_src" \
  "$render_dir" \
  "$catalog" \
  "$control_schema" \
  "$bronze_schema" \
  "$silver_schema" \
  "$checkpoint_table"

"$repo_root/scripts/apply-databricks-sql-dir.sh" "$profile" "$warehouse_id" "$render_dir"

echo "bootstrap complete"
echo "catalog=$catalog"
echo "control_schema=$control_schema"
echo "bronze_schema=$bronze_schema"
echo "silver_schema=$silver_schema"
echo "checkpoint_table=$checkpoint_table"
