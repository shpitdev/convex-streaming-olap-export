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
bootstrap_src="$repo_root/platform/databricks/delta/sql/bootstrap"
render_dir="$(mktemp -d)"
timestamp="$(date +%Y%m%d%H%M%S)"

catalog="${DATABRICKS_DELTA_CATALOG:-workspace}"
control_schema="${DATABRICKS_DELTA_CONTROL_SCHEMA:-convex_streaming_olap_export_delta_smoke_${timestamp}_control}"
bronze_schema="${DATABRICKS_DELTA_BRONZE_SCHEMA:-convex_streaming_olap_export_delta_smoke_${timestamp}_bronze}"
silver_schema="${DATABRICKS_DELTA_SILVER_SCHEMA:-convex_streaming_olap_export_delta_smoke_${timestamp}_silver}"
checkpoint_table="${DATABRICKS_DELTA_CHECKPOINT_TABLE:-connector_checkpoint}"
table_name="${CONVEX_TABLE_NAME:-jobs}"
source_id="${CONVEX_SOURCE_ID:-convex-streaming-olap-export-delta-smoke}"

export DATABRICKS_DELTA_CATALOG="$catalog"
export DATABRICKS_DELTA_CONTROL_SCHEMA="$control_schema"
export DATABRICKS_DELTA_BRONZE_SCHEMA="$bronze_schema"
export DATABRICKS_DELTA_SILVER_SCHEMA="$silver_schema"
export DATABRICKS_DELTA_CHECKPOINT_TABLE="$checkpoint_table"
export CONVEX_SOURCE_ID="$source_id"
export CONVEX_TABLE_NAME="$table_name"

"$repo_root/scripts/render-databricks-delta-bootstrap.sh" \
  "$bootstrap_src" \
  "$render_dir" \
  "$catalog" \
  "$control_schema" \
  "$bronze_schema" \
  "$silver_schema" \
  "$checkpoint_table"

"$repo_root/scripts/apply-databricks-sql-dir.sh" "$profile" "$warehouse_id" "$render_dir"
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
