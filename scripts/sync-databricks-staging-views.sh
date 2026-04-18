#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

profile="${DATABRICKS_PROFILE:-DEFAULT}"
warehouse_id="${DATABRICKS_WAREHOUSE_ID:-}"
catalog="${DATABRICKS_CATALOG:-workspace}"
schema="${DATABRICKS_SCHEMA:-convex_streaming_olap_export}"
bucket="${S3_BUCKET:-}"
prefix="${S3_PREFIX:-}"
label="${LABEL:-sync}"
apply="${APPLY:-1}"

usage() {
  cat >&2 <<'EOF'
usage: sync-databricks-staging-views.sh --warehouse-id <id> --bucket <bucket> [options]

options:
  --profile <profile>
  --warehouse-id <id>
  --catalog <catalog>
  --schema <schema>
  --bucket <bucket>
  --prefix <prefix>
  --label <label>
  --render-only
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --profile) profile="$2"; shift 2 ;;
    --warehouse-id) warehouse_id="$2"; shift 2 ;;
    --catalog) catalog="$2"; shift 2 ;;
    --schema) schema="$2"; shift 2 ;;
    --bucket) bucket="$2"; shift 2 ;;
    --prefix) prefix="$2"; shift 2 ;;
    --label) label="$2"; shift 2 ;;
    --render-only) apply=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

if [ -z "$warehouse_id" ] || [ -z "$bucket" ]; then
  usage
  exit 1
fi

normalized_prefix="${prefix#/}"
normalized_prefix="${normalized_prefix%/}"
manifest_key="staging/manifests/latest.json"
if [ -n "$normalized_prefix" ]; then
  manifest_s3="s3://${bucket}/${normalized_prefix}/${manifest_key}"
else
  manifest_s3="s3://${bucket}/${manifest_key}"
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
work_dir="$repo_root/.memory/databricks-view-sync/${timestamp}-${label}"
mkdir -p "$work_dir/statements"

manifest_path="$work_dir/latest.json"
aws s3 cp "$manifest_s3" "$manifest_path" >/dev/null

header_path="$work_dir/statements/000_schema.sql"
sed \
  -e "s|{{CATALOG}}|$catalog|g" \
  -e "s|{{SCHEMA}}|$schema|g" \
  -e "s|{{BUCKET}}|$bucket|g" \
  -e "s|{{PREFIX}}|$normalized_prefix|g" \
  "$repo_root/ops/databricks/sql/register_staging_views.sql.tmpl" > "$header_path"

sanitize_table_name() {
  local relative_path="$1"
  local without_ext="${relative_path%.parquet}"
  without_ext="${without_ext#_root/}"
  without_ext="${without_ext//\//__}"
  without_ext="$(sed -E 's/[^A-Za-z0-9_]+/_/g' <<<"$without_ext")"
  if [[ "$without_ext" =~ ^[0-9] ]]; then
    without_ext="t__${without_ext}"
  fi
  echo "$without_ext"
}

map_path="$work_dir/statements/001_source_map.sql"
{
  echo "CREATE OR REPLACE VIEW \`$catalog\`.\`$schema\`.\`__source_map\` AS"
  echo "SELECT * FROM VALUES"
  mapfile -t source_rows < <(
    jq -r '
      .tables
      | to_entries
      | sort_by(.key)
      | to_entries[]
      | [
          .value.value.relative_path,
          .value.value.current_key,
          .value.value.versioned_key,
          .value.value.sha256,
          (.value.value.bytes | tostring),
          (.key | tostring)
        ]
      | @tsv
    ' "$manifest_path"
  )
  for idx in "${!source_rows[@]}"; do
    IFS=$'\t' read -r relative_path current_key versioned_key sha256 bytes ordinal <<<"${source_rows[$idx]}"
    comma=","
    if [ "$idx" -eq "$(( ${#source_rows[@]} - 1 ))" ]; then
      comma=""
    fi
    printf "  ('%s', '%s', '%s', '%s', %s, %s)%s\n" \
      "${relative_path//\'/\'\'}" \
      "${current_key//\'/\'\'}" \
      "${versioned_key//\'/\'\'}" \
      "${sha256//\'/\'\'}" \
      "$bytes" \
      "$ordinal" \
      "$comma"
  done
  echo "AS source_map(relative_path, current_key, versioned_key, sha256, bytes, ordinal);"
} > "$map_path"

jq -r '.tables | to_entries | sort_by(.key)[] | [.key, .value.current_key] | @tsv' "$manifest_path" |
while IFS=$'\t' read -r relative_path current_key; do
  table_name="$(sanitize_table_name "$relative_path")"
  statement_path="$work_dir/statements/${table_name}.sql"
  s3_uri="s3://${bucket}/${current_key}"
  cat > "$statement_path" <<EOF
CREATE OR REPLACE VIEW \`$catalog\`.\`$schema\`.\`$table_name\` AS
SELECT * FROM read_files('$s3_uri', format => 'parquet');
EOF
done

manifest_report="$work_dir/MANIFEST.txt"
{
  echo "render_time_utc=$timestamp"
  echo "profile=$profile"
  echo "warehouse_id=$warehouse_id"
  echo "catalog=$catalog"
  echo "schema=$schema"
  echo "bucket=$bucket"
  echo "prefix=$normalized_prefix"
  echo "manifest_s3=$manifest_s3"
  find "$work_dir/statements" -type f | sort | sed "s|$work_dir/||"
} > "$manifest_report"

echo "$work_dir"

if [ "$apply" != "1" ]; then
  exit 0
fi

find "$work_dir/statements" -type f | sort | while read -r statement; do
  "$repo_root/scripts/databricks-exec-sql-statement.sh" "$profile" "$warehouse_id" "$statement" > "${statement}.result.json"
done
