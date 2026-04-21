#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "usage: $0 <profile> <output-file>" >&2
  exit 1
fi

profile="$1"
output_file="$2"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "$repo_root/scripts/load-source-config.sh"
load_convex_sync_source_config "$repo_root"

catalog="${DATABRICKS_DELTA_CATALOG:-workspace}"
source_slug="${CONVEX_SYNC_SOURCE_SLUG:-default}"
source_slug_sql="${CONVEX_SYNC_SOURCE_SQL:-${source_slug//-/_}}"
bronze_schema="${DATABRICKS_DELTA_BRONZE_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_bronze}"
silver_schema="${DATABRICKS_DELTA_SILVER_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_silver}"
config_file="$repo_root/sources/${CONVEX_SYNC_SOURCE:-}/delta-pipeline.json"
template_file="$repo_root/platform/databricks/delta/lakeflow/bronze_to_silver_template.sql"

tables_json="$(databricks tables list "$catalog" "$bronze_schema" -p "$profile" -o json --omit-columns --omit-properties --omit-username)"

python - "$tables_json" "$template_file" "$output_file" "$catalog" "$bronze_schema" "$silver_schema" "$config_file" "$source_slug" <<'PY'
import json
import pathlib
import re
import sys

tables = json.loads(sys.argv[1])
template_path = pathlib.Path(sys.argv[2])
output_path = pathlib.Path(sys.argv[3])
catalog = sys.argv[4]
bronze_schema = sys.argv[5]
silver_schema = sys.argv[6]
config_path = pathlib.Path(sys.argv[7])
source_slug = sys.argv[8]

config = {
    "include_tables": [],
    "exclude_tables": [],
    "target_table_overrides": {},
    "flow_name_overrides": {},
}
if config_path.name != "delta-pipeline.json":
    # no source selected; keep defaults
    pass
elif config_path.exists():
    config.update(json.loads(config_path.read_text()))

include_tables = set(config.get("include_tables") or [])
exclude_tables = set(config.get("exclude_tables") or [])
target_overrides = dict(config.get("target_table_overrides") or {})
flow_overrides = dict(config.get("flow_name_overrides") or {})

def sanitize_identifier(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_]", "_", value)
    if re.match(r"^[0-9]", value):
        value = f"t_{value}"
    return value

def target_name_for(bronze_table: str) -> str:
    if bronze_table in target_overrides:
        return target_overrides[bronze_table]
    if bronze_table.endswith("__cdc"):
        return bronze_table[:-5]
    return bronze_table

sections = []
for table in tables:
    name = table["name"]
    if table.get("table_type") == "VIEW":
        continue
    if not name.endswith("__cdc"):
        continue
    if include_tables and name not in include_tables:
        continue
    if name in exclude_tables:
        continue

    target_name = target_name_for(name)
    flow_name = flow_overrides.get(name, f"autocdc_{sanitize_identifier(target_name)}")
    target_table = f"`{target_name}`"
    source_table = f"`{catalog}`.`{bronze_schema}`.`{name}`"
    rendered = (
        template_path.read_text()
        .replace("{{TARGET_TABLE}}", target_table)
        .replace("{{FLOW_NAME}}", sanitize_identifier(flow_name))
        .replace("{{SOURCE_TABLE}}", source_table)
    )
    sections.append(f"-- source={source_slug} bronze_table={name} silver_table={target_name}\n{rendered.strip()}")

if not sections:
    raise SystemExit(f"no AUTO CDC tables selected from {catalog}.{bronze_schema}")

output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text("\n\n".join(sections) + "\n")
print(output_path)
PY
