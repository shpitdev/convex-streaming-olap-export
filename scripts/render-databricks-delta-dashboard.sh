#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
  echo "usage: $0 <output-file> [profile]" >&2
  exit 1
fi

output_file="$1"
profile="${2:-${DATABRICKS_PROFILE:-}}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "$repo_root/scripts/load-source-config.sh"
load_convex_sync_source_config "$repo_root"

catalog="${DATABRICKS_DELTA_CATALOG:-workspace}"
source_slug="${CONVEX_SYNC_SOURCE_SLUG:-default}"
source_slug_sql="${CONVEX_SYNC_SOURCE_SQL:-${source_slug//-/_}}"
control_schema="${DATABRICKS_DELTA_CONTROL_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_control}"
bronze_schema="${DATABRICKS_DELTA_BRONZE_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_bronze}"
silver_schema="${DATABRICKS_DELTA_SILVER_SCHEMA:-convex_sync_kit_${source_slug_sql}_delta_silver}"
checkpoint_table="${DATABRICKS_DELTA_CHECKPOINT_TABLE:-connector_checkpoint}"
source_label="${CONVEX_SYNC_SOURCE_LABEL:-$source_slug}"

python - "$repo_root" "$output_file" "$catalog" "$control_schema" "$bronze_schema" "$silver_schema" "$checkpoint_table" "$source_label" "$profile" <<'PY'
from pathlib import Path
import json
import subprocess
import sys

repo_root = Path(sys.argv[1])
output_file = Path(sys.argv[2])
catalog, control_schema, bronze_schema, silver_schema, checkpoint_table, source_label, profile = sys.argv[3:]
template_path = repo_root / "platform/databricks/delta/dashboards/convex_sync_overview.lvdash.json.tmpl"
template = template_path.read_text()


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_ident(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def qualify(*parts: str) -> str:
    return ".".join(sql_ident(part) for part in parts)


def empty_row_counts_query() -> str:
    return (
        "SELECT CAST(NULL AS STRING) AS logical_table, "
        "CAST(NULL AS STRING) AS bronze_table, "
        "CAST(NULL AS BIGINT) AS bronze_row_count, "
        "CAST(NULL AS STRING) AS silver_table, "
        "CAST(NULL AS BIGINT) AS silver_row_count "
        "WHERE 1 = 0"
    )


def load_tables(schema: str) -> list[dict]:
    if not profile:
        return []
    command = [
        "databricks",
        "tables",
        "list",
        catalog,
        schema,
        "-p",
        profile,
        "-o",
        "json",
        "--omit-columns",
        "--omit-properties",
        "--omit-username",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stderr.strip(), file=sys.stderr)
        raise SystemExit(result.returncode)
    return json.loads(result.stdout)


def include_table(table: dict) -> bool:
    name = table.get("name") or ""
    table_type = table.get("table_type") or ""
    if not name:
        return False
    if table_type == "VIEW":
        return False
    if name.startswith("__"):
        return False
    if name.startswith("event_log_"):
        return False
    return True


def logical_name_from_bronze(name: str) -> str:
    return name[:-5] if name.endswith("__cdc") else name


row_counts_query = empty_row_counts_query()
if profile:
    bronze_tables = [table for table in load_tables(bronze_schema) if include_table(table)]
    silver_tables = [table for table in load_tables(silver_schema) if include_table(table)]

    bronze_by_logical = {logical_name_from_bronze(table["name"]): table["name"] for table in bronze_tables}
    silver_by_logical = {table["name"]: table["name"] for table in silver_tables}
    logical_names = sorted(set(bronze_by_logical) | set(silver_by_logical))

    if logical_names:
        selects: list[str] = []
        for logical_name in logical_names:
            bronze_name = bronze_by_logical.get(logical_name)
            silver_name = silver_by_logical.get(logical_name)

            bronze_name_sql = sql_string(bronze_name) if bronze_name else "CAST(NULL AS STRING)"
            bronze_count_sql = (
                f"(SELECT COUNT(*) FROM {qualify(catalog, bronze_schema, bronze_name)})"
                if bronze_name
                else "CAST(NULL AS BIGINT)"
            )
            silver_name_sql = sql_string(silver_name) if silver_name else "CAST(NULL AS STRING)"
            silver_count_sql = (
                f"(SELECT COUNT(*) FROM {qualify(catalog, silver_schema, silver_name)})"
                if silver_name
                else "CAST(NULL AS BIGINT)"
            )

            selects.append(
                "SELECT "
                f"{sql_string(logical_name)} AS logical_table, "
                f"{bronze_name_sql} AS bronze_table, "
                f"{bronze_count_sql} AS bronze_row_count, "
                f"{silver_name_sql} AS silver_table, "
                f"{silver_count_sql} AS silver_row_count"
            )
        row_counts_query = " UNION ALL ".join(selects) + " ORDER BY logical_table"

rendered = (
    template
    .replace("{{CATALOG}}", catalog)
    .replace("{{CONTROL_SCHEMA}}", control_schema)
    .replace("{{BRONZE_SCHEMA}}", bronze_schema)
    .replace("{{SILVER_SCHEMA}}", silver_schema)
    .replace("{{CHECKPOINT_TABLE}}", checkpoint_table)
    .replace("{{SOURCE_LABEL}}", source_label)
    .replace("{{TABLE_ROW_COUNTS_SOURCE}}", row_counts_query)
)
output_file.parent.mkdir(parents=True, exist_ok=True)
output_file.write_text(rendered)
print(output_file)
PY
