#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 <output-file>" >&2
  exit 1
fi

output_file="$1"
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

python - "$repo_root" "$output_file" "$catalog" "$control_schema" "$bronze_schema" "$silver_schema" "$checkpoint_table" "$source_label" <<'PY'
from pathlib import Path
import sys

repo_root = Path(sys.argv[1])
output_file = Path(sys.argv[2])
catalog, control_schema, bronze_schema, silver_schema, checkpoint_table, source_label = sys.argv[3:]
template_path = repo_root / "platform/databricks/delta/dashboards/convex_sync_overview.lvdash.json.tmpl"
template = template_path.read_text()
rendered = (
    template
    .replace("{{CATALOG}}", catalog)
    .replace("{{CONTROL_SCHEMA}}", control_schema)
    .replace("{{BRONZE_SCHEMA}}", bronze_schema)
    .replace("{{SILVER_SCHEMA}}", silver_schema)
    .replace("{{CHECKPOINT_TABLE}}", checkpoint_table)
    .replace("{{SOURCE_LABEL}}", source_label)
)
output_file.parent.mkdir(parents=True, exist_ok=True)
output_file.write_text(rendered)
print(output_file)
PY
