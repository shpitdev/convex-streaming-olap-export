#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 2 || "$#" -gt 3 ]]; then
  echo "usage: $0 <profile> <warehouse_id> [dashboard_id]" >&2
  exit 1
fi

profile="$1"
warehouse_id="$2"
dashboard_id="${3:-${DATABRICKS_DELTA_DASHBOARD_ID:-}}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# shellcheck source=/dev/null
source "$repo_root/scripts/load-source-config.sh"
load_convex_sync_source_config "$repo_root"

source_slug="${CONVEX_SYNC_SOURCE_SLUG:-${CONVEX_SYNC_SOURCE:-default}}"
source_label="${CONVEX_SYNC_SOURCE_LABEL:-$source_slug}"
display_name="${DATABRICKS_DELTA_DASHBOARD_NAME:-Convex Sync Overview (${source_label})}"

render_dir="$(mktemp -d)"
trap 'rm -rf "$render_dir"' EXIT
rendered_dashboard="$render_dir/convex_sync_overview.lvdash.json"

"$repo_root/scripts/render-databricks-delta-dashboard.sh" "$rendered_dashboard" >/dev/null

payload="$render_dir/payload.json"
python - "$rendered_dashboard" "$display_name" "$warehouse_id" "$payload" <<'PY'
import json
import pathlib
import sys

dashboard_path = pathlib.Path(sys.argv[1])
display_name = sys.argv[2]
warehouse_id = sys.argv[3]
payload_path = pathlib.Path(sys.argv[4])

payload = {
    "display_name": display_name,
    "warehouse_id": warehouse_id,
    "serialized_dashboard": dashboard_path.read_text(),
}
payload_path.write_text(json.dumps(payload))
PY

if [[ -n "$dashboard_id" ]]; then
  databricks lakeview update "$dashboard_id" -p "$profile" --json @"$payload" >/dev/null
  databricks lakeview publish "$dashboard_id" -p "$profile" --warehouse-id "$warehouse_id" >/dev/null
  echo "updated dashboard_id=$dashboard_id"
else
  create_output="$(databricks lakeview create -p "$profile" --json @"$payload" -o json)"
  dashboard_id="$(jq -r '.dashboard_id' <<<"$create_output")"
  databricks lakeview publish "$dashboard_id" -p "$profile" --warehouse-id "$warehouse_id" >/dev/null
  echo "created dashboard_id=$dashboard_id"
fi

echo "display_name=$display_name"
echo "warehouse_id=$warehouse_id"
