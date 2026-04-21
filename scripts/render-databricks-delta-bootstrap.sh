#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 7 ]]; then
  echo "usage: $0 <src_dir> <dest_dir> <catalog> <control_schema> <bronze_schema> <silver_schema> <checkpoint_table>" >&2
  exit 1
fi

src_dir="$1"
dest_dir="$2"
catalog="$3"
control_schema="$4"
bronze_schema="$5"
silver_schema="$6"
checkpoint_table="$7"

mkdir -p "$dest_dir"

find "$src_dir" -maxdepth 1 -type f -name '*.sql' | sort | while read -r src; do
  dest="$dest_dir/$(basename "$src")"
  python - "$src" "$dest" "$catalog" "$control_schema" "$bronze_schema" "$silver_schema" "$checkpoint_table" <<'PY'
import pathlib
import sys

src = pathlib.Path(sys.argv[1])
dest = pathlib.Path(sys.argv[2])
catalog, control_schema, bronze_schema, silver_schema, checkpoint_table = sys.argv[3:]

rendered = src.read_text()
rendered = rendered.replace("{{CATALOG}}", catalog)
rendered = rendered.replace("{{CONTROL_SCHEMA}}", control_schema)
rendered = rendered.replace("{{BRONZE_SCHEMA}}", bronze_schema)
rendered = rendered.replace("{{SILVER_SCHEMA}}", silver_schema)
rendered = rendered.replace("{{CHECKPOINT_TABLE}}", checkpoint_table)
dest.write_text(rendered)
PY
done
