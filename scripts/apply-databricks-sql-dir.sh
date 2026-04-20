#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <profile> <warehouse_id> <sql_dir>" >&2
  exit 1
fi

profile="$1"
warehouse_id="$2"
sql_dir="$3"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "$sql_dir" != /* ]]; then
  sql_dir="$repo_root/$sql_dir"
fi

if [ ! -d "$sql_dir" ]; then
  echo "sql directory not found: $sql_dir" >&2
  exit 1
fi

find "$sql_dir" -maxdepth 1 -type f -name '*.sql' | sort | while read -r statement; do
  "$repo_root/scripts/databricks-exec-sql-statement.sh" "$profile" "$warehouse_id" "$statement" > "${statement}.result.json"
done
