#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="$repo_root/ops/databricks"
label="${1:-templates}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
dest_dir="$repo_root/.memory/databricks-template-snapshots/${timestamp}-${label}"

mkdir -p "$dest_dir"
cp -R "$source_dir" "$dest_dir/databricks"

manifest_path="$dest_dir/MANIFEST.txt"
{
  echo "snapshot_time_utc=$timestamp"
  echo "label=$label"
  echo "source_dir=$source_dir"
  echo "snapshot_dir=$dest_dir/databricks"
  find "$dest_dir/databricks" -type f | sort | sed "s|$dest_dir/||"
} > "$manifest_path"

echo "$dest_dir"
