#!/usr/bin/env bash
set -euo pipefail

load_convex_sync_source_config() {
  local repo_root="$1"
  local source_name="${CONVEX_SYNC_SOURCE:-}"

  if [[ -z "$source_name" ]]; then
    return 0
  fi
  local source_file="$repo_root/sources/$source_name/env.sh"

  if [[ ! -f "$source_file" ]]; then
    echo "unknown Convex source config: $source_name" >&2
    return 1
  fi

  # shellcheck source=/dev/null
  source "$source_file"
}
