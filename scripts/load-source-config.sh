#!/usr/bin/env bash
set -euo pipefail

load_convex_sync_source_config() {
  local repo_root="$1"
  local source_name="${CONVEX_SYNC_SOURCE:-meshix-api}"
  local source_file="$repo_root/sources/$source_name/env.sh"

  if [[ ! -f "$source_file" ]]; then
    if [[ -n "${CONVEX_SYNC_SOURCE:-}" ]]; then
      echo "unknown Convex source config: $source_name" >&2
      return 1
    fi
    return 0
  fi

  # shellcheck source=/dev/null
  source "$source_file"
}
