#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 1 || "$#" -gt 3 ]]; then
  echo "usage: $0 <profile> [scope] [key]" >&2
  exit 1
fi

profile="$1"
scope_arg="${2:-}"
key_arg="${3:-}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

read_env_file_value() {
  local key="$1"
  local env_file="$repo_root/.env"
  if [[ ! -f "$env_file" ]]; then
    return 1
  fi
  local line
  line="$(grep -E "^${key}=" "$env_file" | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    return 1
  fi
  printf '%s' "${line#*=}"
}

scope="${scope_arg:-${DATABRICKS_DELTA_SECRET_SCOPE:-convex-streaming-olap-export}}"
key="${key_arg:-${DATABRICKS_DELTA_SECRET_KEY:-convex-deploy-key}}"
deploy_key="${CONVEX_DEPLOY_KEY:-$(read_env_file_value CONVEX_DEPLOY_KEY || true)}"

scopes_json="$(databricks secrets list-scopes -p "$profile" -o json)"
scope_exists=false
if jq -e --arg scope "$scope" '.[] | select(.name == $scope)' <<<"$scopes_json" >/dev/null; then
  scope_exists=true
fi

if [[ -n "$deploy_key" ]]; then
  if [[ "$scope_exists" == false ]]; then
    databricks secrets create-scope "$scope" -p "$profile" >/dev/null
  fi
  printf '%s' "$deploy_key" | databricks secrets put-secret "$scope" "$key" -p "$profile" >/dev/null
  echo "synced Databricks secret $scope/$key"
  exit 0
fi

if [[ "$scope_exists" == false ]]; then
  echo "Databricks secret scope $scope does not exist and CONVEX_DEPLOY_KEY is not available to create it" >&2
  exit 1
fi

if ! databricks secrets list-secrets "$scope" -p "$profile" -o json | jq -e --arg key "$key" '.[] | select(.key == $key)' >/dev/null; then
  echo "Databricks secret $scope/$key does not exist and CONVEX_DEPLOY_KEY is not available to create it" >&2
  exit 1
fi

echo "using existing Databricks secret $scope/$key"
