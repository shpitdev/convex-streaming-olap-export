#!/usr/bin/env bash
set -euo pipefail

repo="${1:?usage: resolve-prerelease-version.sh <repo> [base-branch] [head-sha]}"
base_branch="${2:-main}"
head_sha="${3:-}"

if ! command -v gh >/dev/null 2>&1; then
  printf 'gh is required to resolve prerelease versions\n' >&2
  exit 1
fi

escape_ere() {
  printf '%s' "$1" | sed 's/[.[\*^$()+?{|\\/]/\\&/g'
}

latest_open_release_version() {
  local title
  title="$(
    gh pr list \
      --repo "$repo" \
      --state open \
      --base "$base_branch" \
      --label "autorelease: pending" \
      --json title,updatedAt \
      --jq 'sort_by(.updatedAt) | last.title // ""'
  )"

  printf '%s\n' "$title" | sed -En 's/^chore\([^)]*\): release ([0-9]+\.[0-9]+\.[0-9]+)$/\1/p'
}

latest_stable_version() {
  gh api "repos/${repo}/releases?per_page=100" \
    --jq 'map(select((.draft | not) and (.prerelease | not))) | sort_by(.published_at) | last.tag_name // ""' \
    2>/dev/null || true
}

bump_patch() {
  local version="${1#v}" major minor patch
  IFS=. read -r major minor patch <<<"$version"
  printf '%s.%s.%s\n' "$major" "$minor" "$((patch + 1))"
}

base_version="$(latest_open_release_version)"
if [[ -z "$base_version" ]]; then
  stable_tag="$(latest_stable_version)"
  if [[ -n "$stable_tag" ]]; then
    base_version="$(bump_patch "$stable_tag")"
  else
    base_version="0.0.1"
  fi
fi

base_tag="v${base_version#v}"
prefix="${base_tag}-rc."
prefix_re="$(escape_ere "$prefix")"
max_ordinal=0
existing_tag=""
existing_ordinal=0

while IFS=$'\t' read -r tag prerelease draft target; do
  if [[ "$draft" != "false" || "$prerelease" != "true" ]]; then
    continue
  fi

  if [[ ! "$tag" =~ ^${prefix_re}([0-9]+)$ ]]; then
    continue
  fi

  ordinal="${BASH_REMATCH[1]}"
  if (( ordinal > max_ordinal )); then
    max_ordinal=$ordinal
  fi

  if [[ -n "$head_sha" && "$target" == "$head_sha" && "$ordinal" -ge "$existing_ordinal" ]]; then
    existing_tag="$tag"
    existing_ordinal=$ordinal
  fi
done < <(
  gh api "repos/${repo}/releases?per_page=100" \
    --jq '.[] | [.tag_name, (.prerelease | tostring), (.draft | tostring), (.target_commitish // "")] | @tsv'
)

if [[ -n "$existing_tag" ]]; then
  tag_name="$existing_tag"
else
  tag_name="${prefix}$((max_ordinal + 1))"
fi

printf 'tag_name=%s\n' "$tag_name"
printf 'cli_version=%s\n' "${tag_name#v}"
