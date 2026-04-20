#!/usr/bin/env bash
set -euo pipefail

BINARY_NAME="convex-sync"
DEV_BINARY_NAME="${CONVEX_SYNC_DEV_BIN_NAME:-convex-sync-dev}"
REPO="shpitdev/convex-streaming-olap-export"

VERSION="${CONVEX_SYNC_VERSION:-latest}"
INSTALL_MODE="${CONVEX_SYNC_INSTALL_MODE:-auto}"
INSTALL_DIR="${CONVEX_SYNC_INSTALL_DIR:-$HOME/.local/share/${BINARY_NAME}}"
BIN_DIR="${CONVEX_SYNC_BIN_DIR:-$HOME/.local/bin}"
DEV_STATE_DIR="${CONVEX_SYNC_DEV_STATE_DIR:-$HOME/.local/share/${BINARY_NAME}/install-dev}"

FORCE=false
NO_SHELL_UPDATE=false

usage() {
  cat <<EOF
Usage: install.sh [OPTIONS]

Options:
  --mode <dev|release|auto>  Install mode (default: auto - dev in checkout, release otherwise)
  --version <VERSION>        Version to install: latest, next, or v0.0.1 (default: latest)
  --force                    Overwrite an existing installation
  --no-shell-update          Skip PATH changes in shell rc files
  -h, --help                 Show this help

Environment variables:
  CONVEX_SYNC_VERSION         Same as --version
  CONVEX_SYNC_INSTALL_MODE    Same as --mode
  CONVEX_SYNC_INSTALL_DIR     Install directory for release binaries
  CONVEX_SYNC_BIN_DIR         Directory for installed command symlinks
  CONVEX_SYNC_DEV_STATE_DIR   State directory for checkout-linked dev installs
  CONVEX_SYNC_DEV_BIN_NAME    Override the installed dev command name
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) INSTALL_MODE="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --force) FORCE=true; shift ;;
    --no-shell-update) NO_SHELL_UPDATE=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'Unknown option: %s\n' "$1" >&2; exit 1 ;;
  esac
done

has_command() {
  command -v "$1" >/dev/null 2>&1
}

script_dir() {
  local source_path="${BASH_SOURCE[0]:-}"
  if [[ -z "$source_path" || "$source_path" == "bash" || "$source_path" == "-bash" ]]; then
    return 1
  fi
  cd -- "$(dirname -- "$source_path")" >/dev/null 2>&1 && pwd
}

local_checkout_root() {
  local dir
  dir="$(script_dir || true)"
  if [[ -z "$dir" ]]; then
    return 1
  fi
  if [[ -f "$dir/Cargo.toml" && -f "$dir/apps/convex-sync/src/main.rs" ]]; then
    printf '%s' "$dir"
    return 0
  fi
  return 1
}

gh_is_authenticated() {
  if ! has_command gh; then
    return 1
  fi
  if [[ -n "${GH_TOKEN:-}" || -n "${GITHUB_TOKEN:-}" ]]; then
    return 0
  fi
  gh auth status >/dev/null 2>&1
}

github_api_get() {
  local url="$1"
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    curl -fsSL -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "Accept: application/vnd.github+json" "$url"
    return
  fi
  curl -fsSL -H "Accept: application/vnd.github+json" "$url"
}

detect_os() {
  case "$(uname -s)" in
    Linux) printf 'linux' ;;
    Darwin) printf 'darwin' ;;
    *) printf 'unsupported operating system: %s\n' "$(uname -s)" >&2; exit 1 ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) printf 'amd64' ;;
    arm64|aarch64) printf 'arm64' ;;
    *) printf 'unsupported architecture: %s\n' "$(uname -m)" >&2; exit 1 ;;
  esac
}

validate_release_platform() {
  local os="$1" arch="$2"
  if [[ "$os" != "linux" || "$arch" != "amd64" ]]; then
    printf 'release installs currently support linux-amd64 only\n' >&2
    exit 1
  fi
}

resolve_install_mode() {
  case "$INSTALL_MODE" in
    auto)
      if local_checkout_root >/dev/null 2>&1; then
        printf 'dev'
      else
        printf 'release'
      fi
      ;;
    dev|release) printf '%s' "$INSTALL_MODE" ;;
    *) printf 'unsupported install mode: %s\n' "$INSTALL_MODE" >&2; exit 1 ;;
  esac
}

resolve_latest_release() {
  local tag=""
  if gh_is_authenticated; then
    tag="$(gh release list --repo "$REPO" --exclude-drafts --exclude-pre-releases --limit 1 --json tagName --jq '.[0].tagName' 2>/dev/null || true)"
  fi
  if [[ -z "$tag" || "$tag" == "null" ]]; then
    tag="$(github_api_get "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null | sed -n 's/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1 || true)"
  fi
  if [[ -n "$tag" && "$tag" != "null" ]]; then
    printf '%s' "$tag"
  fi
}

resolve_latest_prerelease() {
  local tag=""
  if gh_is_authenticated; then
    tag="$(gh release list --repo "$REPO" --exclude-drafts --limit 20 --json isPrerelease,tagName,publishedAt --jq 'map(select(.isPrerelease)) | sort_by(.publishedAt) | last.tagName' 2>/dev/null || true)"
  fi
  if [[ -z "$tag" || "$tag" == "null" ]]; then
    tag="$(
      github_api_get "https://api.github.com/repos/${REPO}/releases?per_page=20" 2>/dev/null \
        | tr '\n' ' ' \
        | sed 's/},{/}\n{/g' \
        | sed -n '/"prerelease":[[:space:]]*true/ s/.*"tag_name":[[:space:]]*"\([^"]*\)".*/\1/p' \
        | head -n1 || true
    )"
  fi
  if [[ -n "$tag" && "$tag" != "null" ]]; then
    printf '%s' "$tag"
  fi
}

resolve_release_version() {
  if [[ "$VERSION" == "next" ]]; then
    local prerelease
    prerelease="$(resolve_latest_prerelease)"
    if [[ -z "$prerelease" ]]; then
      printf 'no prerelease found for %s\n' "$REPO" >&2
      exit 1
    fi
    printf '%s' "$prerelease"
    return
  fi
  if [[ "$VERSION" != "latest" ]]; then
    printf '%s' "$VERSION"
    return
  fi
  local latest
  latest="$(resolve_latest_release)"
  if [[ -z "$latest" ]]; then
    latest="$(resolve_latest_prerelease)"
  fi
  if [[ -z "$latest" ]]; then
    printf 'no release found for %s\n' "$REPO" >&2
    exit 1
  fi
  printf '%s' "$latest"
}

download_release() {
  local version="$1" asset="$2" dest_dir="$3"
  if gh_is_authenticated; then
    gh release download "$version" --repo "$REPO" --pattern "$asset" --dir "$dest_dir" --clobber >/dev/null
    return
  fi
  curl -fsSL "https://github.com/${REPO}/releases/download/${version}/${asset}" -o "${dest_dir}/${asset}"
}

ensure_shell_path() {
  local marker_begin marker_end rc_file shell_name
  [[ "$NO_SHELL_UPDATE" == "true" ]] && return 0

  shell_name="$(basename "${SHELL:-}")"
  case "$shell_name" in
    zsh) rc_file="${HOME}/.zshrc" ;;
    bash)
      if [[ "$(detect_os)" == "darwin" ]]; then
        rc_file="${HOME}/.bash_profile"
      else
        rc_file="${HOME}/.bashrc"
      fi
      ;;
    *) rc_file="${HOME}/.profile" ;;
  esac

  marker_begin="# >>> ${BINARY_NAME} install >>>"
  marker_end="# <<< ${BINARY_NAME} install <<<"
  mkdir -p "$(dirname "$rc_file")"
  touch "$rc_file"

  tmp_file="$(mktemp)"
  awk -v begin="$marker_begin" -v end="$marker_end" '
    $0 == begin { skip = 1; next }
    skip && $0 == end { skip = 0; next }
    !skip { print }
  ' "$rc_file" >"$tmp_file"
  cat "$tmp_file" >"$rc_file"
  rm -f "$tmp_file"

  {
    printf '\n%s\n' "$marker_begin"
    printf 'case ":%s:" in\n' '$PATH'
    printf '  *:"%s":*) ;;\n' "$BIN_DIR"
    printf '  *) export PATH="%s:$PATH" ;;\n' "$BIN_DIR"
    printf 'esac\n'
    printf '%s\n\n' "$marker_end"
  } >>"$rc_file"
}

verify_path_resolution() {
  local bin_name="$1" expected_path="$2"
  local resolved
  resolved="$(command -v "$bin_name" 2>/dev/null || true)"
  if [[ -n "$resolved" && "$resolved" != "$expected_path" ]]; then
    printf 'warning: PATH resolves %s to %s instead of %s\n' "$bin_name" "$resolved" "$expected_path" >&2
  fi
}

install_dev_link() {
  local repo_root wrapper_path link_path
  repo_root="$(local_checkout_root || true)"
  if [[ -z "$repo_root" ]]; then
    printf 'dev mode requires running from a local checkout\n' >&2
    exit 1
  fi

  wrapper_path="${repo_root}/scripts/convex-sync-dev"
  link_path="${BIN_DIR}/${DEV_BINARY_NAME}"

  [[ -f "$wrapper_path" ]] || { printf 'missing dev wrapper: %s\n' "$wrapper_path" >&2; exit 1; }

  mkdir -p "$BIN_DIR" "$DEV_STATE_DIR"
  if [[ -e "$link_path" && "$FORCE" != "true" ]]; then
    printf 'existing installation: %s\nUse --force to replace.\n' "$link_path" >&2
    exit 1
  fi
  rm -f "$link_path"
  ln -s "$wrapper_path" "$link_path"
  printf '%s\n' "$repo_root" > "${DEV_STATE_DIR}/checkout-root"

  ensure_shell_path

  installed_version="$("$link_path" --version 2>/dev/null || true)"
  printf 'Installed %s -> %s\n' "$DEV_BINARY_NAME" "$link_path"
  printf 'Version: %s\n' "${installed_version:-unknown}"
  verify_path_resolution "$DEV_BINARY_NAME" "$link_path"
}

install_release_binary() {
  local version os arch asset temp_dir binary_path target_dir install_path installed_version link_path
  os="$(detect_os)"
  arch="$(detect_arch)"
  validate_release_platform "$os" "$arch"
  link_path="${BIN_DIR}/${BINARY_NAME}"

  if [[ -e "$link_path" && "$FORCE" != "true" ]]; then
    printf 'existing installation: %s\nUse --force to replace.\n' "$link_path" >&2
    exit 1
  fi

  version="$(resolve_release_version)"
  asset="${BINARY_NAME}_${version}_${os}_${arch}.tar.gz"

  temp_dir="$(mktemp -d)"
  trap "rm -rf -- '$temp_dir'" EXIT
  download_release "$version" "$asset" "$temp_dir"
  tar -xzf "${temp_dir}/${asset}" -C "$temp_dir"

  binary_path="$(find "$temp_dir" -type f -name "$BINARY_NAME" | head -n1)"
  [[ -n "$binary_path" ]] || { printf 'release asset did not contain %s\n' "$BINARY_NAME" >&2; exit 1; }

  target_dir="${INSTALL_DIR}/${version}"
  install_path="${target_dir}/${BINARY_NAME}"
  mkdir -p "$target_dir" "$BIN_DIR"
  cp "$binary_path" "$install_path"
  chmod 0755 "$install_path"

  rm -f "$link_path"
  ln -s "$install_path" "$link_path"

  ensure_shell_path

  installed_version="$("$link_path" --version 2>/dev/null || true)"
  printf 'Installed %s to %s\n' "$BINARY_NAME" "$link_path"
  printf 'Version: %s\n' "${installed_version:-unknown}"
  verify_path_resolution "$BINARY_NAME" "$link_path"
}

mode="$(resolve_install_mode)"
case "$mode" in
  dev) install_dev_link ;;
  release) install_release_binary ;;
esac
