#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Install a checksummed CDF release artifact.

Usage:
  install-cdf.sh [--version VERSION] [--prefix PREFIX] [--base-url URL_OR_PATH]
                 [--artifact URL_OR_PATH] [--checksum URL_OR_PATH]
                 [--target TARGET] [--dry-run]

Environment overrides:
  CDF_INSTALL_VERSION   Release version to install. Default: 0.1.0
  CDF_INSTALL_PREFIX    Install prefix. Default: $HOME/.local
  CDF_INSTALL_BASE_URL  Artifact directory or URL.
  CDF_INSTALL_ARTIFACT  Explicit artifact URL or local path.
  CDF_INSTALL_CHECKSUM  Explicit checksum URL or local path.
  CDF_INSTALL_TARGET    Explicit target triple.

Expected artifact names:
  cdf-<version>-<target>.tar.gz
  cdf-<version>-<target>.tar.gz.sha256
USAGE
}

default_prefix() {
  if [[ -n "${CDF_INSTALL_PREFIX:-}" ]]; then
    printf '%s\n' "$CDF_INSTALL_PREFIX"
    return
  fi
  [[ -n "${HOME:-}" ]] || die 'HOME is unset; pass --prefix'
  printf '%s/.local\n' "$HOME"
}

detect_target() {
  local os arch
  os="$(uname -s 2>/dev/null || true)"
  arch="$(uname -m 2>/dev/null || true)"

  case "${os}:${arch}" in
    Darwin:arm64 | Darwin:aarch64)
      printf 'aarch64-apple-darwin\n'
      ;;
    Darwin:x86_64 | Darwin:amd64)
      printf 'x86_64-apple-darwin\n'
      ;;
    Linux:aarch64 | Linux:arm64)
      printf 'aarch64-unknown-linux-gnu\n'
      ;;
    Linux:x86_64 | Linux:amd64)
      printf 'x86_64-unknown-linux-gnu\n'
      ;;
    *)
      die "unsupported OS/architecture: ${os:-unknown}/${arch:-unknown}"
      ;;
  esac
}

ensure_supported_target() {
  case "$1" in
    aarch64-apple-darwin | x86_64-apple-darwin | aarch64-unknown-linux-gnu | x86_64-unknown-linux-gnu)
      ;;
    *)
      die "unsupported target: $1"
      ;;
  esac
}

duckdb_library_name() {
  case "$1" in
    *-apple-darwin) printf 'libduckdb.dylib\n' ;;
    *-unknown-linux-gnu) printf 'libduckdb.so\n' ;;
    *) die "unsupported target: $1" ;;
  esac
}

require_value() {
  local flag value
  flag="$1"
  value="${2:-}"
  [[ -n "$value" && "$value" != --* ]] || die "$flag requires a value"
}

fetch_to() {
  local source destination kind local_path
  source="$1"
  destination="$2"
  kind="$3"

  case "$source" in
    http://* | https://*)
      if command -v curl >/dev/null 2>&1; then
        if ! curl -fsSL "$source" -o "$destination"; then
          if [[ "$kind" == 'checksum' ]]; then
            die "missing checksum: $source"
          fi
          die "download failed: $source"
        fi
      elif command -v wget >/dev/null 2>&1; then
        if ! wget -qO "$destination" "$source"; then
          if [[ "$kind" == 'checksum' ]]; then
            die "missing checksum: $source"
          fi
          die "download failed: $source"
        fi
      else
        die "download failed: curl or wget is required to fetch $kind"
      fi
      ;;
    file://*)
      local_path="${source#file://}"
      if [[ ! -f "$local_path" ]]; then
        if [[ "$kind" == 'checksum' ]]; then
          die "missing checksum: $source"
        fi
        die "download failed: $source"
      fi
      cp "$local_path" "$destination" || die "download failed: $source"
      ;;
    *)
      if [[ ! -f "$source" ]]; then
        if [[ "$kind" == 'checksum' ]]; then
          die "missing checksum: $source"
        fi
        die "download failed: $source"
      fi
      cp "$source" "$destination" || die "download failed: $source"
      ;;
  esac
}

sha256_file() {
  local file
  file="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
  else
    die 'SHA-256 tool unavailable: install sha256sum or shasum'
  fi
}

read_expected_checksum() {
  local checksum_file expected
  checksum_file="$1"
  expected="$(
    LC_ALL=C awk 'match($0, /[[:xdigit:]]{64}/) { print substr($0, RSTART, RLENGTH); exit }' "$checksum_file" \
      | tr '[:upper:]' '[:lower:]'
  )"
  [[ -n "$expected" ]] || die "missing checksum: no SHA-256 digest in $checksum_source"
  printf '%s\n' "$expected"
}

version="${CDF_INSTALL_VERSION:-0.1.0}"
prefix="$(default_prefix)"
base_url="${CDF_INSTALL_BASE_URL:-}"
artifact_source="${CDF_INSTALL_ARTIFACT:-}"
checksum_source="${CDF_INSTALL_CHECKSUM:-}"
target="${CDF_INSTALL_TARGET:-}"
dry_run=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      require_value "$1" "${2:-}"
      version="$2"
      shift 2
      ;;
    --prefix)
      require_value "$1" "${2:-}"
      prefix="$2"
      shift 2
      ;;
    --base-url)
      require_value "$1" "${2:-}"
      base_url="$2"
      shift 2
      ;;
    --artifact)
      require_value "$1" "${2:-}"
      artifact_source="$2"
      shift 2
      ;;
    --checksum)
      require_value "$1" "${2:-}"
      checksum_source="$2"
      shift 2
      ;;
    --target)
      require_value "$1" "${2:-}"
      target="$2"
      shift 2
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[[ -n "$version" ]] || die 'version must not be empty'
[[ -n "$prefix" ]] || die 'prefix must not be empty'
if [[ "$prefix" != '/' ]]; then
  prefix="${prefix%/}"
fi
if [[ -z "$base_url" ]]; then
  base_url="https://github.com/z3z1ma/cdf/releases/download/v${version}"
fi

if [[ -z "$target" ]]; then
  target="$(detect_target)"
fi
ensure_supported_target "$target"

artifact_name="cdf-${version}-${target}.tar.gz"
if [[ -z "$artifact_source" ]]; then
  artifact_source="${base_url%/}/${artifact_name}"
fi
if [[ -z "$checksum_source" ]]; then
  checksum_source="${artifact_source}.sha256"
fi

install_dir="${prefix}/bin"
target_path="${install_dir}/cdf"
duckdb_name="$(duckdb_library_name "$target")"
duckdb_target_path="${install_dir}/${duckdb_name}"

if [[ "$dry_run" -eq 1 ]]; then
  cat <<DRYRUN
cdf installer dry run
version: ${version}
target: ${target}
artifact: ${artifact_source}
checksum: ${checksum_source}
prefix: ${prefix}
install path: ${target_path}
DuckDB library: ${duckdb_target_path}
No files written.
DRYRUN
  exit 0
fi

command -v tar >/dev/null 2>&1 || die 'tar is required to extract the release artifact'

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/cdf-install.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

checksum_file="${tmpdir}/${artifact_name}.sha256"
artifact_file="${tmpdir}/${artifact_name}"
extract_dir="${tmpdir}/extract"
mkdir -p "$extract_dir"

fetch_to "$checksum_source" "$checksum_file" 'checksum'
expected_checksum="$(read_expected_checksum "$checksum_file")"

fetch_to "$artifact_source" "$artifact_file" 'artifact'
actual_checksum="$(sha256_file "$artifact_file" | tr '[:upper:]' '[:lower:]')"

if [[ "$actual_checksum" != "$expected_checksum" ]]; then
  die "checksum mismatch for $artifact_name: expected $expected_checksum, got $actual_checksum"
fi

tar -xzf "$artifact_file" -C "$extract_dir" || die "failed to extract artifact: $artifact_source"
binary_path="$(find "$extract_dir" -type f -name cdf -print -quit)"
duckdb_path="$(find "$extract_dir" -type f -name "$duckdb_name" -print -quit)"
[[ -n "$binary_path" ]] || die 'artifact does not contain a cdf binary'
[[ -x "$binary_path" ]] || die 'artifact cdf binary is not executable'
[[ -n "$duckdb_path" ]] || die "artifact does not contain ${duckdb_name}"

installed_version="$("$binary_path" version 2>/dev/null)" || die 'artifact cdf binary did not print a version'
[[ -n "$installed_version" ]] || die 'artifact cdf binary printed an empty version'

install -d "$install_dir" || die "failed to create install directory: $install_dir"
tmp_target="${install_dir}/.cdf.tmp.$$"
tmp_duckdb_target="${install_dir}/.${duckdb_name}.tmp.$$"
rm -f "$tmp_target" "$tmp_duckdb_target"
if ! install -m 0755 "$binary_path" "$tmp_target"; then
  rm -f "$tmp_target" "$tmp_duckdb_target"
  die "failed to stage cdf binary in $install_dir"
fi
if ! install -m 0755 "$duckdb_path" "$tmp_duckdb_target"; then
  rm -f "$tmp_target" "$tmp_duckdb_target"
  die "failed to stage ${duckdb_name} in $install_dir"
fi
if ! mv "$tmp_duckdb_target" "$duckdb_target_path"; then
  rm -f "$tmp_target" "$tmp_duckdb_target"
  die "failed to install DuckDB library at $duckdb_target_path"
fi
if ! mv "$tmp_target" "$target_path"; then
  rm -f "$tmp_target"
  die "failed to install cdf binary at $target_path"
fi

printf 'Installed %s to %s\n' "$installed_version" "$target_path"
