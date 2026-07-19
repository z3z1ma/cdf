#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
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

usage() {
  cat <<'USAGE'
Verify CDF release archives and their adjacent checksums.

Usage:
  verify-release-artifacts.sh VERSION DIST_DIR TARGET...
USAGE
}

version="${1:-}"
dist_dir="${2:-}"
[[ -n "$version" && -n "$dist_dir" ]] || {
  usage
  die 'VERSION and DIST_DIR are required'
}
shift 2 || true
[[ $# -gt 0 ]] || die 'at least one target is required'
[[ -d "$dist_dir" ]] || die "distribution directory does not exist: $dist_dir"

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/cdf-release-verify.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

for target in "$@"; do
  base="cdf-${version}-${target}"
  archive="${dist_dir}/${base}.tar.gz"
  checksum="${archive}.sha256"
  [[ -f "$archive" ]] || die "missing archive: $archive"
  [[ -f "$checksum" ]] || die "missing checksum: $checksum"

  expected="$(awk 'match($0, /[[:xdigit:]]{64}/) { print substr($0, RSTART, RLENGTH); exit }' "$checksum" | tr '[:upper:]' '[:lower:]')"
  [[ -n "$expected" ]] || die "checksum file has no SHA-256 digest: $checksum"
  actual="$(sha256_file "$archive" | tr '[:upper:]' '[:lower:]')"
  [[ "$expected" == "$actual" ]] || die "checksum mismatch for $archive"

  list_file="${tmpdir}/${base}.list"
  tar -tzf "$archive" >"$list_file"
  grep -qx "${base}/LICENSE" "$list_file" || die "archive lacks LICENSE: $archive"
  grep -qx "${base}/CHANGELOG-excerpt.md" "$list_file" || die "archive lacks changelog excerpt: $archive"
  grep -qx "${base}/release-metadata.txt" "$list_file" || die "archive lacks release metadata: $archive"
  grep -qx "${base}/duckdb-nanoarrow-build.json" "$list_file" || die "archive lacks DuckDB nanoarrow build metadata: $archive"
  grep -qx "${base}/generated/ARTIFACTS.txt" "$list_file" || die "archive lacks generated artifact inventory: $archive"
  case "$target" in
    x86_64-pc-windows-msvc)
      grep -qx "${base}/bin/cdf.exe" "$list_file" || die "archive lacks cdf.exe: $archive"
      grep -qx "${base}/bin/duckdb.dll" "$list_file" || die "archive lacks duckdb.dll: $archive"
      ;;
    *-apple-darwin)
      grep -qx "${base}/bin/cdf" "$list_file" || die "archive lacks cdf binary: $archive"
      grep -qx "${base}/bin/libduckdb.dylib" "$list_file" || die "archive lacks libduckdb.dylib: $archive"
      ;;
    *-unknown-linux-gnu)
      grep -qx "${base}/bin/cdf" "$list_file" || die "archive lacks cdf binary: $archive"
      grep -qx "${base}/bin/libduckdb.so" "$list_file" || die "archive lacks libduckdb.so: $archive"
      ;;
    *)
      die "unsupported release target: $target"
      ;;
  esac
done

printf 'verified %s artifact(s) for %s\n' "$#" "$version"
