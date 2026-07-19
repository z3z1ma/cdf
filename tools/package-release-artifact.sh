#!/usr/bin/env bash
set -euo pipefail

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Package a built CDF binary into a checksummed release archive.

Usage:
  package-release-artifact.sh --version VERSION --target TARGET --binary PATH
                              --duckdb-library PATH --out-dir DIR
                              [--completions-dir DIR] [--man-dir DIR]
                              [--skip-binary-run REASON]

The archive name is cdf-<version>-<target>.tar.gz, with an adjacent .sha256.
USAGE
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

duckdb_library_name() {
  case "$1" in
    *-apple-darwin) printf 'libduckdb.dylib\n' ;;
    *-unknown-linux-gnu) printf 'libduckdb.so\n' ;;
    x86_64-pc-windows-msvc) printf 'duckdb.dll\n' ;;
    *) die "unsupported release target: $1" ;;
  esac
}

python_cmd() {
  if [[ -n "${PYTHON:-}" ]]; then
    printf '%s\n' "$PYTHON"
  elif command -v python3 >/dev/null 2>&1; then
    command -v python3
  elif command -v python >/dev/null 2>&1; then
    command -v python
  else
    die 'Python 3 is required to write reproducible release archives'
  fi
}

copy_generated_dir() {
  local source destination label
  source="$1"
  destination="$2"
  label="$3"

  if [[ -n "$source" && -d "$source" ]]; then
    mkdir -p "$(dirname "$destination")"
    cp -R "$source" "$destination"
    printf '%s: included from %s\n' "$label" "$source"
  else
    printf '%s: not included; run cdf-generate-cli-artifacts before packaging release artifacts\n' "$label"
  fi
}

version=""
target=""
binary=""
duckdb_library=""
out_dir=""
completions_dir=""
man_dir=""
skip_binary_run_reason=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--version requires a value'
      version="$2"
      shift 2
      ;;
    --target)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--target requires a value'
      target="$2"
      shift 2
      ;;
    --binary)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--binary requires a value'
      binary="$2"
      shift 2
      ;;
    --duckdb-library)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--duckdb-library requires a value'
      duckdb_library="$2"
      shift 2
      ;;
    --out-dir)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--out-dir requires a value'
      out_dir="$2"
      shift 2
      ;;
    --completions-dir)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--completions-dir requires a value'
      completions_dir="$2"
      shift 2
      ;;
    --man-dir)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--man-dir requires a value'
      man_dir="$2"
      shift 2
      ;;
    --skip-binary-run)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--skip-binary-run requires a reason'
      skip_binary_run_reason="$2"
      shift 2
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

[[ -n "$version" ]] || die '--version is required'
[[ -n "$target" ]] || die '--target is required'
[[ -n "$binary" ]] || die '--binary is required'
[[ -n "$duckdb_library" ]] || die '--duckdb-library is required'
[[ -n "$out_dir" ]] || die '--out-dir is required'
[[ -f "$binary" ]] || die "binary does not exist: $binary"
[[ -x "$binary" ]] || die "binary is not executable: $binary"
[[ -f "$duckdb_library" ]] || die "DuckDB library does not exist: $duckdb_library"
[[ -f LICENSE ]] || die 'LICENSE is required'
[[ -f CHANGELOG.md ]] || die 'CHANGELOG.md is required'
[[ -f tools/write-reproducible-targz.py ]] || die 'tools/write-reproducible-targz.py is required'

tools/verify-release-metadata.sh "$version" >/dev/null
python_bin="$(python_cmd)"
"$python_bin" - <<'PYTHON_CHECK'
import sys

if sys.version_info < (3, 8):
    raise SystemExit("Python 3.8+ is required")
PYTHON_CHECK

if [[ -z "$skip_binary_run_reason" ]]; then
  version_output="$("$binary" version 2>/dev/null)" || die "binary failed version probe: $binary"
  case "$version_output" in
    *"$version"*) ;;
    *) die "binary version output '$version_output' does not contain $version" ;;
  esac
else
  version_output="skipped: $skip_binary_run_reason"
fi

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/cdf-release-artifact.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

archive_base="cdf-${version}-${target}"
stage_dir="${tmpdir}/${archive_base}"
mkdir -p "$stage_dir/bin" "$stage_dir/generated" "$out_dir"

binary_name="$(basename "$binary")"
duckdb_library_name="$(duckdb_library_name "$target")"
case "$target" in
  x86_64-pc-windows-msvc) binary_name="cdf.exe" ;;
  *) binary_name="cdf" ;;
esac
cp "$binary" "${stage_dir}/bin/${binary_name}"
chmod 0755 "${stage_dir}/bin/${binary_name}"
cp "$duckdb_library" "${stage_dir}/bin/${duckdb_library_name}"
cp LICENSE "${stage_dir}/LICENSE"

tools/verify-release-metadata.sh "$version" --write-changelog-excerpt "${stage_dir}/CHANGELOG-excerpt.md" >/dev/null

{
  copy_generated_dir "$completions_dir" "${stage_dir}/generated/completions" "completions"
  copy_generated_dir "$man_dir" "${stage_dir}/generated/man" "man_pages"
} >"${stage_dir}/generated/ARTIFACTS.txt"

cat >"${stage_dir}/release-metadata.txt" <<METADATA
name: CDF
version: ${version}
target: ${target}
archive: ${archive_base}.tar.gz
binary: bin/${binary_name}
duckdb_library: bin/${duckdb_library_name}
duckdb_linkage: dynamic, exact version selected by the pinned libduckdb-sys crate
binary_version_probe: ${version_output}
license: Apache-2.0
crates_io_publication: disabled while the DataFusion git pin is active
generated_cli_artifacts: conditional; see generated/ARTIFACTS.txt
METADATA

archive_path="${out_dir}/${archive_base}.tar.gz"
checksum_path="${archive_path}.sha256"

"$python_bin" tools/write-reproducible-targz.py "$stage_dir" "$archive_path"
digest="$(sha256_file "$archive_path" | tr '[:upper:]' '[:lower:]')"
printf '%s  %s\n' "$digest" "$(basename "$archive_path")" >"$checksum_path"

actual="$(sha256_file "$archive_path" | tr '[:upper:]' '[:lower:]')"
[[ "$actual" == "$digest" ]] || die "checksum verification failed for $archive_path"

printf 'packaged %s\n' "$archive_path"
printf 'checksum %s\n' "$checksum_path"
