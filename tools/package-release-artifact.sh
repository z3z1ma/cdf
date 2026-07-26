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
                              --runtime-library [NAME=]PATH
                              [--runtime-library [NAME=]PATH ...]
                              --runtime-license NAME=PATH
                              [--runtime-license NAME=PATH ...]
                              --out-dir DIR
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
runtime_libraries=()
runtime_licenses=()
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
    --runtime-library)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--runtime-library requires a value'
      runtime_libraries+=("$2")
      shift 2
      ;;
    --runtime-license)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--runtime-license requires NAME=PATH'
      runtime_licenses+=("$2")
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
[[ "${#runtime_libraries[@]}" -gt 0 ]] || die 'at least one --runtime-library is required'
[[ "${#runtime_licenses[@]}" -gt 0 ]] || die 'at least one --runtime-license is required'
[[ -n "$out_dir" ]] || die '--out-dir is required'
[[ -f "$binary" ]] || die "binary does not exist: $binary"
[[ -x "$binary" ]] || die "binary is not executable: $binary"
for runtime_library in "${runtime_libraries[@]}"; do
  runtime_path="${runtime_library#*=}"
  [[ -f "$runtime_path" ]] || die "runtime library does not exist: $runtime_path"
done
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

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/cdf-release-artifact.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

archive_base="cdf-${version}-${target}"
stage_dir="${tmpdir}/${archive_base}"
mkdir -p "$stage_dir/bin" "$stage_dir/generated" "$stage_dir/THIRD_PARTY_LICENSES" "$out_dir"

binary_name="$(basename "$binary")"
case "$target" in
  x86_64-pc-windows-msvc) binary_name="cdf.exe" ;;
  *) binary_name="cdf" ;;
esac
cp "$binary" "${stage_dir}/bin/${binary_name}"
chmod 0755 "${stage_dir}/bin/${binary_name}"

runtime_names=()
runtime_dependency_names=()
for runtime_library in "${runtime_libraries[@]}"; do
  case "$runtime_library" in
    *=*)
      runtime_name="${runtime_library%%=*}"
      runtime_path="${runtime_library#*=}"
      ;;
    *)
      runtime_path="$runtime_library"
      runtime_name="$(basename "$runtime_path")"
      ;;
  esac
  case "$runtime_name" in
    *.dylib | *.so | *.so.* | *.dll) ;;
    *) die "runtime library has unsupported filename: $runtime_name" ;;
  esac
  if [[ -e "${stage_dir}/bin/${runtime_name}" ]]; then
    die "duplicate runtime library filename: $runtime_name"
  fi
  cp "$runtime_path" "${stage_dir}/bin/${runtime_name}"
  runtime_names+=("$runtime_name")
  runtime_dependency_names+=("$(basename "$runtime_path")")
done

for runtime_license in "${runtime_licenses[@]}"; do
  case "$runtime_license" in
    *=*) ;;
    *) die "runtime license must use NAME=PATH: $runtime_license" ;;
  esac
  license_name="${runtime_license%%=*}"
  license_path="${runtime_license#*=}"
  [[ "$license_name" =~ ^[A-Za-z0-9._-]+$ ]] || die "invalid runtime license name: $license_name"
  [[ -f "$license_path" ]] || die "runtime license does not exist: $license_path"
  cp "$license_path" "${stage_dir}/THIRD_PARTY_LICENSES/${license_name}.txt"
done

if [[ "$target" == *-apple-darwin ]]; then
  command -v otool >/dev/null 2>&1 || die 'otool is required to inspect macOS runtime linkage'
  command -v install_name_tool >/dev/null 2>&1 || die 'install_name_tool is required to make macOS runtime linkage relocatable'
  command -v codesign >/dev/null 2>&1 || die 'codesign is required to validate relocated macOS runtime libraries'
  for index in "${!runtime_names[@]}"; do
    runtime_name="${runtime_names[$index]}"
    dependency_name="${runtime_dependency_names[$index]}"
    dependency="$(
      otool -L "${stage_dir}/bin/${binary_name}" \
        | awk 'NR > 1 { print $1 }' \
        | awk -v name="$dependency_name" '$0 == name || $0 ~ ("/" name "$") || $0 ~ ("@rpath/" name "$") { print; exit }'
    )"
    [[ -n "$dependency" ]] || die "binary does not reference staged runtime library $runtime_name"
    if [[ "$dependency" != "@rpath/${runtime_name}" ]]; then
      install_name_tool -change "$dependency" "@rpath/${runtime_name}" "${stage_dir}/bin/${binary_name}"
    fi
    # A Python framework executable carries a bundle signature whose sealed Info.plist is not
    # present after the executable is staged as a standalone dylib. Re-sign every relocated
    # runtime ad hoc so dyld sees a valid self-contained Mach-O instead of host-specific bundle
    # authority. Release signing/notarization remains a separate future distribution concern.
    codesign --force --sign - --timestamp=none "${stage_dir}/bin/${runtime_name}"
    codesign --verify --strict --verbose=2 "${stage_dir}/bin/${runtime_name}"
  done
  # install_name_tool changes the executable's load commands. Restore a valid deterministic
  # ad-hoc signature after the final change and verify it before the executable smoke.
  codesign --force --sign - --timestamp=none "${stage_dir}/bin/${binary_name}"
  codesign --verify --strict --verbose=2 "${stage_dir}/bin/${binary_name}"
fi

cp LICENSE "${stage_dir}/LICENSE"

tools/verify-release-metadata.sh "$version" --write-changelog-excerpt "${stage_dir}/CHANGELOG-excerpt.md" >/dev/null

if [[ -z "$skip_binary_run_reason" ]]; then
  case "$target" in
    *-apple-darwin)
      version_output="$(DYLD_FALLBACK_LIBRARY_PATH="${stage_dir}/bin${DYLD_FALLBACK_LIBRARY_PATH:+:${DYLD_FALLBACK_LIBRARY_PATH}}" "${stage_dir}/bin/${binary_name}" version)" \
        || die "staged binary failed version probe: ${stage_dir}/bin/${binary_name}"
      ;;
    *-unknown-linux-gnu)
      version_output="$(LD_LIBRARY_PATH="${stage_dir}/bin${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}" "${stage_dir}/bin/${binary_name}" version)" \
        || die "staged binary failed version probe: ${stage_dir}/bin/${binary_name}"
      ;;
    x86_64-pc-windows-msvc)
      version_output="$(PATH="${stage_dir}/bin:${PATH}" "${stage_dir}/bin/${binary_name}" version)" \
        || die "staged binary failed version probe: ${stage_dir}/bin/${binary_name}"
      ;;
    *) die "unsupported release target: $target" ;;
  esac
  case "$version_output" in
    *"$version"*) ;;
    *) die "binary version output '$version_output' does not contain $version" ;;
  esac
else
  version_output="skipped: $skip_binary_run_reason"
fi

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
runtime_linkage: required non-system runtime libraries staged beside executable
binary_version_probe: ${version_output}
license: Apache-2.0
crates_io_publication: disabled while the DataFusion git pin is active
generated_cli_artifacts: conditional; see generated/ARTIFACTS.txt
METADATA
for runtime_name in "${runtime_names[@]}"; do
  printf 'runtime_library: bin/%s\n' "$runtime_name" >>"${stage_dir}/release-metadata.txt"
done

archive_path="${out_dir}/${archive_base}.tar.gz"
checksum_path="${archive_path}.sha256"

"$python_bin" tools/write-reproducible-targz.py "$stage_dir" "$archive_path"
digest="$(sha256_file "$archive_path" | tr '[:upper:]' '[:lower:]')"
printf '%s  %s\n' "$digest" "$(basename "$archive_path")" >"$checksum_path"

actual="$(sha256_file "$archive_path" | tr '[:upper:]' '[:lower:]')"
[[ "$actual" == "$digest" ]] || die "checksum verification failed for $archive_path"

printf 'packaged %s\n' "$archive_path"
printf 'checksum %s\n' "$checksum_path"
