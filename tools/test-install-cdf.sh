#!/usr/bin/env bash
set -euo pipefail

fail() {
  printf 'test failed: %s\n' "$*" >&2
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
    fail 'SHA-256 tool unavailable: install sha256sum or shasum'
  fi
}

assert_contains() {
  local file needle content
  file="$1"
  needle="$2"
  content="$(cat "$file")"
  case "$content" in
    *"$needle"*) ;;
    *) fail "expected $file to contain: $needle" ;;
  esac
}

assert_absent() {
  local path
  path="$1"
  [[ ! -e "$path" ]] || fail "expected path to be absent: $path"
}

expect_failure() {
  local output expected
  output="$1"
  expected="$2"
  shift 2

  if "$@" >"${output}.out" 2>"${output}.err"; then
    fail "command unexpectedly succeeded: $*"
  fi
  assert_contains "${output}.err" "$expected"
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
installer="${repo_root}/tools/install-cdf.sh"

test_root="$(mktemp -d "${TMPDIR:-/tmp}/cdf-install-test.XXXXXX")"
trap 'rm -rf "$test_root"' EXIT

version="0.1.0"
target="x86_64-unknown-linux-gnu"
artifact_name="cdf-${version}-${target}.tar.gz"

make_fixture() {
  local fixture_dir checksum_mode build_dir digest
  fixture_dir="$1"
  checksum_mode="$2"
  build_dir="${test_root}/build-${checksum_mode}"

  mkdir -p "$fixture_dir" "$build_dir"
  cat >"${build_dir}/cdf" <<'FAKE_CDF'
#!/usr/bin/env sh
if [ "${1:-}" = "version" ] || [ "${1:-}" = "--version" ]; then
  echo "cdf 0.1.0"
  exit 0
fi
echo "fake cdf"
FAKE_CDF
  chmod +x "${build_dir}/cdf"
  printf 'fixture DuckDB shared library\n' >"${build_dir}/libduckdb.so"
  printf '{"nanoarrow":{"version":"0.8.0","ipc_lz4":true}}\n' >"${build_dir}/duckdb-nanoarrow-build.json"

  tar -czf "${fixture_dir}/${artifact_name}" -C "$build_dir" cdf libduckdb.so duckdb-nanoarrow-build.json
  digest="$(sha256_file "${fixture_dir}/${artifact_name}")"
  if [[ "$checksum_mode" == 'mismatch' ]]; then
    digest="0000000000000000000000000000000000000000000000000000000000000000"
  fi
  printf '%s  %s\n' "$digest" "$artifact_name" >"${fixture_dir}/${artifact_name}.sha256"
}

valid_fixture="${test_root}/fixtures-valid"
mismatch_fixture="${test_root}/fixtures-mismatch"
missing_checksum_fixture="${test_root}/fixtures-missing-checksum"
missing_artifact_fixture="${test_root}/fixtures-missing-artifact"
make_fixture "$valid_fixture" valid
make_fixture "$mismatch_fixture" mismatch
mkdir -p "$missing_checksum_fixture" "$missing_artifact_fixture"
cp "${valid_fixture}/${artifact_name}" "$missing_checksum_fixture/"
printf '%064d  %s\n' 0 "$artifact_name" >"${missing_artifact_fixture}/${artifact_name}.sha256"

success_prefix="${test_root}/prefix-success"
success_output="${test_root}/success.out"
"$installer" \
  --version "$version" \
  --target "$target" \
  --base-url "$valid_fixture" \
  --prefix "$success_prefix" >"$success_output"
[[ -x "${success_prefix}/bin/cdf" ]] || fail 'success install did not write an executable cdf'
[[ -f "${success_prefix}/bin/libduckdb.so" ]] || fail 'success install did not write libduckdb.so'
[[ -f "${success_prefix}/share/cdf/duckdb-nanoarrow-build.json" ]] || fail 'success install did not write DuckDB build metadata'
[[ "$("${success_prefix}/bin/cdf" version)" == 'cdf 0.1.0' ]] || fail 'installed fixture version output mismatch'
assert_contains "$success_output" "Installed cdf 0.1.0 to ${success_prefix}/bin/cdf"
printf 'ok success install verifies checksum and prints version\n'

dry_prefix="${test_root}/prefix-dry-run"
dry_output="${test_root}/dry-run.out"
"$installer" \
  --dry-run \
  --version "$version" \
  --target "$target" \
  --base-url "$valid_fixture" \
  --prefix "$dry_prefix" >"$dry_output"
assert_contains "$dry_output" 'No files written.'
assert_absent "${dry_prefix}/bin/cdf"
printf 'ok dry-run leaves prefix untouched\n'

version_url_output="${test_root}/version-url.out"
"$installer" \
  --dry-run \
  --version "9.9.9" \
  --target "$target" \
  --prefix "${test_root}/prefix-version-url" >"$version_url_output"
assert_contains "$version_url_output" 'https://github.com/z3z1ma/cdf/releases/download/v9.9.9/'
printf 'ok default release URL follows requested version\n'

mismatch_prefix="${test_root}/prefix-mismatch"
expect_failure "${test_root}/mismatch" 'checksum mismatch' \
  "$installer" --version "$version" --target "$target" --base-url "$mismatch_fixture" --prefix "$mismatch_prefix"
assert_absent "${mismatch_prefix}/bin/cdf"
printf 'ok checksum mismatch fails before install\n'

missing_checksum_prefix="${test_root}/prefix-missing-checksum"
expect_failure "${test_root}/missing-checksum" 'missing checksum' \
  "$installer" --version "$version" --target "$target" --base-url "$missing_checksum_fixture" --prefix "$missing_checksum_prefix"
assert_absent "${missing_checksum_prefix}/bin/cdf"
printf 'ok missing checksum fails before install\n'

missing_artifact_prefix="${test_root}/prefix-missing-artifact"
expect_failure "${test_root}/missing-artifact" 'download failed' \
  "$installer" --version "$version" --target "$target" --base-url "$missing_artifact_fixture" --prefix "$missing_artifact_prefix"
assert_absent "${missing_artifact_prefix}/bin/cdf"
printf 'ok failed artifact download fails before install\n'

unsupported_prefix="${test_root}/prefix-unsupported"
expect_failure "${test_root}/unsupported" 'unsupported target' \
  "$installer" --version "$version" --target "riscv64-unknown-linux-gnu" --base-url "$valid_fixture" --prefix "$unsupported_prefix"
assert_absent "${unsupported_prefix}/bin/cdf"
printf 'ok unsupported target fails before install\n'

printf 'installer smoke tests passed\n'
