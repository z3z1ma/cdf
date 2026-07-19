#!/usr/bin/env bash
set -euo pipefail

fail() {
  printf 'test failed: %s\n' "$*" >&2
  exit 1
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

test_root="$(mktemp -d "${TMPDIR:-/tmp}/cdf-release-artifacts-test.XXXXXX")"
trap 'rm -rf "$test_root"' EXIT

version="0.1.0"
target="x86_64-unknown-linux-gnu"
fake_bin_dir="${test_root}/bin"
dist_dir="${test_root}/dist"
generated_dir="${test_root}/generated"
mkdir -p "$fake_bin_dir" "$generated_dir/completions" "$generated_dir/man"

cat >"${fake_bin_dir}/cdf" <<'FAKE_CDF'
#!/usr/bin/env sh
if [ "${1:-}" = "version" ] || [ "${1:-}" = "--version" ]; then
  echo "cdf 0.1.0"
  exit 0
fi
echo "fake cdf"
FAKE_CDF
chmod +x "${fake_bin_dir}/cdf"
printf 'fixture DuckDB shared library\n' >"${fake_bin_dir}/libduckdb.so"
printf '{"nanoarrow":{"version":"0.8.0","ipc_lz4":true}}\n' >"${fake_bin_dir}/duckdb-nanoarrow-build.json"
printf 'complete -c cdf\n' >"${generated_dir}/completions/cdf.bash"
printf '.TH cdf 1\n' >"${generated_dir}/man/cdf.1"

tools/verify-release-metadata.sh "$version" --write-changelog-excerpt "${test_root}/CHANGELOG-excerpt.md" >"${test_root}/metadata.out"
grep -q "release metadata ok for $version" "${test_root}/metadata.out" || fail 'metadata verifier did not report success'
grep -q "## \\[$version\\]" "${test_root}/CHANGELOG-excerpt.md" || fail 'changelog excerpt missing requested version'
printf 'ok metadata verifier checks versioned release inputs\n'

tools/package-release-artifact.sh \
  --version "$version" \
  --target "$target" \
  --binary "${fake_bin_dir}/cdf" \
  --duckdb-library "${fake_bin_dir}/libduckdb.so" \
  --duckdb-build-metadata "${fake_bin_dir}/duckdb-nanoarrow-build.json" \
  --out-dir "$dist_dir" \
  --completions-dir "${generated_dir}/completions" \
  --man-dir "${generated_dir}/man" >"${test_root}/package.out"

tools/verify-release-artifacts.sh "$version" "$dist_dir" "$target" >"${test_root}/verify.out"
grep -q "verified 1 artifact(s) for $version" "${test_root}/verify.out" || fail 'artifact verifier did not report success'
printf 'ok artifact package includes binary license changelog generated artifacts and checksum\n'

repro_dist_a="${test_root}/dist-repro-a"
repro_dist_b="${test_root}/dist-repro-b"
tools/package-release-artifact.sh \
  --version "$version" \
  --target "$target" \
  --binary "${fake_bin_dir}/cdf" \
  --duckdb-library "${fake_bin_dir}/libduckdb.so" \
  --duckdb-build-metadata "${fake_bin_dir}/duckdb-nanoarrow-build.json" \
  --out-dir "$repro_dist_a" \
  --completions-dir "${generated_dir}/completions" \
  --man-dir "${generated_dir}/man" >/dev/null
sleep 1
tools/package-release-artifact.sh \
  --version "$version" \
  --target "$target" \
  --binary "${fake_bin_dir}/cdf" \
  --duckdb-library "${fake_bin_dir}/libduckdb.so" \
  --duckdb-build-metadata "${fake_bin_dir}/duckdb-nanoarrow-build.json" \
  --out-dir "$repro_dist_b" \
  --completions-dir "${generated_dir}/completions" \
  --man-dir "${generated_dir}/man" >/dev/null
digest_a="$(awk '{print $1}' "${repro_dist_a}/cdf-${version}-${target}.tar.gz.sha256")"
digest_b="$(awk '{print $1}' "${repro_dist_b}/cdf-${version}-${target}.tar.gz.sha256")"
[[ "$digest_a" == "$digest_b" ]] || fail 'same staged release inputs did not produce identical archive SHA-256 values'
cmp -s "${repro_dist_a}/cdf-${version}-${target}.tar.gz" "${repro_dist_b}/cdf-${version}-${target}.tar.gz" || fail 'same staged release inputs did not produce byte-identical archives'
printf 'ok reproducible package hash is stable for identical staged inputs\n'

extract_dir="${test_root}/extract"
mkdir -p "$extract_dir"
tar -xzf "${dist_dir}/cdf-${version}-${target}.tar.gz" -C "$extract_dir"
[[ -f "${extract_dir}/cdf-${version}-${target}/generated/completions/cdf.bash" ]] || fail 'completion artifact was not packaged'
[[ -f "${extract_dir}/cdf-${version}-${target}/generated/man/cdf.1" ]] || fail 'man artifact was not packaged'
[[ -f "${extract_dir}/cdf-${version}-${target}/bin/libduckdb.so" ]] || fail 'DuckDB shared library was not packaged'
[[ -f "${extract_dir}/cdf-${version}-${target}/duckdb-nanoarrow-build.json" ]] || fail 'DuckDB nanoarrow build metadata was not packaged'
printf 'ok generated completions and man pages are packaged when present\n'

missing_generated_dist="${test_root}/dist-missing-generated"
tools/package-release-artifact.sh \
  --version "$version" \
  --target "$target" \
  --binary "${fake_bin_dir}/cdf" \
  --duckdb-library "${fake_bin_dir}/libduckdb.so" \
  --duckdb-build-metadata "${fake_bin_dir}/duckdb-nanoarrow-build.json" \
  --out-dir "$missing_generated_dist" >"${test_root}/package-missing-generated.out"
tools/verify-release-artifacts.sh "$version" "$missing_generated_dist" "$target" >/dev/null
tar -xzf "${missing_generated_dist}/cdf-${version}-${target}.tar.gz" -C "$extract_dir"
grep -q 'cdf-generate-cli-artifacts' "${extract_dir}/cdf-${version}-${target}/generated/ARTIFACTS.txt" || fail 'missing generated artifact note did not name the generator'
printf 'ok absent generated artifacts are recorded without blocking WS8B packaging\n'

printf '%064d  cdf-%s-%s.tar.gz\n' 0 "$version" "$target" >"${dist_dir}/cdf-${version}-${target}.tar.gz.sha256"
if tools/verify-release-artifacts.sh "$version" "$dist_dir" "$target" >"${test_root}/bad-checksum.out" 2>"${test_root}/bad-checksum.err"; then
  fail 'artifact verifier accepted a mismatched checksum'
fi
grep -q 'checksum mismatch' "${test_root}/bad-checksum.err" || fail 'checksum mismatch failure was not explicit'
printf 'ok checksum mismatch fails closed\n'

if tools/verify-release-metadata.sh "9.9.9" >"${test_root}/bad-version.out" 2>"${test_root}/bad-version.err"; then
  fail 'metadata verifier accepted a mismatched version'
fi
grep -q 'workspace version' "${test_root}/bad-version.err" || fail 'metadata mismatch failure was not explicit'
printf 'ok inconsistent release metadata fails closed\n'

printf 'release artifact smoke tests passed\n'
