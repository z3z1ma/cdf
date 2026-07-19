#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: tools/build-duckdb-nanoarrow-runtime.sh --target TARGET --out-dir DIR [options]

Build CDF's pinned DuckDB shared runtime with nanoarrow 0.8.0 and statically linked LZ4.

Options:
  --target TARGET   Rust target label for the native build (required)
  --out-dir DIR     Stable runtime output directory (required)
  --work-dir DIR    Build workspace (default: target/duckdb-nanoarrow-build/TARGET)
  --cache-dir DIR   Verified source archive cache (default: target/duckdb-source-cache)
  --jobs N          Native build parallelism (default: detected logical CPUs)
EOF
}

die() {
  echo "error: $*" >&2
  exit 2
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target=""
out_dir=""
work_dir=""
cache_dir="${repo_root}/target/duckdb-source-cache"
jobs=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--target requires a value'
      target="$2"
      shift 2
      ;;
    --out-dir)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--out-dir requires a value'
      out_dir="$2"
      shift 2
      ;;
    --work-dir)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--work-dir requires a value'
      work_dir="$2"
      shift 2
      ;;
    --cache-dir)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--cache-dir requires a value'
      cache_dir="$2"
      shift 2
      ;;
    --jobs)
      [[ -n "${2:-}" && "${2:-}" != --* ]] || die '--jobs requires a value'
      jobs="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *) die "unknown argument: $1" ;;
  esac
done

[[ -n "$target" ]] || die '--target is required'
[[ -n "$out_dir" ]] || die '--out-dir is required'
work_dir="${work_dir:-${repo_root}/target/duckdb-nanoarrow-build/${target}}"
if [[ -z "$jobs" ]]; then
  jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 1)"
fi
[[ "$jobs" =~ ^[1-9][0-9]*$ ]] || die "--jobs must be a positive integer, observed ${jobs:?}"

duckdb_revision="08e34c447bae34eaee3723cac61f2878b6bdf787"
duckdb_sha256="0deb78bb53dd5030323503b2f3ff5d52d66af7969dcf1f3849a203d7c769e481"
extension_revision="42e4199a67c4cd0789087562a025e87e7130fdc3"
extension_sha256="f8c15cf737c93957f51cebdff06c1ee3a127851cd61ee23546eee2ec98951fae"
nanoarrow_revision="a579fbf5d192e85b6249935e117de7d02a6dc4e9"
nanoarrow_version="0.8.0"
nanoarrow_sha256="ed186f0b8151c323fd41a1b7cfa830abad0ac84e1657cd597da12d98fa9a4be1"
lz4_version="1.9.4"
lz4_sha256="0b0e3aa07c8c063ddf40b082bdf7e37a1562bda40a0ff5272957f3e987e0e54b"

mkdir -p "$cache_dir"

verify_sha256() {
  python3 - "$1" "$2" <<'PY'
import hashlib
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected = sys.argv[2]
observed = hashlib.sha256(path.read_bytes()).hexdigest()
if observed != expected:
    raise SystemExit(f"SHA-256 mismatch for {path}: observed {observed}, expected {expected}")
PY
}

download_verified() {
  local url="$1"
  local destination="$2"
  local expected="$3"
  if [[ -f "$destination" ]]; then
    if verify_sha256 "$destination" "$expected"; then
      return
    fi
    cmake -E remove -f "$destination"
  fi
  local partial="${destination}.partial"
  cmake -E remove -f "$partial"
  curl --fail --location --retry 5 --retry-all-errors --output "$partial" "$url"
  verify_sha256 "$partial" "$expected"
  cmake -E rename "$partial" "$destination"
}

duckdb_archive="${cache_dir}/duckdb-${duckdb_revision}.tar.gz"
extension_archive="${cache_dir}/duckdb-nanoarrow-${extension_revision}.tar.gz"
nanoarrow_archive="${cache_dir}/nanoarrow-${nanoarrow_revision}.zip"
lz4_archive="${cache_dir}/lz4-${lz4_version}.tar.gz"

download_verified "https://github.com/duckdb/duckdb/archive/${duckdb_revision}.tar.gz" "$duckdb_archive" "$duckdb_sha256"
download_verified "https://github.com/paleolimbot/duckdb-nanoarrow/archive/${extension_revision}.tar.gz" "$extension_archive" "$extension_sha256"
download_verified "https://github.com/apache/arrow-nanoarrow/archive/${nanoarrow_revision}.zip" "$nanoarrow_archive" "$nanoarrow_sha256"
download_verified "https://github.com/lz4/lz4/archive/refs/tags/v${lz4_version}.tar.gz" "$lz4_archive" "$lz4_sha256"

cmake -E remove_directory "$work_dir"
cmake -E make_directory "$work_dir"
cmake -E remove_directory "$out_dir"
cmake -E make_directory "$out_dir/lib"
cmake -E make_directory "$out_dir/include"

extract_tar() {
  local archive="$1"
  local destination="$2"
  cmake -E make_directory "$destination"
  tar -xzf "$archive" -C "$destination" --strip-components=1
}

duckdb_source="${work_dir}/duckdb"
extension_source="${work_dir}/duckdb-nanoarrow"
lz4_source="${work_dir}/lz4"
extract_tar "$duckdb_archive" "$duckdb_source"
extract_tar "$extension_archive" "$extension_source"
extract_tar "$lz4_archive" "$lz4_source"

python3 - "$extension_source/CMakeLists.txt" "$nanoarrow_archive" "$nanoarrow_sha256" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
archive = pathlib.Path(sys.argv[2]).resolve().as_uri()
sha256 = sys.argv[3]
text = path.read_text()
old_url = 'URL "https://github.com/apache/arrow-nanoarrow/archive/4bf5a9322626e95e3717e43de7616c0a256179eb.zip"'
old_hash = 'URL_HASH SHA256=49d588ee758a2a1d099ed4525c583a04adf71ce40405011e0190aa1e75e61b59'
if text.count(old_url) != 1 or text.count(old_hash) != 1 or text.count('set(NANOARROW_IPC ON)') != 1:
    raise SystemExit("duckdb-nanoarrow source no longer matches the reviewed pin patch")
text = text.replace('set(NANOARROW_IPC ON)', 'set(NANOARROW_IPC ON)\nset(NANOARROW_IPC_WITH_LZ4 ON)')
text = text.replace(old_url, f'URL "{archive}"')
text = text.replace(old_hash, f'URL_HASH SHA256={sha256}')
path.write_text(text)
PY

lz4_build="${work_dir}/lz4-build"
lz4_install="${work_dir}/lz4-install"
cmake \
  -S "$lz4_source/build/cmake" \
  -B "$lz4_build" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$lz4_install" \
  -DBUILD_SHARED_LIBS=OFF \
  -DBUILD_STATIC_LIBS=ON \
  -DLZ4_BUILD_CLI=OFF \
  -DLZ4_BUILD_LEGACY_LZ4C=OFF \
  -DLZ4_POSITION_INDEPENDENT_LIB=ON
cmake --build "$lz4_build" --config Release --parallel "$jobs" --target install

case "$target" in
  *-pc-windows-msvc) lz4_name='lz4.lib' ;;
  *) lz4_name='liblz4.a' ;;
esac
lz4_library="$(find "$lz4_install" -type f -name "$lz4_name" -print -quit)"
[[ -n "$lz4_library" ]] || die "static LZ4 library ${lz4_name} was not installed"

duckdb_build="${work_dir}/duckdb-build"
cmake \
  -S "$duckdb_source" \
  -B "$duckdb_build" \
  -DCMAKE_BUILD_TYPE=Release \
  -DOVERRIDE_GIT_DESCRIBE="v1.5.4-0-g${duckdb_revision:0:10}" \
  -DEXTENSION_STATIC_BUILD=1 \
  -DDUCKDB_EXTENSION_CONFIGS="$extension_source/extension_config.cmake" \
  -DBUILD_SHELL=OFF \
  -DBUILD_UNITTESTS=OFF \
  -DLZ4_INCLUDE_DIRS="$lz4_install/include" \
  -DLZ4_LIBRARY_DIRS="$(dirname "$lz4_library")" \
  -DLZ4_LIBRARIES="$lz4_library"
cmake --build "$duckdb_build" --config Release --parallel "$jobs" --target duckdb

case "$target" in
  *-apple-darwin)
    runtime_name='libduckdb.dylib'
    import_name=''
    ;;
  *-unknown-linux-gnu)
    runtime_name='libduckdb.so'
    import_name=''
    ;;
  *-pc-windows-msvc)
    runtime_name='duckdb.dll'
    import_name='duckdb.lib'
    ;;
  *) die "unsupported release target: ${target}" ;;
esac

runtime_library="$(find "$duckdb_build" -type f -name "$runtime_name" -print -quit)"
[[ -n "$runtime_library" ]] || die "DuckDB runtime ${runtime_name} was not built"
cmake -E copy "$runtime_library" "$out_dir/lib/$runtime_name"
if [[ -n "$import_name" ]]; then
  import_library="$(find "$duckdb_build" -type f -name "$import_name" -print -quit)"
  [[ -n "$import_library" ]] || die "DuckDB import library ${import_name} was not built"
  cmake -E copy "$import_library" "$out_dir/lib/$import_name"
fi
cmake -E copy "$duckdb_source/src/include/duckdb.h" "$out_dir/include/duckdb.h"
cmake -E copy "$duckdb_source/src/include/duckdb.hpp" "$out_dir/include/duckdb.hpp"

case "$target" in
  *-unknown-linux-gnu)
    if readelf -d "$out_dir/lib/$runtime_name" | grep -Eq 'NEEDED.*liblz4'; then
      die 'custom DuckDB runtime retained a dynamic liblz4 dependency'
    fi
    ;;
  *-apple-darwin)
    if otool -L "$out_dir/lib/$runtime_name" | grep -Eq 'liblz4'; then
      die 'custom DuckDB runtime retained a dynamic liblz4 dependency'
    fi
    ;;
  *-pc-windows-msvc)
    if command -v dumpbin >/dev/null 2>&1 && dumpbin //dependents "$out_dir/lib/$runtime_name" | grep -Eiq 'lz4\.dll'; then
      die 'custom DuckDB runtime retained a dynamic lz4.dll dependency'
    fi
    ;;
esac

python3 - "$out_dir/duckdb-nanoarrow-build.json" "$target" "$runtime_name" \
  "$duckdb_revision" "$duckdb_sha256" "$extension_revision" "$extension_sha256" \
  "$nanoarrow_revision" "$nanoarrow_version" "$nanoarrow_sha256" "$lz4_version" "$lz4_sha256" <<'PY'
import hashlib
import json
import pathlib
import sys

(
    output,
    target,
    runtime_name,
    duckdb_revision,
    duckdb_source_sha256,
    extension_revision,
    extension_source_sha256,
    nanoarrow_revision,
    nanoarrow_version,
    nanoarrow_source_sha256,
    lz4_version,
    lz4_source_sha256,
) = sys.argv[1:]
runtime = pathlib.Path(output).parent / "lib" / runtime_name
document = {
    "schema_version": 1,
    "target": target,
    "duckdb": {"revision": duckdb_revision, "source_sha256": duckdb_source_sha256},
    "duckdb_nanoarrow": {
        "revision": extension_revision,
        "source_sha256": extension_source_sha256,
        "linkage": "statically_linked",
    },
    "nanoarrow": {
        "revision": nanoarrow_revision,
        "version": nanoarrow_version,
        "source_sha256": nanoarrow_source_sha256,
        "ipc_lz4": True,
    },
    "lz4": {
        "version": lz4_version,
        "source_sha256": lz4_source_sha256,
        "linkage": "static",
    },
    "runtime": {
        "file": runtime_name,
        "sha256": hashlib.sha256(runtime.read_bytes()).hexdigest(),
        "bytes": runtime.stat().st_size,
    },
}
pathlib.Path(output).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
PY

echo "DuckDB nanoarrow runtime built: ${out_dir}/lib/${runtime_name}"
echo "DUCKDB_LIB_DIR=${out_dir}/lib"
echo "DUCKDB_INCLUDE_DIR=${out_dir}/include"
echo "CDF_DUCKDB_NANOARROW_STATIC_LINK=1"
