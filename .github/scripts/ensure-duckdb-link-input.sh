#!/usr/bin/env bash
set -euo pipefail

if find target/duckdb-download -type f \
  \( -name 'libduckdb.so*' -o -name 'libduckdb.a' \) \
  -print -quit 2>/dev/null | grep -q .; then
  exit 0
fi

# Rust caches can restore libduckdb-sys build artifacts without the downloaded native library.
# Removing only this package's artifacts makes Cargo rerun its build script and restore the
# platform library before the next workspace command links a binary.
cargo clean -p libduckdb-sys
