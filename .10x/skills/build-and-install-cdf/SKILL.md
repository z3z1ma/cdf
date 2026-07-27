---
name: build-and-install-cdf
description: Use when building CDF for local iteration, refreshing the user's local binary, or validating portable static release artifacts.
metadata:
  created: 2026-07-26
  updated: 2026-07-26
---

# Build and Install CDF

## Choose the correct artifact

CDF has two intentionally different DuckDB linkage modes:

| Need | DuckDB path | Portable? |
|---|---|---|
| Local development, tests, benchmark host | `DUCKDB_DOWNLOAD_LIB=1`, downloaded dynamic library | No; keep binary with its target runtime |
| Published release artifact | `--features bundled-duckdb`, source-built static library | Yes, after release packaging/verification |

Read `.10x/knowledge/developer-build-duckdb-linkage.md` before changing these modes. Never use
`DUCKDB_DOWNLOAD_LIB=1` to produce a published release, and never pay the static source-build cost
for routine local checks.

## Fast local debug build

Use real host parallelism:

```bash
DUCKDB_DOWNLOAD_LIB=1 \
CARGO_BUILD_JOBS=12 \
cargo build -p cdf-cli --bin cdf --locked -j 12

target/debug/cdf version
```

Adjust `12` to the machine's admitted CPU/memory capacity; do not force one build job. The initial
build needs network access to the exact DuckDB release asset. Later builds reuse
`target/duckdb-download/<target>/<duckdb-version>/`.

## Optimized local developer build

```bash
DUCKDB_DOWNLOAD_LIB=1 \
CARGO_BUILD_JOBS=12 \
cargo build -p cdf-cli --bin cdf --release --locked -j 12

target/release/cdf version
```

This uses the repository release profile (`opt-level=3`, fat LTO, one codegen unit, aborting panic,
stripped symbols) but still dynamically links the downloaded developer DuckDB. Fat-LTO final links
can be quiet and mostly single-core for minutes even after parallel compilation finishes.

The repository's aarch64 macOS Cargo config uses `target-cpu=native`; this developer binary is
host-tuned, not a distribution artifact.

## Refresh the command used in a local shell

Prefer one of these explicit approaches:

1. Point the shell directly at the workspace binary:

   ```bash
   export CDF="$PWD/target/release/cdf"
   "$CDF" version
   ```

2. Install the current published checksummed artifact:

   ```bash
   CDF_RELEASE_VERSION=0.2.0-alpha.1 # replace with the current published version
   tools/install-cdf.sh --version "$CDF_RELEASE_VERSION" --prefix "$HOME/.local"
   "$HOME/.local/bin/cdf" version
   ```

3. Package and install a local artifact only when testing release mechanics:

   ```bash
   tools/test-release-artifacts.sh
   tools/test-install-cdf.sh
   ```

Do not copy `target/release/cdf` alone into `~/.local/bin`. The developer build depends on the
downloaded `libduckdb` under the target tree and may also depend on the embedded Python runtime
available in the build environment. A bare copy can link successfully and then fail at process
startup.

## Focused checks

Keep the developer linkage explicit:

```bash
CRATE=cdf-source-files

DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 \
  cargo test -p "$CRATE" --locked -j 12

DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 \
  cargo clippy -p "$CRATE" --all-targets --locked -j 12 -- -D warnings
```

Use the exact profile from `QUALITY.md` for a ticket. Avoid broad workspace builds when focused
owner tests prove the change. Never run `cargo geiger` against the normal target cache; see
`.10x/knowledge/quality-gate-execution.md`.

## Validate the static release graph

Published artifacts are built only through `.github/workflows/release-artifacts.yml`:

```bash
cargo tree -p cdf-cli --locked --features bundled-duckdb \
  -e features -i libduckdb-sys
```

The required chain is:

```text
cdf-cli/bundled-duckdb
→ cdf-dest-duckdb/bundled-duckdb
→ duckdb/bundled
→ libduckdb-sys/bundled
```

The release build command is conceptually:

```bash
RELEASE_TARGET=aarch64-apple-darwin # choose one hosted target

cargo build -p cdf-cli --release --locked --target "$RELEASE_TARGET" \
  --features bundled-duckdb
```

Do not set `DUCKDB_DOWNLOAD_LIB`. The hosted workflow overrides developer-native flags, verifies
the binary has no dynamic DuckDB dependency, stages the required Python runtime and license,
packages deterministic checksummed archives, verifies every promised target, and exercises an
actual published artifact through the public installer.

A local static build is not substitute evidence for the hosted target matrix. It is expensive,
platform-specific, and can miss target-native linker/runtime defects.

## Diagnose DuckDB build/link failures

1. Determine the intended mode:

   ```bash
   env | grep '^DUCKDB_' || true
   cargo tree -p cdf-cli --locked -e features -i libduckdb-sys
   ```

2. For developer mode, ensure `DUCKDB_DOWNLOAD_LIB=1` is on the same Cargo command that builds the
   graph. It is not globally set by the repository.
3. Inspect the exact cache:

   ```text
   target/duckdb-download/<target>/<duckdb-version>/
   target/<profile>/deps/libduckdb.*
   ```

4. If the downloaded marker exists but the archive/library is corrupt, delete only that exact
   versioned cache directory and rebuild. Do not delete the entire target tree reflexively.
5. If a moved binary cannot load `libduckdb`, run it from the target layout or use a packaged
   release. Avoid global `DYLD_*`/`LD_LIBRARY_PATH` workarounds that can silently select a different
   DuckDB.
6. If Cargo attempts `-lduckdb` from the system, the download variable was absent/false and no
   supplied/system library was found.
7. If a release artifact dynamically depends on DuckDB, fail the release; do not package the
   dynamic developer runtime as a shortcut.

## Target/disk hygiene

DuckDB source builds can make `target/` enormous. Routine developer commands should use the
downloaded-prebuilt mode. Before deleting caches:

- inspect disk and swap pressure;
- preserve unrelated workers' target directories;
- prefer a ticket-specific `CARGO_TARGET_DIR=/tmp/...` only when isolation is worth a rebuild;
- remember that changing `CARGO_TARGET_DIR` creates a separate DuckDB download cache.

Do not benchmark a laptop under disk-full/swap pressure. Do not interpret a one-core fat-LTO link
as a runtime CPU-saturation defect.
