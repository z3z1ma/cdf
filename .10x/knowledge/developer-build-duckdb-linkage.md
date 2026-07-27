Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Developer build and DuckDB linkage

## Purpose

CDF intentionally uses different DuckDB linkage at the developer/benchmark boundary and the
published-release boundary. This record prevents a cold agent from either rebuilding DuckDB from
source on every local change or publishing a nonportable dynamically linked binary.

The build runbook is `.10x/skills/build-and-install-cdf/SKILL.md`.

## Version tuple

The workspace currently pins:

```toml
duckdb = "=1.10504.0"
```

The crate version encodes DuckDB 1.5.4. Treat the Rust crate, `libduckdb-sys`, headers, native
library, feature set, and produced binary as one dependency tuple. Never mix a header/library from
another DuckDB version because it happens to be installed on the host.

## Routine developer and benchmark builds

Set:

```bash
DUCKDB_DOWNLOAD_LIB=1
```

before Cargo:

```bash
DUCKDB_DOWNLOAD_LIB=1 \
CARGO_BUILD_JOBS=12 \
cargo build -p cdf-cli --bin cdf --release --locked -j 12
```

This activates the `libduckdb-sys` prebuilt-library path. It downloads the exact platform archive
published for the DuckDB version encoded by the crate, extracts it beneath:

```text
target/duckdb-download/<target>/<duckdb-version>/
```

and dynamically links it. The native library is also copied into the Cargo profile dependency
directory and rpaths are added for the build layout.

Properties:

- Only values `1` and `true` activate the observed helper.
- The download path is always dynamic; setting `DUCKDB_STATIC` does not turn the downloaded
  archive into a static link.
- The observed helper recognizes macOS universal, Linux amd64/arm64, and Windows amd64/arm64
  release archives. A new target may require source/bundled build or upstream support.
- The first build needs network access to GitHub Releases.
- Subsequent builds reuse that target-directory cache.
- A different `CARGO_TARGET_DIR` has a separate download cache.
- Removing `target/` removes the cache and forces another download.
- `cargo clean` can therefore turn the next build into a networked first build.
- The setting is not stored in `.cargo/config.toml`; every caller or wrapper must provide it.
- The resulting executable is not self-contained and should remain with the matching runtime
  library/layout.

The EC2 benchmark helper sets and records this mode. Keep it that way unless the benchmark is
explicitly measuring release packaging rather than product throughput.

## Published release builds

The release feature chain is:

```text
cdf-cli/bundled-duckdb
→ cdf-dest-duckdb/bundled-duckdb
→ duckdb/bundled
```

Release workflows invoke the bundled feature and deliberately omit `DUCKDB_DOWNLOAD_LIB`:

```bash
cargo build \
  -p cdf-cli \
  --bin cdf \
  --release \
  --locked \
  --features bundled-duckdb
```

This compiles DuckDB into the release artifact. The hosted workflow inspects native dependencies
and fails if a dynamic DuckDB library remains. That expensive compile is a portability and
distribution requirement, not local-development toil.

Do not make hosted release CI use `DUCKDB_DOWNLOAD_LIB=1`. Do not weaken the native-dependency
inspection to make a workflow green.

Current release authority:

- `.github/workflows/release-artifacts.yml`
- `.10x/specs/versioning-lts-release-policy.md`
- `.10x/evidence/2026-07-26-p1-ws8-hosted-release.md`
- `docs/operators/release-install.md`

## Local installation choices

The safest developer choices are:

1. Execute the workspace binary in place.
2. Add the workspace profile directory to the current shell path.
3. Install a published checksummed release with `tools/install-cdf.sh`.

Do not copy only a dynamically linked developer executable to `~/.local/bin` and delete its target
tree. It can fail at startup because the matching native DuckDB library is no longer available at
the recorded rpath.

If a local developer binary must be installed independently, either:

- use the published static artifact; or
- install the exact matching native library and deliberately configure its runtime search path,
  accepting that this is a machine-local developer installation.

## macOS and Linux loader behavior

The repository Cargo configuration adds loader-relative rpaths. On aarch64 macOS it also selects
Clang, lld, and `target-cpu=native` for local performance. Consequences:

- a local aarch64 macOS developer binary can be tuned to that host CPU;
- it is not a universal distribution binary;
- a downloaded dynamic library must remain reachable through the produced layout/rpath;
- changing loader environment variables can cause a different compatible-looking DuckDB library
  to be selected and invalidate both correctness and benchmark comparability.

When diagnosing an unexpected binary:

```bash
otool -L target/release/cdf      # macOS
ldd target/release/cdf           # Linux
```

Verify the native dependency and run:

```bash
target/release/cdf version
```

Do not rely on `which cdf` alone; shells frequently resolve an older global install while the
workspace build is fresh.

## Weaknesses in the downloaded-library path

The `libduckdb-sys 1.10504.0` build script inspected on 2026-07-26 uses HTTPS and a
version-derived GitHub release URL, but it does not verify a separately pinned checksum before
extraction. Its local cache reuse is based on the extracted library being present, not on a
CDF-owned content hash. Revalidate this finding whenever the pinned crate changes.

Therefore:

- this path is allowed for local iteration and controlled benchmark hosts;
- it is not the artifact supply-chain authority for published releases;
- a corrupted or replaced cache should be deleted and redownloaded;
- benchmark evidence must record the crate/lock revision and linkage mode;
- the benchmark host should not compare a downloaded developer tuple with a bundled release tuple
  as though only product code changed;
- introducing a CDF-owned checksum manifest would be a legitimate future hardening ticket if
  developer build supply-chain integrity becomes a product requirement.

Do not silently invent a checksum in a script without an owning record and update procedure. A
stale checksum table is worse than an explicit developer-only boundary.

## Build-time and disk expectations

Without the download setting, DuckDB C++ compilation dominates local clean-build time and creates
large native archives in Cargo output. Repeated target directories or feature tuples multiply that
cost. Use:

- one normal workspace `target/` for ordinary work;
- isolated `CARGO_TARGET_DIR`s only for checks that truly require isolation;
- `DUCKDB_DOWNLOAD_LIB=1` for each isolated developer target that includes DuckDB;
- `du -sh target` before assuming a slow run is a product regression.

Do not run destructive cleanup while another worker or Cargo process owns the target directory.
Target pressure can cause swap, disk-full errors, and misleading throughput results. Cleanup is an
operational action, not a substitute for identifying the first bad source change.

## Historical-record trap

Some terminal ticket journals describe an earlier state in which a downloaded dynamic DuckDB path
was discussed for release. Those statements are historical. The current hosted workflow and
release evidence establish the static-release rule. Never copy a terminal journal command into CI
without reconciling it against current workflow source.

## Change checklist

Any change to the DuckDB crate version, feature chain, download mode, release target matrix, loader
paths, or artifact packaging must verify:

1. `Cargo.lock` and the crate/native version tuple agree.
2. Fast developer builds still avoid C++ compilation.
3. The benchmark helper records the actual linkage mode.
4. Published artifacts contain no dynamic DuckDB dependency.
5. All supported release targets build and execute `cdf version`.
6. The install script verifies checksums before replacing an existing binary.
7. Documentation names the same commands as the workflow.
8. No superseded nanoarrow/custom-DuckDB build path has been revived.
