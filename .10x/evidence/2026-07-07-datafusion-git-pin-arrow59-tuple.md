Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md, .10x/decisions/superseded/datafusion-git-pin-arrow59-tuple.md, .10x/decisions/arrow-datafusion-tuple-policy.md, .10x/decisions/datafusion-tier-b-delegation-boundary.md

# DataFusion git-pin Arrow 59 tuple evidence

## What was observed

The workspace now uses the ratified Apache DataFusion git pin at rev `7ff7278edc1bf7446303bff51e5883a38414bbdf`. First-party Arrow and Parquet crate manifests and `Cargo.lock` resolve to the Arrow/Parquet `59.1.0` line. `cdf-python` remains on `pyo3-arrow 0.19.0`, `pyo3 0.29.0`, and `numpy 0.29.0`. `deny.toml` allows only the exact Apache DataFusion git source URL, and `supply-chain/config.toml` contains cargo-vet exemptions for the newly resolved tuple entries.

No lower crate outside `cdf-engine` exposes or depends on DataFusion source references. The test fixtures that encode the dependency tuple were updated from `59.0.0` to `59.1.0`. One stale CLI test assertion was updated to the active ordered-cursor blocker text introduced by the already-closed non-file window-close semantics work.

## Procedure

- `cargo metadata --locked --format-version 1 >/tmp/cdf-metadata-datafusion-gitpin.json`: passed.
- `cargo tree --workspace --locked -i datafusion@54.0.0`: passed; DataFusion resolves from `https://github.com/apache/datafusion.git?rev=7ff7278edc1bf7446303bff51e5883a38414bbdf#7ff7278e` through `cdf-engine`.
- `cargo tree --workspace --locked -i arrow-array@59.1.0`: passed; DataFusion and first-party CDF Arrow paths are on Arrow `59.1.0`.
- `cargo tree --workspace --locked -i pyo3@0.29.0` and `cargo tree --workspace --locked -i pyo3-arrow@0.19.0`: passed.
- `cargo tree --workspace --locked -i arrow-array@58.3.0`: still finds only the existing `duckdb 1.10504.0 -> arrow 58.3.0` path.
- `cargo tree --workspace --locked -i thrift@0.17.0`: no matching package; `cargo tree --workspace --locked | rg -n "\bthrift\b" || true` produced no output.
- `rg -n "datafusion::|\bdatafusion\b|DataFusion" <lower-crate paths>`: no output for kernel, package, formats, declarative, destination, state, subprocess, Python, or HTTP crates.
- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed.
- `cargo hack check --workspace --all-targets --feature-powerset --locked`: passed across 17 workspace crates.
- `cargo test -p cdf-cli run_sql_resource_resolves_secret_without_leaking_before_cursor_blocker --locked`: passed after aligning the assertion to active ordered-cursor semantics.
- `cargo test -p cdf-package --locked --no-fail-fast`: passed, 26 unit tests and 0 doc tests.
- `cargo test -p cdf-conformance --locked --no-fail-fast`: passed, 40 tests and 0 doc tests, including the 100-run golden-package checks.
- `cargo test -p cdf-formats --locked --no-fail-fast`: passed, 6 tests and 0 doc tests.
- `cargo test -p cdf-dest-parquet --locked --no-fail-fast`: passed, 18 tests and 0 doc tests.
- `cargo nextest run --workspace --locked --no-fail-fast`: passed, 411/411 tests.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed; all workspace crates had 0 doc tests.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo semver-checks --workspace --baseline-rev HEAD`: passed for all workspace crates; each reported no semver update required.
- `cargo machete`: passed, no unused dependency candidates.
- `rg -n "\bunsafe\b|extern \"|unsafe impl|Send for|Sync for|from_raw|into_raw|\*const|\*mut" crates --glob '*.rs'`: found only ordinary string/test occurrences of the word `unsafe`; no Rust unsafe, FFI, raw pointer, or unsafe impl surfaces.
- `cargo deny check`: passed. It emitted duplicate-version warnings for the known DuckDB Arrow 58 residual and the new Arrow 59.1 graph, then reported advisories, bans, licenses, and sources ok.
- `cargo audit`: passed with one allowed warning, `RUSTSEC-2024-0436` for `paste 1.0.15`.
- `cargo vet --locked`: passed; `Vetting Succeeded (393 exempted)`.
- `osv-scanner --lockfile Cargo.lock`: exited 1 only for the already-ratified `RUSTSEC-2024-0436` / `paste 1.0.15` advisory, with no fixed version.
- `semgrep scan --config auto --error --quiet`: passed.
- `tools/codeql-rust-quality.sh`: passed. The reusable database at `target/quality/codeql-db-rust` was refreshed because Rust source, manifest, or lockfile content changed. SARIF result count was 0. The wrapper reported 0 extraction errors, 2764 extraction warnings, 153 files extracted, and the known local macro-expansion warning profile.

## What this supports

This supports closing the DataFusion tuple ticket: CDF's engine can now compile against a same-major Arrow/DataFusion tuple without a permanent Arrow-major bridge, without downgrading the Python bridge tuple, without adding unratified `pyo3 0.28.x` or `thrift 0.17.0` advisories, and without exposing DataFusion through lower crates.

## Limits

The workspace still contains Arrow `58.3.0` through `duckdb 1.10504.0`; that is not the DataFusion engine tuple and is now owned by `.10x/tickets/done/2026-07-07-duckdb-arrow58-transitive-residual.md`.

CodeQL extraction warnings match the known local extractor limitation recorded in `.10x/knowledge/quality-gate-execution.md`; the actual SARIF result count was 0. OSV's nonzero exit is the already-ratified `paste` advisory only. Coverage, Miri, fuzzing, Kani, mutation testing, and profiling were not run for this dependency tuple slice because the implementation changed manifests, lockfile policy, and test fixtures but did not add new unsafe code, parsers, algorithms, or performance-sensitive runtime logic.
