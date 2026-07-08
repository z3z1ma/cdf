Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-p0-b3-generic-project-run-resolution.md, .10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md

# P0 B3 Generic Project Run Resolution Evidence

## What Was Observed

`run_project` now consumes `ProjectRunSource<'_>` and `ResolvedProjectDestination` instead of the former public closed `ProjectRunResource` / `ProjectRunDestination` enums. The old enum names are absent from Rust source:

```text
rg -n "ProjectRunDestination|ProjectRunResource" crates/cdf-project/src crates/cdf-cli/src crates/cdf-conformance/src -g '*.rs'
exit 1, no matches
```

`ProjectRunSource` wraps `&dyn QueryableResource` and exposes the stream, descriptor, and capabilities through the trait. Local file, REST, and SQL constructors retain resource-specific dependency validation without making orchestration match on a public closed enum.

`ResolvedProjectDestination` wraps a destination runtime resolved through `ProjectDestinationRegistry` / `ProjectDestinationDriver`. Built-in drivers now resolve `duckdb://`, `parquet://`, filesystem Parquet paths, and Postgres destinations behind the registry. `cdf-cli run` delegates destination resolution to `cdf-project::resolve_project_run_destination`.

`run_project` and `run_project_inner` no longer match over DuckDB, Parquet, or Postgres destination variants. They call the generic `replay_package_with_runtime` path through `execution.destination.runtime_mut()`.

The destination runtime code is split rather than concentrated in one module:

```text
   83 crates/cdf-project/src/runtime.rs
  498 crates/cdf-project/src/runtime/destinations.rs
  124 crates/cdf-project/src/runtime/destinations/duckdb.rs
  169 crates/cdf-project/src/runtime/destinations/parquet.rs
  278 crates/cdf-project/src/runtime/destinations/postgres.rs
```

The specialized replay/recover wrapper families still exist and are intentionally owned by B4:

```text
.10x/tickets/2026-07-07-p0-b4-caller-migration-wrapper-deletion.md
```

## Procedure

Commands run after the final B3 source shape:

```text
cargo check -p cdf-project -p cdf-cli -p cdf-conformance --all-targets
cargo clippy -p cdf-project -p cdf-cli -p cdf-conformance --all-targets -- -D warnings
cargo test -p cdf-project runtime_tests -- --nocapture
cargo test -p cdf-cli run_ -- --nocapture
cargo test -p cdf-conformance live_run -- --nocapture
cargo hack check -p cdf-project -p cdf-cli -p cdf-conformance --all-targets --each-feature --locked
cargo hack clippy -p cdf-project -p cdf-cli -p cdf-conformance --all-targets --each-feature --locked -- -D warnings
cargo nextest run -p cdf-project -p cdf-cli -p cdf-conformance --locked
cargo fmt --all --check
git diff --check
semgrep scan --config p/rust --error --json --output target/quality/reports/b3/semgrep-rust.json --no-git-ignore crates/cdf-project/src crates/cdf-cli/src crates/cdf-conformance/src/live_run
cargo deny check
cargo audit
cargo vet --locked
osv-scanner scan source -r . --format json --output target/quality/reports/b3/osv.json
tools/codeql-rust-quality.sh
gitleaks dir crates/cdf-project/src --no-banner --redact --report-format json --report-path target/quality/reports/b3/gitleaks-project-src.json
gitleaks dir crates/cdf-cli/src --no-banner --redact --report-format json --report-path target/quality/reports/b3/gitleaks-cli-src.json
gitleaks dir crates/cdf-conformance/src/live_run --no-banner --redact --report-format json --report-path target/quality/reports/b3/gitleaks-conformance-live-run.json
jscpd crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs crates/cdf-cli/src crates/cdf-conformance/src/live_run --reporters json --output target/quality/reports/b3/jscpd
rust-code-analysis-cli -m -p crates/cdf-project/src/runtime -O json -o target/quality/reports/b3/rust-code-analysis-runtime
scc --format json crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime crates/cdf-project/src/runtime_tests.rs crates/cdf-cli/src crates/cdf-conformance/src/live_run
cargo semver-checks -p cdf-project --baseline-rev HEAD
```

## Results

Focused Rust verification passed:

- `cargo check -p cdf-project -p cdf-cli -p cdf-conformance --all-targets`
- `cargo clippy -p cdf-project -p cdf-cli -p cdf-conformance --all-targets -- -D warnings`
- `cargo test -p cdf-project runtime_tests -- --nocapture`: 49 passed
- `cargo test -p cdf-cli run_ -- --nocapture`: 25 passed
- `cargo test -p cdf-conformance live_run -- --nocapture`: 4 passed
- `cargo hack check -p cdf-project -p cdf-cli -p cdf-conformance --all-targets --each-feature --locked`
- `cargo hack clippy -p cdf-project -p cdf-cli -p cdf-conformance --all-targets --each-feature --locked -- -D warnings`
- `cargo nextest run -p cdf-project -p cdf-cli -p cdf-conformance --locked`: 208 passed
- `cargo fmt --all --check`
- `git diff --check`

Security and supply-chain checks:

- Semgrep: 0 findings in `target/quality/reports/b3/semgrep-rust.json`.
- Gitleaks: 0 leaks across project, CLI, and conformance live-run reports.
- CodeQL: reused `target/quality/codeql-db-rust`; SARIF `target/quality/reports/codeql-rust-current.sarif` has 1 run, 25 rules, 0 results, and `executionSuccessful=true`.
- `cargo deny check`: passed with the known duplicate Arrow warnings.
- `cargo audit`: passed except for the already-ratified `RUSTSEC-2024-0436` paste advisory.
- `cargo vet --locked`: passed.
- OSV: one vulnerability, `paste 1.0.15 / RUSTSEC-2024-0436`, already ratified.

Quality metrics:

- `jscpd`: 49 Rust files, 20,195 lines, 154 clones, 1,610 duplicated lines (7.97%), 11,472 duplicated tokens (9.25%). This decreased from the inherited B3 pre-split snapshot.
- `scc`: 49 files, 20,195 lines, 14,727 code lines, 4,489 comment lines, 979 blank lines, complexity 687.
- `rust-code-analysis-cli` top runtime module cyclomatic totals: `replay.rs` 166, `destinations.rs` 107, `artifacts.rs` 97, `destinations/postgres.rs` 65, `validation.rs` 41, `orchestration.rs` 39, `destinations/parquet.rs` 39, `destinations/duckdb.rs` 33.
- `rust-code-analysis-cli` top function cyclomatic totals: `replay_package_with_runtime` 19, `close_cursor_value` 19, Postgres driver `resolve` 17, `run_project_inner` 14.

`cargo semver-checks -p cdf-project --baseline-rev HEAD` exited 1 only for the intentional public removal of `cdf_project::ProjectRunResource` and `cdf_project::ProjectRunDestination`. This is expected under B3's acceptance criteria and the pre-1.0 P0 refactor: the closed public enums were the debt being removed.

## What This Supports

B3 acceptance criteria are satisfied:

- Old closed run resource/destination enum names are gone from Rust source.
- `run_project` no longer closes over DuckDB/Parquet/Postgres destination variants.
- File, REST, and SQL resources enter orchestration through a trait-backed source wrapper after dependency validation.
- Checkpointability and dependency validation are descriptor/capability/resource driven rather than based on the old public closed resource enum.
- CLI `run` uses project-owned destination resolution.
- Focused project, CLI, and conformance live-run tests pass through the generic run path.

## Limits

B3 does not close all Workstream B obligations. B4 still owns migration and deletion of specialized public replay/recover wrappers, CLI replay/resume migration, conformance package replay migration, and Workstream B parent closure.
