Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-prepared-package-chaos-conformance.md
Verdict: pass

# Prepared-package chaos conformance review

## Target

Reviewed the prepared-package DuckDB/SQLite chaos conformance foundation implemented in `crates/cdf-conformance/src/package_replay/`, the scoped dependency changes in `crates/cdf-conformance/Cargo.toml` and `Cargo.lock`, the thin crate-root export in `crates/cdf-conformance/src/lib.rs`, and the closure evidence in `.10x/evidence/2026-07-06-prepared-package-chaos-conformance.md`.

## Findings

No blocking findings.

- The implementation stays within the ticket's conformance scope. It consumes public `cdf-project`, `cdf-package`, `cdf-dest-duckdb`, and `cdf-state-sqlite` APIs rather than duplicating product sequencing or changing runtime behavior.
- The crate root remains a thin module/export root. The new behavior is split into `package_replay/mod.rs` and `package_replay/tests.rs`, matching `.10x/knowledge/rust-crate-organization.md`.
- The helper-process crash boundary is narrow and test-only. It uses `after_receipt_verified` to stop after durable receipt verification and before checkpoint commit, which is exactly the ticketed committed-before-checkpointed window.
- The assertions cover the high-risk edges: package receipt durability, DuckDB receipt verification, `_cdf_loads` and `_cdf_state` mirror evidence, checkpoint-head absence/presence, duplicate/no-op replay identity, recovery without a second destination write, and package segment or receipt tampering.
- The negative self-tests and mutation result reduce the risk that the harness can pass while silently skipping core assertions. The mutation run over `package_replay/mod.rs` had 0 missed mutants.
- The Semgrep `current_exe` suppression is constrained to a test-only helper that must respawn the current libtest binary; it is not product execution or a user-controlled subprocess path.

## Residual Risk

- Geiger and Kani remain tool-limited for this repository state, as recorded in evidence. The direct owned-source unsafe scan found no owned unsafe Rust, FFI, or raw-pointer surface in this slice, and `cargo careful` passed the conformance tests.
- The broader parent still owns full lifecycle killpoints, golden-package fixtures, resource data completeness, live Postgres conformance, and MVP demo evidence. This review does not claim those parent outcomes are complete.

## Verdict

Pass. The ticket acceptance criteria are supported by implementation, focused tests, broad quality gates, mutation testing, and documented tool limits.
