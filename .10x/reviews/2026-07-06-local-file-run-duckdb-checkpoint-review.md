Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md
Verdict: pass

# Local file run to DuckDB checkpoint review

## Target

Review of the explicit local file resource to DuckDB destination and SQLite checkpoint run slice implemented under `.10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md`.

## Findings

No blocking findings remain.

Resolved during parent review:

- The CLI report originally inferred `package_id` from the package directory. The project report now carries the explicit package id from the request, and CLI JSON uses that field.
- The project helper originally accepted path-like package ids. It now rejects empty or multi-component package ids before package/destination/checkpoint writes; CLI tests cover `../pkg-run-escape`.
- Negative coverage originally missed non-file resource validation, plan/package id mismatch, and divergent engine/package segment source positions. Mutation testing exposed those gaps; focused tests now kill the surviving mutants.
- The engine exposes source-position evidence through additive APIs while preserving the original `execute_to_package` and `execute_to_package_with_run_id` return shapes.

## Assumptions tested

- Explicit ids remain explicit: pipeline id, target, package id, and checkpoint id come from command inputs and are not inferred from filenames, project names, or destination names.
- No-write failures happen before durable writes for omitted ids, unsupported resource kinds, unsupported destinations, existing package dirs, path-like package ids, and discovered-schema resources.
- The firn-line invariant holds: SQLite head is committed only after the destination receipt verifies, and the post-receipt/pre-checkpoint window is recoverable.
- Source-position evidence used for checkpoint output position comes from engine-observed file manifest segment positions and fails closed on divergence.
- The implementation does not broaden native Parquet policy, advisory ignores, run-ledger semantics, multi-resource runs, `resume`, or package replay CLI behavior.

## Evidence reviewed

- `.10x/evidence/2026-07-06-local-file-run-duckdb-checkpoint.md`
- Source diff in `crates/firn-engine`, `crates/firn-project`, `crates/firn-cli`, and relevant Cargo manifests/lockfile.
- Final mutation rerun with 45 mutants tested, 36 caught, 9 unviable, 0 missed.
- CodeQL SARIF `target/quality/reports/codeql-rust-current.sarif` with 0 results and the known macro warning profile.

## Residual risk

- This slice intentionally leaves broader `run`, run-ledger, `resume`, `replay package`, REST/SQL resources, non-DuckDB destinations, and multi-resource orchestration open under the parent CLI/system tickets.
- CodeQL Rust extractor diagnostics continue to be macro-warning heavy. This is bounded by 0 SARIF findings, 0 extraction error query results, and the broader scanner/test matrix.
- The CLI uses `futures_executor::block_on` for this synchronous command slice. That is acceptable for the current CLI entry point; a later async runtime policy would need its own ticket if CLI orchestration becomes concurrently async.
