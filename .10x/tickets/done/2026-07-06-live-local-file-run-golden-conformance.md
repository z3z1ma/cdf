Status: done
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md, .10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md, .10x/tickets/done/2026-07-06-golden-package-conformance-foundation.md, .10x/tickets/done/2026-07-06-prepared-package-chaos-conformance.md

# Add live local-file run golden conformance

## Scope

Extend `cdf-conformance` from prepared packages into the first live source execution gate: a deterministic declarative local-file resource run into DuckDB with a SQLite checkpoint store. Owns `crates/cdf-conformance/**`, conformance golden fixtures under `crates/cdf-conformance/golden/**`, and this ticket's evidence/review records.

The implementation should consume the existing public `cdf-project::run_local_file_to_duckdb_checkpoint` primitive and existing conformance helpers where possible. It must keep the crate root thin and place new code in a focused module such as `crates/cdf-conformance/src/live_run/`.

## Acceptance criteria

- A conformance-owned live local-file fixture builds a declarative file resource, executes it through `run_local_file_to_duckdb_checkpoint`, and proves the run produces a verified package, a verified DuckDB receipt, a committed SQLite checkpoint, and the expected destination rows.
- The fixture records committed golden evidence for the live run package and proves deterministic package evidence across 100 local rebuilds using explicit package ids, checkpoint ids, resource ids, pipeline ids, file contents, and target names.
- The conformance harness proves the commit-gate invariant for the live run committed-before-checkpoint window: after an injected failure immediately after receipt verification, the destination receipt is durable and verifies, the checkpoint head is not committed, and recovery can commit the checkpoint without touching or re-opening the source file.
- The harness proves duplicate replay safety for the live package by re-driving the produced package into the same DuckDB destination and asserting duplicate/no-op receipt behavior and unchanged destination/mirror row counts.
- Negative self-tests prove the harness catches corrupted expected evidence, missing checkpoint commit, missing receipt durability, and wrong destination row counts.
- The implementation does not add native arrow-rs `parquet`/`paste`, does not change the current supply-chain policy, and does not edit production runtime behavior unless the existing public API cannot express an acceptance criterion.

## Evidence expectations

Record focused conformance tests, mutation testing over the new live-run conformance module where feasible, `cargo fmt --all -- --check`, `cargo test -p cdf-conformance --locked --no-fail-fast`, `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`, downstream `cargo test -p cdf-project --locked --no-fail-fast`, `cargo deny check`, `cargo audit`, OSV, Semgrep, gitleaks, and reusable CodeQL analysis. If any QUALITY.md tool is unavailable or structurally inapplicable, record the exact limit.

## Explicit exclusions

No GitHub/API source execution, no SQL source execution, no contract-freeze/drift behavior, no CLI `resume`, no CLI `replay package`, no run-ledger semantics beyond the existing checkpoint ledger, no native Parquet backend policy change, no `.gitignore` edits, and no unrelated conformance refactors.

## Progress and notes

- 2026-07-06: Split from `.10x/tickets/2026-07-05-conformance-chaos-golden.md` after `.10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md` closed the public runtime primitive needed by conformance. The next MVP acceptance demo step is to make the conformance harness consume live local-file execution instead of only prepared package fixtures.
- 2026-07-06: Worker implemented the live-run conformance module in `crates/cdf-conformance/src/live_run/` using the public `cdf_project::run_local_file_to_duckdb_checkpoint` primitive plus existing package replay assertions. The implementation avoids production runtime edits and avoids new dependency edges so `Cargo.lock` and supply-chain policy remain untouched.
- 2026-07-06: Parent review and verification completed. The committed `live-local-file-v1` golden fixture now proves deterministic package evidence across 100 live local-file runs, verified DuckDB receipts, committed SQLite checkpoints, destination/mirror row counts, post-receipt recovery without the source file, duplicate/no-op replay, and negative self-test failures for corrupted expectations. Evidence is recorded in `.10x/evidence/2026-07-06-live-local-file-run-golden-conformance.md`; review passed in `.10x/reviews/2026-07-06-live-local-file-run-golden-conformance-review.md`.

## References

- `.10x/evidence/2026-07-06-live-local-file-run-golden-conformance.md`
- `.10x/reviews/2026-07-06-live-local-file-run-golden-conformance-review.md`

## Blockers

None.
