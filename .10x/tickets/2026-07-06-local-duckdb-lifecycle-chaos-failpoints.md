Status: open
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-06-package-replay-firn-line-runtime.md, .10x/tickets/done/2026-07-06-prepared-package-chaos-conformance.md, .10x/tickets/done/2026-07-06-live-local-file-run-golden-conformance.md

# Mechanize local DuckDB lifecycle chaos failpoints

## Scope

Extend the local DuckDB/SQLite runtime and conformance harness from the single post-receipt hook into named lifecycle chaos failpoints for the normative package/checkpoint crash matrix.

Owns:

- `crates/firn-project/**` for additive test-only/runtime failpoint hooks and reports.
- `crates/firn-conformance/**` for reusable lifecycle chaos harnesses and tests.
- `.10x/` evidence/review/ticket records for this child.

The implementation must keep crate roots thin and place new code in focused modules. The public runtime surface may grow only by additive hook/config types that default to no hooks and preserve existing behavior.

## Acceptance criteria

- `firn-project` exposes additive, named local DuckDB/SQLite runtime failpoints covering at least:
  - after package reaches `packaged` and before any destination write;
  - after checkpoint proposal and before destination write;
  - after durable destination receipt verification and before checkpoint commit;
  - after checkpoint commit and before package status becomes `checkpointed`.
- Existing `after_receipt_verified` behavior remains source-compatible or has a narrow compatibility adapter.
- Existing live local-file and prepared-package runtime calls with no failpoint configured behave identically and keep package hashes stable where no new identity artifacts are written.
- `firn-conformance` drives each named failpoint through a helper process or equivalent process-boundary mechanism where durable state matters, then asserts recovery terminates with no checkpoint head ahead of durable destination data.
- The pre-destination failpoints prove no destination rows, no `_firn_loads`, no `_firn_state`, and no committed checkpoint head exist after failure.
- The post-receipt/pre-checkpoint failpoint continues to prove durable receipt recovery without source contact and no second destination write.
- The post-checkpoint/pre-status failpoint proves the ledger head is committed, the destination receipt verifies, and recovery/finalization makes package status `checkpointed` without reloading destination data or moving the source cursor.
- Negative self-tests prove the chaos harness fails if it skips destination-no-write, checkpoint-head, receipt durability, or package-status assertions.
- The implementation does not change the package state/commit artifact contract tracked by `.10x/tickets/done/2026-07-06-package-state-commit-artifact-contract.md`, does not implement CLI `resume`, and does not broaden native Parquet policy.

## Evidence expectations

Record focused `cargo fmt --all -- --check`, `git diff --check`, `cargo test -p firn-project --locked --no-fail-fast`, `cargo test -p firn-conformance --locked --no-fail-fast`, `cargo clippy -p firn-project -p firn-conformance --all-targets --locked -- -D warnings`, and bounded mutation testing over the new runtime/failpoint and conformance chaos modules where feasible.

Before closure, run relevant `QUALITY.md` gates, parallelized where practical, including workspace check/test/clippy, nextest, docs, deny/audit/vet/OSV, Semgrep, source-only gitleaks, direct unsafe scan, dependency hygiene, and CodeQL through the reusable `tools/codeql-rust-quality.sh` database wrapper.

## Explicit exclusions

No package state/commit artifact schema changes, no CLI `resume` or `replay package`, no run-ledger default IDs, no generic destination finalization trait, no Postgres/Parquet chaos, no REST/SQL source execution, no full MVP killer-demo harness, no CI workflow changes, no native Arrow/DataFusion Parquet policy change, and no `.gitignore` edits.

## References

- `firn-the-book-of-the-system.md` Chapter 11 lifecycle/crash matrix, Chapter 12 firn-line invariant, Chapter 19 chaos layer, and Chapter 22 killer demo.
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/checkpoint-state-firn-line.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/tickets/done/2026-07-06-package-replay-firn-line-runtime.md`
- `.10x/tickets/done/2026-07-06-prepared-package-chaos-conformance.md`
- `.10x/tickets/done/2026-07-06-live-local-file-run-golden-conformance.md`
- `.10x/tickets/done/2026-07-06-package-state-commit-artifact-contract.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/knowledge/rust-crate-organization.md`

## Progress and notes

- 2026-07-06: Split from the conformance parent after live local-file golden conformance closed. Current runtime/conformance already prove the post-receipt/pre-checkpoint window; this child broadens that into named failpoints for the local DuckDB/SQLite crash matrix without waiting on the separate package artifact contract.
- 2026-07-06: Do not implement in the ticket-creation turn. Assign to a worker in a later turn with a write boundary of `crates/firn-project/**`, `crates/firn-conformance/**`, and this ticket's records.

## Blockers

None for the local DuckDB/SQLite lifecycle chaos failpoint slice.
