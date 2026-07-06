Status: blocked
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/tickets/done/2026-07-05-contract-compiler-normalization.md

# Implement Postgres destination

## Scope

Implement `firn-dest-postgres`: destination sheet, type mappings, identifier rules, transactional DDL, append/replace/merge with `ON CONFLICT`, xid-bearing receipts, `_firn_loads` and `_firn_state`, receipt verification, and Postgres source-side exercise hooks where appropriate. Owns `crates/firn-dest-postgres/**`.

## Acceptance criteria

- Append, transactional replace, and merge work with deterministic dedup.
- DDL migration plans are dry-runnable and included in commit plans.
- Receipts include transaction metadata, counts, schema hash, migrations, and verification query.
- Ledger/mirror drift can be detected by project/doctor code.
- Type mappings handle exact/widening/lossy/unsupported fidelity per sheet.

## Evidence expectations

Record integration tests against Postgres or a test container, including dispositions, DDL, idempotency/replay, receipt verification, and mirror state.

## Explicit exclusions

No warehouse destinations in this ticket.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to Postgres destination worker. Worker owns `crates/firn-dest-postgres/**` plus its own evidence/review records and this ticket. Do not touch `.gitignore`, parent ticket, other destination crates, or unrelated records.
- 2026-07-06: Implemented deterministic Postgres destination sheet, exact/widening/lossy/unsupported type mappings, identifier validation/quoting, dry-runnable transactional DDL/DML plans for append/replace/merge, explicit merge dedup policy SQL, xid-bearing receipt construction, `_firn_loads`/`_firn_state` mirror SQL, source-side exercise SQL hooks, and doctor/project drift probe hooks in `crates/firn-dest-postgres`. Evidence recorded in `.10x/evidence/2026-07-06-postgres-destination.md`.
- 2026-07-06: Parent integration revalidated package tests, clippy, formatting, `cargo audit`, `cargo deny check advisories`, OSV, and `git diff --check` after Python upgraded to PyO3 0.29 through `pyo3-arrow`. The stale PyO3 advisory blocker is resolved. This ticket remains blocked because the current crate has no live Postgres driver/execution path or live integration evidence for append, replace, merge, receipt verification, or rollback behavior.
- 2026-07-06: Split the large `crates/firn-dest-postgres/src/lib.rs` into focused files under `crates/firn-dest-postgres/src/` while preserving the crate-root API. Organization evidence recorded in `.10x/evidence/2026-07-06-rust-crate-organization-refactor.md`.
- 2026-07-06: Replaced the intermediate `include!` split with ordinary Rust modules under `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`. Final parent quality gates recorded in `.10x/evidence/2026-07-06-project-python-destinations-quality-gates.md`. This ticket remains blocked on live Postgres execution evidence.

## Blockers

- No live Postgres execution evidence exists. `pg_isready` reports `/tmp:5432 - no response`, `docker` is not installed, and `TEST_DATABASE_URL`/`DATABASE_URL` are unset. The current implementation is a deterministic planning/SQL/receipt surface only; ticket closure requires either a reachable Postgres-backed commit implementation/test or an active superseding decision that narrows this child ticket to planning-only behavior.
