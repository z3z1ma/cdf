Status: done
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-package-builder-reader.md, .10x/tickets/done/2026-07-05-contract-compiler-normalization.md

# Implement DuckDB destination

## Scope

Implement `firn-dest-duckdb`: destination sheet, dry-run commit planning, DDL migration, append/replace/merge, Arrow appender/Parquet replay paths, idempotency tokens, `_firn_loads` and `_firn_state` mirrors, receipt verification, single-writer locking, and ICU doctor probe. Owns `crates/firn-dest-duckdb/**`.

## Acceptance criteria

- Destination sheet declares type mappings, identifier rules, dispositions, transactions, idempotency, bulk paths, migration support, and single-writer constraint.
- Append, atomic replace, and deterministic merge work against package segments.
- Replaying the same package returns duplicate/no-op behavior where package-token idempotency applies.
- Receipt verify query can confirm durable commit after process restart.
- ICU/timezone support is detectable.

## Evidence expectations

Record DuckDB integration tests for dispositions, DDL, idempotency, crash-recovery receipt verification, mirrors, and ICU probe.

## Explicit exclusions

No CLI-specific doctor UI; expose probes for project/CLI ticket.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Assigned to DuckDB destination worker. Worker owns `crates/firn-dest-duckdb/**` plus its own evidence/review records and this ticket. Do not touch `.gitignore`, parent ticket, other destination crates, or unrelated records.
- 2026-07-06: Implemented `firn-dest-duckdb` with real `duckdb` bundled-driver integration, destination sheet/capability report, dry-run package planning, DDL migration planning, append/replace/merge commits from `firn-package` Arrow IPC replay, deterministic exact-row merge dedup with conflicting duplicate-key rejection, package-token idempotency, `_firn_loads`/`_firn_state` mirrors, verifiable receipts, single-writer lockfile, and ICU probe API.
- 2026-07-06: Direct duckdb-rs Arrow appender is not used because the current DuckDB crate appender API is Arrow 58-facing while Firn packages use Arrow 59; this crate uses the real DuckDB row appender over decoded Firn Arrow IPC package batches. Parquet package replay is declared unsupported until package archive Parquet data exists under `.10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md`.
- 2026-07-06: Evidence recorded in `.10x/evidence/2026-07-06-duckdb-destination.md`; closure review recorded in `.10x/reviews/2026-07-06-duckdb-destination-review.md`. `cargo check -p firn-dest-duckdb`, `cargo fmt -p firn-dest-duckdb`, `cargo test -p firn-dest-duckdb --locked --no-fail-fast`, `cargo clippy -p firn-dest-duckdb --all-targets --locked -- -D warnings`, `git diff --check`, `cargo deny check advisories`, and `cargo audit` passed.
- 2026-07-06: Split the large `crates/firn-dest-duckdb/src/lib.rs` into focused files under `crates/firn-dest-duckdb/src/` while preserving the crate-root API. Organization evidence recorded in `.10x/evidence/2026-07-06-rust-crate-organization-refactor.md`.
- 2026-07-06: Replaced the intermediate `include!` split with ordinary Rust modules under `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`. Final parent quality gates recorded in `.10x/evidence/2026-07-06-project-python-destinations-quality-gates.md`.

## Blockers

None.
