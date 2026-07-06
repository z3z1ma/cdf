Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/knowledge/rust-crate-organization.md

# Split existing monolithic Rust crate roots

## Scope

Refactor existing large `src/lib.rs` roots into focused files or modules without changing public APIs or behavior. Initial candidates observed on 2026-07-06 are `firn-state-sqlite`, `firn-contract`, `firn-declarative`, `firn-http`, `firn-kernel`, `firn-package`, `firn-engine`, and `firn-formats`.

## Acceptance criteria

- Each affected crate has a compact `src/lib.rs` that acts as an index/API surface rather than a monolithic implementation file.
- Public crate-root APIs remain stable unless an owning implementation ticket explicitly changes them.
- Existing tests, formatting, clippy, and applicable quality gates continue to pass.
- The split follows `.10x/knowledge/rust-crate-organization.md`.

## Evidence expectations

Record file-organization before/after counts and targeted tests for every crate touched. Run workspace formatting and at least targeted package tests/clippy for touched crates; broader quality gates are required before committing a batch that includes these changes.

## Explicit exclusions

No semantic rewrites, dependency changes, behavior changes, or public API renames. This is an organization-only refactor.

## Progress and notes

- 2026-07-06: Opened after the user requested avoiding monolithic `lib.rs` files across crates. Current active-batch crates `firn-project`, `firn-python`, `firn-dest-duckdb`, and `firn-dest-postgres` were already split under `.10x/evidence/2026-07-06-rust-crate-organization-refactor.md`; this ticket owns the older large roots.

## Blockers

None.
