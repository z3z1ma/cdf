Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md
Depends-On: .10x/knowledge/rust-crate-organization.md

# Split engine and authoring crate roots

## Scope

Refactor `crates/firn-engine/src/lib.rs`, `crates/firn-declarative/src/lib.rs`, `crates/firn-formats/src/lib.rs`, and `crates/firn-subprocess/src/lib.rs` into focused internal modules without changing public crate-root APIs or behavior.

## Acceptance criteria

- Each scoped crate has a compact `src/lib.rs` index with ordinary `mod` declarations.
- Public crate-root APIs remain available.
- No dependency changes, semantic rewrites, or public API renames.
- Targeted package tests and clippy pass for the scoped crates.

## Evidence expectations

Record before/after root line counts, touched module list, targeted test/clippy output, and any public API preservation notes.

## Explicit exclusions

No changes outside `crates/firn-engine/**`, `crates/firn-declarative/**`, `crates/firn-formats/**`, `crates/firn-subprocess/**`, and this ticket's own evidence/review records.

## Progress and notes

- 2026-07-06: Split from `.10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md` to keep organization refactors independently executable.
- 2026-07-06: Assigned to engine/authoring crate-root split worker. Worker owns scoped crates and its own evidence/review records only.
- 2026-07-06: Split `firn-engine`, `firn-declarative`, `firn-formats`, and `firn-subprocess` crate roots into focused internal modules with crate-root re-exports. Before/after root counts and module list recorded in `.10x/evidence/2026-07-06-engine-authoring-crate-root-split.md`.
- 2026-07-06: Integration recheck preserved `FormatRead: Clone + Debug` with manual implementations and passed scoped tests/clippy plus workspace compile after parallel Python blockers cleared.
- 2026-07-06: Closure review recorded in `.10x/reviews/2026-07-06-engine-authoring-crate-root-split.md` with pass verdict.

## Blockers

None.
