Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md
Depends-On: .10x/knowledge/rust-crate-organization.md

# Split core contract crate roots

## Scope

Refactor `crates/firn-kernel/src/lib.rs`, `crates/firn-contract/src/lib.rs`, and `crates/firn-package/src/lib.rs` into focused internal modules without changing public crate-root APIs or behavior.

## Acceptance criteria

- Each scoped crate has a compact `src/lib.rs` index with ordinary `mod` declarations.
- Public crate-root APIs remain available.
- No dependency changes, semantic rewrites, or public API renames.
- Targeted package tests and clippy pass for the scoped crates.

## Evidence expectations

Record before/after root line counts, touched module list, targeted test/clippy output, and any public API preservation notes.

## Explicit exclusions

No changes outside `crates/firn-kernel/**`, `crates/firn-contract/**`, `crates/firn-package/**`, and this ticket's own evidence/review records.

## Progress and notes

- 2026-07-06: Split from `.10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md` to keep organization refactors independently executable.
- 2026-07-06: Assigned to core crate-root split worker. Worker owns scoped crates and its own evidence/review records only.
- 2026-07-06: Split `firn-kernel`, `firn-contract`, and `firn-package` crate roots into focused internal modules with crate-root re-exports. Before/after root counts and module list recorded in `.10x/evidence/2026-07-06-core-contract-package-crate-root-split.md`.
- 2026-07-06: Verification for scoped criteria passed: `cargo fmt -p firn-kernel -p firn-contract -p firn-package -- --check`, `cargo test -p firn-kernel -p firn-contract -p firn-package --locked --no-fail-fast`, and `cargo clippy -p firn-kernel -p firn-contract -p firn-package --all-targets --locked -- -D warnings`.
- 2026-07-06: `cargo fmt --all -- --check` was attempted and failed on out-of-scope workspace paths, including `crates/firn-cli/src/commands.rs` resolution and formatting diffs in `crates/firn-python`; this ticket did not edit those excluded paths.
- 2026-07-06: Closure review recorded in `.10x/reviews/2026-07-06-core-contract-package-crate-root-split.md` with pass verdict for the scoped split.
- 2026-07-06: Repaired integration semver regression by restoring the exact pre-split derives on `ObservedSchema`: `Clone`, `Debug`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`. Fresh verification passed: `cargo fmt --all -- --check`, `cargo check --workspace --all-targets --locked`, `cargo test -p firn-contract --locked --no-fail-fast`, and `cargo clippy -p firn-contract --all-targets --locked -- -D warnings`.

## Blockers

None.
