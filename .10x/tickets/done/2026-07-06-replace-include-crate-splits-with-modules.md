Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/knowledge/rust-crate-organization.md

# Replace include-based crate splits with modules

## Scope

Replace the `include!`-based crate-root splits in the current project/Python/DuckDB/Postgres batch with ordinary Rust module declarations while preserving the current public crate-root API and behavior. Owns only:

- `crates/firn-project/src/**`
- `crates/firn-python/src/**`
- `crates/firn-dest-duckdb/src/**`
- `crates/firn-dest-postgres/src/**`

## Acceptance criteria

- The four crate roots use normal `mod` declarations rather than `include!`.
- Public items previously available at each crate root remain available at the crate root.
- `cargo fmt --all -- --check` passes.
- `cargo test -p firn-project -p firn-python -p firn-dest-duckdb -p firn-dest-postgres --locked --no-fail-fast` passes.
- `cargo clippy -p firn-project -p firn-python -p firn-dest-duckdb -p firn-dest-postgres --all-targets --locked -- -D warnings` passes.
- CodeQL database creation no longer reports `include` macro expansion failures for these four crate roots.

## Evidence expectations

Record focused fmt/test/clippy output and CodeQL extraction-log evidence.

## Explicit exclusions

No behavioral changes, no dependency changes, no `.gitignore` changes, no edits to older monolithic crate roots already owned by `.10x/tickets/2026-07-06-split-existing-rust-crate-roots.md`.

## Progress and notes

- 2026-07-06: Opened after CodeQL reported `macro expansion failed for 'include'` on the four newly split crate roots. The user specifically asked to avoid monolithic `lib.rs` files; ordinary modules better satisfy that request and reduce extractor friction.
- 2026-07-06: Replaced the four crate-root include maps with ordinary `mod` declarations, preserved crate-root public exports with `pub use`, adjusted only the sibling-module imports and `pub(crate)` helper visibility required by the new module boundaries, and unwrapped nested moved test modules to satisfy clippy. Evidence recorded in `.10x/evidence/2026-07-06-replace-include-crate-splits-with-modules.md`; fmt, targeted tests, targeted clippy, and CodeQL temp-source database creation all passed, with no remaining `include` macro expansion failure for the scoped roots.
- 2026-07-06: Parent revalidated with `cargo check --workspace --all-targets --all-features --locked`, `cargo check --workspace --all-targets --no-default-features --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --all-targets --locked --no-fail-fast`, final reusable CodeQL database refresh, and final CodeQL analysis. Closure review recorded in `.10x/reviews/2026-07-06-replace-include-crate-splits-with-modules-review.md`; consolidated quality evidence recorded in `.10x/evidence/2026-07-06-project-python-destinations-quality-gates.md`.

## Blockers

None.
