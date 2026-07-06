Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/knowledge/rust-crate-organization.md, .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/done/2026-07-05-python-sdk-bridge.md, .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/2026-07-05-postgres-destination.md

# Rust crate organization refactor evidence

## What was observed

The large new crate roots for `firn-project`, `firn-python`, `firn-dest-duckdb`, and `firn-dest-postgres` were split into focused source files while preserving the existing public crate-root API.

`lib.rs` in each crate now acts as a compact index with crate documentation, shared imports/constants, ordinary `mod` declarations, and crate-root `pub use` exports where needed. Implementation sections moved into files such as `models.rs`, `secrets.rs`, `bridge.rs`, `interpreter.rs`, `api.rs`, `planning.rs`, `receipts.rs`, `ddl.rs`, and `dml.rs`. Tests moved into `tests.rs`.

The durable convention is recorded in `.10x/knowledge/rust-crate-organization.md`.

## Procedure

- Mechanically moved contiguous sections from four large `lib.rs` files into focused files under each crate's `src/` directory.
- Kept public item names and paths unchanged by re-exporting the focused modules from the crate root.
- Moved stranded split-boundary attributes back onto their owning items after the initial compile failure exposed the issue.
- Replaced the intermediate `include!` map with ordinary Rust modules after CodeQL reported `include` macro expansion failures against the four crate roots.

## Command results

- `cargo fmt --all`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo test -p firn-project -p firn-dest-postgres -p firn-dest-duckdb -p firn-python --locked --no-fail-fast`: passed with 46 unit tests and 0 doctests.
- `cargo clippy -p firn-project -p firn-dest-postgres -p firn-dest-duckdb -p firn-python --all-targets --locked -- -D warnings`: passed.
- `rg -n "include!" crates/firn-project/src crates/firn-python/src crates/firn-dest-duckdb/src crates/firn-dest-postgres/src -S`: no matches.
- `python3 -m compileall -q python/firn_sdk python/examples && uvx pyright python/firn_sdk python/examples`: passed with 0 errors.
- `git diff --check`: passed.

## What this supports or challenges

This supports the user's crate-organization preference without changing runtime behavior. The targeted package tests and clippy pass after the split, so the organization refactor did not break the currently implemented project, Python, DuckDB, or Postgres surfaces.

## Limits

This refactor split the newest large crate roots in the current batch. Older large roots such as `firn-engine`, `firn-declarative`, and `firn-formats` remain candidates for future organization-only refactors when their owning tickets are active or a dedicated refactor ticket is opened.
