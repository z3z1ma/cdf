Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md, .10x/knowledge/rust-crate-organization.md

# Replace include crate splits with module evidence

## What was observed

The four scoped crate roots no longer use `include!` to assemble split source files:

- `crates/firn-project/src/lib.rs`
- `crates/firn-python/src/lib.rs`
- `crates/firn-dest-duckdb/src/lib.rs`
- `crates/firn-dest-postgres/src/lib.rs`

Each root now declares ordinary Rust modules with `mod`, and re-exports the prior crate-root public API with `pub use` where needed. Split implementation files use minimal `crate` imports and `pub(crate)` visibility for helpers that are shared across sibling modules.

`rg -n "include!" crates/firn-project/src crates/firn-python/src crates/firn-dest-duckdb/src crates/firn-dest-postgres/src -S` produced no matches.

## Procedure

- Replaced the crate-root include maps with normal `mod` declarations.
- Preserved root public exports with `pub use` from the owning modules.
- Added sibling-module imports and crate-private helper visibility only where the module boundary required it.
- Unwrapped the moved test files from nested `mod tests` blocks so `#[cfg(test)] mod tests;` in each crate root does not trigger `clippy::module_inception`.

## Command results

- `cargo fmt --all -- --check`: passed.
- `cargo test -p firn-project -p firn-python -p firn-dest-duckdb -p firn-dest-postgres --locked --no-fail-fast`: passed with 46 unit tests and 0 doctests.
- `cargo clippy -p firn-project -p firn-python -p firn-dest-duckdb -p firn-dest-postgres --all-targets --locked -- -D warnings`: passed.

CodeQL verification used a temporary source copy excluding `.git`, `target`, and `reports`:

```text
codeql database create /tmp/firn-codeql-db-include-modules --language=rust --source-root /tmp/firn-codeql-src-include-modules --overwrite --command "env CARGO_TARGET_DIR=/tmp/firn-codeql-target-include-modules cargo check -p firn-project -p firn-python -p firn-dest-duckdb -p firn-dest-postgres --locked"
```

The database command completed successfully:

```text
Successfully created database at /tmp/firn-codeql-db-include-modules.
```

`rg -n "macro expansion failed for 'include'|include!" /tmp/firn-codeql-db-include-modules/log /tmp/firn-codeql-src-include-modules/crates/firn-project/src /tmp/firn-codeql-src-include-modules/crates/firn-python/src /tmp/firn-codeql-src-include-modules/crates/firn-dest-duckdb/src /tmp/firn-codeql-src-include-modules/crates/firn-dest-postgres/src -S` produced no output.

## What this supports or challenges

This supports the ticket acceptance criteria: the scoped crate roots use normal Rust modules, the public root API remains exported, cargo fmt/test/clippy pass for the four packages, and CodeQL no longer reports `include` macro expansion failures for those crate roots.

## Limits

CodeQL still emitted unrelated extractor warnings for other macro expansions such as standard assertion/vector/format-style macros and cargo metadata warnings involving `--lockfile-path`. This evidence only supports the removal of the `include` macro expansion failure for the scoped crate roots.
