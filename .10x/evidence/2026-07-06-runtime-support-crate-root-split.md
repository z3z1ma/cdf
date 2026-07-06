Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-split-runtime-support-crate-roots.md

# Runtime support crate root split evidence

## What was observed

`crates/firn-state-sqlite/src/lib.rs` and `crates/firn-http/src/lib.rs` were split into focused internal modules while preserving crate-root public re-exports.

Before line counts:

- `crates/firn-state-sqlite/src/lib.rs`: 1974 lines.
- `crates/firn-http/src/lib.rs`: 1409 lines.

After line counts:

- `crates/firn-state-sqlite/src/lib.rs`: 11 lines.
- `crates/firn-http/src/lib.rs`: 31 lines.

Touched module list:

- `crates/firn-state-sqlite/src/lib.rs`
- `crates/firn-state-sqlite/src/in_memory.rs`
- `crates/firn-state-sqlite/src/sqlite.rs`
- `crates/firn-state-sqlite/src/support.rs`
- `crates/firn-state-sqlite/src/tests.rs`
- `crates/firn-http/src/lib.rs`
- `crates/firn-http/src/auth.rs`
- `crates/firn-http/src/egress.rs`
- `crates/firn-http/src/message.rs`
- `crates/firn-http/src/pagination.rs`
- `crates/firn-http/src/rate_limit.rs`
- `crates/firn-http/src/redaction.rs`
- `crates/firn-http/src/retry.rs`
- `crates/firn-http/src/support.rs`
- `crates/firn-http/src/tests.rs`
- `crates/firn-http/src/trace.rs`

Public API preservation notes:

- `firn-state-sqlite` crate root re-exports `InMemoryCheckpointStore` and `SqliteCheckpointStore`.
- `firn-http` crate root re-exports the same public request/response, pagination, rate-limit, retry, auth, egress, redaction, and trace symbols from focused modules.
- Test-only helpers were added for private SQLite/in-memory corruption setup instead of widening public APIs.

## Procedure

Scoped format check:

```text
rustfmt --edition 2024 --check crates/firn-state-sqlite/src/*.rs crates/firn-http/src/*.rs
```

Result: exit 0.

Official workspace format command:

```text
cargo fmt --all -- --check
```

Result: exit 1. Final rerun no longer reported diffs in the touched scoped files. It failed on out-of-scope workspace state: missing `crates/firn-cli/src/commands.rs` and formatting diffs in `crates/firn-python/src/dlt.rs`.

Official targeted tests:

```text
cargo test -p firn-state-sqlite -p firn-http --locked --no-fail-fast
```

Result: exit 101 before compilation. Cargo refused to update `Cargo.lock` because `--locked` was passed.

Official targeted clippy:

```text
cargo clippy -p firn-state-sqlite -p firn-http --all-targets --locked -- -D warnings
```

Result: exit 101 before compilation. Cargo refused to update `Cargo.lock` because `--locked` was passed.

Supplemental temp-copy test, run outside the workspace at `/tmp/firn-runtime-support-verify.mJiy7L` so lockfile churn stayed out of the repo:

```text
cargo test -p firn-state-sqlite -p firn-http --offline --no-fail-fast
```

Result: exit 0. `firn-http` ran 6 tests, all passed. `firn-state-sqlite` ran 14 tests, all passed. Both doc-test suites ran 0 tests and passed.

Supplemental temp-copy clippy:

```text
cargo clippy -p firn-state-sqlite -p firn-http --all-targets --offline -- -D warnings
```

Result: exit 0.

## What this supports or challenges

This supports that the scoped module split preserves behavior and compiles cleanly when Cargo is allowed to refresh the temporary lockfile outside the project workspace.

The earlier workspace-local blocker was cleared by later integration work. Parent reran the exact project-workspace checks after `Cargo.lock`, CLI, Python, and parallel split changes settled:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test -p firn-state-sqlite -p firn-http --locked --no-fail-fast
cargo clippy -p firn-state-sqlite -p firn-http --all-targets --locked -- -D warnings
```

All four commands passed in the live workspace. The targeted test run covered 20 unit tests across the two packages plus 0 doctests. The workspace check also compiled the downstream crates that consume these public crate-root re-exports.

## Limits

No formal public API diff was produced for these two crates. Public API preservation is supported by crate-root `pub use` re-exports, targeted locked tests, targeted locked clippy, and successful workspace compile.
