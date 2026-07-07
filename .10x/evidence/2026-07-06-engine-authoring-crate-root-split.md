Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-split-engine-authoring-crate-roots.md, .10x/knowledge/rust-crate-organization.md

# Engine and Authoring Crate Root Split Evidence

## What was observed

The scoped crate roots were split into focused internal modules using ordinary `mod` declarations and crate-root `pub use` re-exports. Public crate-root APIs remain exposed through the roots.

Before root line counts from `HEAD`:

```text
crates/cdf-engine/src/lib.rs 1314
crates/cdf-declarative/src/lib.rs 1436
crates/cdf-formats/src/lib.rs 646
crates/cdf-subprocess/src/lib.rs 367
```

After root line counts in the working tree:

```text
14 crates/cdf-engine/src/lib.rs
12 crates/cdf-declarative/src/lib.rs
14 crates/cdf-formats/src/lib.rs
12 crates/cdf-subprocess/src/lib.rs
52 total
```

Touched scoped module inventory:

```text
crates/cdf-declarative/src/compiled.rs
crates/cdf-declarative/src/declarations.rs
crates/cdf-declarative/src/lib.rs
crates/cdf-declarative/src/tests.rs
crates/cdf-engine/src/execution.rs
crates/cdf-engine/src/lib.rs
crates/cdf-engine/src/planning.rs
crates/cdf-engine/src/predicates.rs
crates/cdf-engine/src/tests.rs
crates/cdf-engine/src/types.rs
crates/cdf-formats/src/lib.rs
crates/cdf-formats/src/readers.rs
crates/cdf-formats/src/schema.rs
crates/cdf-formats/src/tests.rs
crates/cdf-formats/src/types.rs
crates/cdf-subprocess/src/command.rs
crates/cdf-subprocess/src/lib.rs
crates/cdf-subprocess/src/runner.rs
crates/cdf-subprocess/src/tests.rs
```

## Procedure

A clean verification overlay was created at `/tmp/cdf-engine-authoring-verify.f4uTet` from `HEAD`, then only these scoped source trees were copied into it:

```text
crates/cdf-engine/src/
crates/cdf-declarative/src/
crates/cdf-formats/src/
crates/cdf-subprocess/src/
```

The requested verification commands were run in that clean overlay.

```text
cargo fmt --all -- --check
```

Result: passed with no formatter output.

```text
cargo test -p cdf-engine -p cdf-declarative -p cdf-formats -p cdf-subprocess --locked --no-fail-fast
```

Result: passed.

Observed test counts:

```text
cdf-declarative: 7 passed, 0 failed
cdf-engine: 5 passed, 0 failed
cdf-formats: 6 passed, 0 failed
cdf-subprocess: 5 passed, 0 failed
doc tests for all four crates: 0 passed, 0 failed
```

```text
cargo clippy -p cdf-engine -p cdf-declarative -p cdf-formats -p cdf-subprocess --all-targets --locked -- -D warnings
```

Result: passed.

Observed terminal output:

```text
Finished `dev` profile [unoptimized + debuginfo] target(s) in 32.54s
```

## Public API preservation notes

The roots preserve public access with re-exports:

- `cdf-engine` re-exports `execute_to_package`, `DATAFUSION_TABLE_PROVIDER_KIND`, `Planner`, `datafusion_filter_pushdown`, `negotiate_scan_plan`, and `types::*`.
- `cdf-declarative` re-exports compiled resource types/functions and all declaration types/functions via `declarations::*`.
- `cdf-formats` re-exports reader entry points, schema helpers/constants, and `types::*`.
- `cdf-subprocess` re-exports command/output/supervision types and `run_stdout_adapter`.

No dependency files or public API names were intentionally changed by this ticket.

## What this supports or challenges

This supports the ticket acceptance criteria that each scoped crate now has a compact `src/lib.rs` index, public crate-root APIs remain available, no dependency changes were required, and targeted formatter/test/clippy checks pass for the scoped crates in a clean overlay.

## Limits

The live working tree has unrelated dirty files outside this ticket's write boundary, including `.gitignore`, `Cargo.lock`, and other `.10x` records. Earlier live-worktree `--locked` verification stopped before compilation because the dirty workspace metadata wanted a lockfile update; live-worktree `cargo fmt --all -- --check` also encountered unrelated out-of-scope broken modules. The passing verification above isolates this ticket by applying only the scoped source changes to a clean `HEAD` overlay.

## Integration recheck

An integration recheck after this record was first written found a scoped compile failure in `crates/cdf-formats/src/types.rs`: `FormatRead` derived `Clone` and `Debug`, but the current `cdf-contract::ObservedSchema` did not implement those traits. The scoped fix keeps the public `FormatRead: Clone + Debug` surface by replacing the derive with manual implementations that clone/debug the public `ObservedSchema.fields` data, without touching `cdf-contract`.

Additional live-worktree verification after the fix:

```text
cargo fmt --all -- --check
```

Result: passed with no formatter output.

```text
cargo check --workspace --all-targets --locked
```

Result: initially failed outside this ticket's owned scope in `crates/cdf-python/src/bridge.rs`. That parallel dlt/Python blocker was repaired before parent integration closure.

```text
cargo test -p cdf-engine -p cdf-declarative -p cdf-formats -p cdf-subprocess --locked --no-fail-fast
```

Result: passed.

Observed test counts:

```text
cdf-declarative: 7 passed, 0 failed
cdf-engine: 5 passed, 0 failed
cdf-formats: 6 passed, 0 failed
cdf-subprocess: 5 passed, 0 failed
doc tests for all four crates: 0 passed, 0 failed
```

```text
cargo clippy -p cdf-engine -p cdf-declarative -p cdf-formats -p cdf-subprocess --all-targets --locked -- -D warnings
```

Result: passed.

Observed terminal output:

```text
Finished `dev` profile [unoptimized + debuginfo] target(s) in 16.24s
```

Final parent integration recheck after the Python blocker cleared:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test -p cdf-engine -p cdf-declarative -p cdf-formats -p cdf-subprocess --locked --no-fail-fast
cargo clippy -p cdf-engine -p cdf-declarative -p cdf-formats -p cdf-subprocess --all-targets --locked -- -D warnings
```

All commands passed in the live workspace. The targeted test run covered 23 unit tests across the four packages plus 0 doctests.
