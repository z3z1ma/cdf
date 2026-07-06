Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-split-core-contract-crate-roots.md

# Core Contract Package Crate Root Split Evidence

## What was observed

The `firn-kernel`, `firn-contract`, and `firn-package` crate roots were split from monolithic `src/lib.rs` files into focused internal modules while keeping compact crate-root indexes with ordinary `mod` declarations and crate-root `pub use` re-exports.

An integration semver check later found that `crates/firn-contract/src/schema.rs` had lost the public derives on `ObservedSchema` during the split. The exact original derive set was restored: `Clone`, `Debug`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`.

Before line counts:

- `crates/firn-kernel/src/lib.rs`: 1387
- `crates/firn-contract/src/lib.rs`: 1549
- `crates/firn-package/src/lib.rs`: 1329

After line counts:

- `crates/firn-kernel/src/lib.rs`: 28
- `crates/firn-contract/src/lib.rs`: 19
- `crates/firn-package/src/lib.rs`: 17

Touched scoped module files:

- `crates/firn-kernel/src/async_types.rs`
- `crates/firn-kernel/src/batch.rs`
- `crates/firn-kernel/src/checkpoint.rs`
- `crates/firn-kernel/src/contract.rs`
- `crates/firn-kernel/src/destination.rs`
- `crates/firn-kernel/src/error.rs`
- `crates/firn-kernel/src/ids.rs`
- `crates/firn-kernel/src/lib.rs`
- `crates/firn-kernel/src/metadata.rs`
- `crates/firn-kernel/src/position.rs`
- `crates/firn-kernel/src/resource.rs`
- `crates/firn-kernel/src/scope.rs`
- `crates/firn-kernel/src/tests.rs`
- `crates/firn-contract/src/compiler.rs`
- `crates/firn-contract/src/lattice.rs`
- `crates/firn-contract/src/lib.rs`
- `crates/firn-contract/src/normalization.rs`
- `crates/firn-contract/src/policy.rs`
- `crates/firn-contract/src/program.rs`
- `crates/firn-contract/src/schema.rs`
- `crates/firn-contract/src/tests.rs`
- `crates/firn-contract/src/transforms.rs`
- `crates/firn-package/src/builder.rs`
- `crates/firn-package/src/json.rs`
- `crates/firn-package/src/lib.rs`
- `crates/firn-package/src/model.rs`
- `crates/firn-package/src/ops.rs`
- `crates/firn-package/src/reader.rs`
- `crates/firn-package/src/storage.rs`
- `crates/firn-package/src/tests.rs`

## Procedure

1. Inspected `.10x/tickets/done/2026-07-06-split-core-contract-crate-roots.md` and `.10x/knowledge/rust-crate-organization.md`.
2. Recorded before line counts with `wc -l crates/firn-kernel/src/lib.rs crates/firn-contract/src/lib.rs crates/firn-package/src/lib.rs`.
3. Split scoped crate roots into internal modules and replaced each `lib.rs` with a module index and re-exports.
4. Ran `cargo fmt -p firn-kernel -p firn-contract -p firn-package`, then `cargo fmt -p firn-kernel -p firn-contract -p firn-package -- --check`; both passed.
5. Initial closure ran `cargo fmt --all -- --check`; it failed on out-of-scope workspace files. The observed failure reported missing `crates/firn-cli/src/commands.rs` and formatting diffs in `crates/firn-python/src/bridge.rs` and `crates/firn-python/src/dlt.rs`. Those paths are outside this ticket's write scope.
6. Ran `cargo test -p firn-kernel -p firn-contract -p firn-package --locked --no-fail-fast`; it passed:
   - `firn-contract`: 10 tests passed.
   - `firn-kernel`: 8 tests passed.
   - `firn-package`: 7 tests passed.
   - Doc tests for all three crates: 0 tests, all passed.
7. Ran `cargo clippy -p firn-kernel -p firn-contract -p firn-package --all-targets --locked -- -D warnings`; it passed.
8. Restored `#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]` on `ObservedSchema` in `crates/firn-contract/src/schema.rs` after the integration semver check identified the regression.
9. Reran `cargo fmt --all -- --check`; it passed.
10. Ran `cargo check --workspace --all-targets --locked`; it passed, including `firn-contract`, `firn-formats`, `firn-engine`, `firn-project`, `firn-python`, `firn-subprocess`, and `firn-cli`.
11. Ran `cargo test -p firn-contract --locked --no-fail-fast`; it passed:
    - `firn-contract`: 10 tests passed.
    - Doc tests for `firn-contract`: 0 tests, passed.
12. Ran `cargo clippy -p firn-contract --all-targets --locked -- -D warnings`; it passed.

## What this supports or challenges

This supports the scoped acceptance criteria: the three crate roots are compact indexes, public crate-root surfaces remain re-exported, no dependency files were changed, targeted tests pass, and targeted clippy passes. The semver repair restores the public trait implementations that `ObservedSchema` had before the split.

The earlier workspace-wide fmt attempt showed an out-of-scope workspace limit. A later rerun after the semver repair passed for the current tree.

## Limits

This evidence does not include a formal public API diff. Public API preservation is supported by crate-root `pub use` re-exports, the semver derive repair for `ObservedSchema`, successful workspace check, and successful targeted tests and clippy for the scoped crates.
