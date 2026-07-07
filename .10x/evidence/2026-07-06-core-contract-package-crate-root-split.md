Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-split-core-contract-crate-roots.md

# Core Contract Package Crate Root Split Evidence

## What was observed

The `cdf-kernel`, `cdf-contract`, and `cdf-package` crate roots were split from monolithic `src/lib.rs` files into focused internal modules while keeping compact crate-root indexes with ordinary `mod` declarations and crate-root `pub use` re-exports.

An integration semver check later found that `crates/cdf-contract/src/schema.rs` had lost the public derives on `ObservedSchema` during the split. The exact original derive set was restored: `Clone`, `Debug`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`.

Before line counts:

- `crates/cdf-kernel/src/lib.rs`: 1387
- `crates/cdf-contract/src/lib.rs`: 1549
- `crates/cdf-package/src/lib.rs`: 1329

After line counts:

- `crates/cdf-kernel/src/lib.rs`: 28
- `crates/cdf-contract/src/lib.rs`: 19
- `crates/cdf-package/src/lib.rs`: 17

Touched scoped module files:

- `crates/cdf-kernel/src/async_types.rs`
- `crates/cdf-kernel/src/batch.rs`
- `crates/cdf-kernel/src/checkpoint.rs`
- `crates/cdf-kernel/src/contract.rs`
- `crates/cdf-kernel/src/destination.rs`
- `crates/cdf-kernel/src/error.rs`
- `crates/cdf-kernel/src/ids.rs`
- `crates/cdf-kernel/src/lib.rs`
- `crates/cdf-kernel/src/metadata.rs`
- `crates/cdf-kernel/src/position.rs`
- `crates/cdf-kernel/src/resource.rs`
- `crates/cdf-kernel/src/scope.rs`
- `crates/cdf-kernel/src/tests.rs`
- `crates/cdf-contract/src/compiler.rs`
- `crates/cdf-contract/src/lattice.rs`
- `crates/cdf-contract/src/lib.rs`
- `crates/cdf-contract/src/normalization.rs`
- `crates/cdf-contract/src/policy.rs`
- `crates/cdf-contract/src/program.rs`
- `crates/cdf-contract/src/schema.rs`
- `crates/cdf-contract/src/tests.rs`
- `crates/cdf-contract/src/transforms.rs`
- `crates/cdf-package/src/builder.rs`
- `crates/cdf-package/src/json.rs`
- `crates/cdf-package/src/lib.rs`
- `crates/cdf-package/src/model.rs`
- `crates/cdf-package/src/ops.rs`
- `crates/cdf-package/src/reader.rs`
- `crates/cdf-package/src/storage.rs`
- `crates/cdf-package/src/tests.rs`

## Procedure

1. Inspected `.10x/tickets/done/2026-07-06-split-core-contract-crate-roots.md` and `.10x/knowledge/rust-crate-organization.md`.
2. Recorded before line counts with `wc -l crates/cdf-kernel/src/lib.rs crates/cdf-contract/src/lib.rs crates/cdf-package/src/lib.rs`.
3. Split scoped crate roots into internal modules and replaced each `lib.rs` with a module index and re-exports.
4. Ran `cargo fmt -p cdf-kernel -p cdf-contract -p cdf-package`, then `cargo fmt -p cdf-kernel -p cdf-contract -p cdf-package -- --check`; both passed.
5. Initial closure ran `cargo fmt --all -- --check`; it failed on out-of-scope workspace files. The observed failure reported missing `crates/cdf-cli/src/commands.rs` and formatting diffs in `crates/cdf-python/src/bridge.rs` and `crates/cdf-python/src/dlt.rs`. Those paths are outside this ticket's write scope.
6. Ran `cargo test -p cdf-kernel -p cdf-contract -p cdf-package --locked --no-fail-fast`; it passed:
   - `cdf-contract`: 10 tests passed.
   - `cdf-kernel`: 8 tests passed.
   - `cdf-package`: 7 tests passed.
   - Doc tests for all three crates: 0 tests, all passed.
7. Ran `cargo clippy -p cdf-kernel -p cdf-contract -p cdf-package --all-targets --locked -- -D warnings`; it passed.
8. Restored `#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]` on `ObservedSchema` in `crates/cdf-contract/src/schema.rs` after the integration semver check identified the regression.
9. Reran `cargo fmt --all -- --check`; it passed.
10. Ran `cargo check --workspace --all-targets --locked`; it passed, including `cdf-contract`, `cdf-formats`, `cdf-engine`, `cdf-project`, `cdf-python`, `cdf-subprocess`, and `cdf-cli`.
11. Ran `cargo test -p cdf-contract --locked --no-fail-fast`; it passed:
    - `cdf-contract`: 10 tests passed.
    - Doc tests for `cdf-contract`: 0 tests, passed.
12. Ran `cargo clippy -p cdf-contract --all-targets --locked -- -D warnings`; it passed.

## What this supports or challenges

This supports the scoped acceptance criteria: the three crate roots are compact indexes, public crate-root surfaces remain re-exported, no dependency files were changed, targeted tests pass, and targeted clippy passes. The semver repair restores the public trait implementations that `ObservedSchema` had before the split.

The earlier workspace-wide fmt attempt showed an out-of-scope workspace limit. A later rerun after the semver repair passed for the current tree.

## Limits

This evidence does not include a formal public API diff. Public API preservation is supported by crate-root `pub use` re-exports, the semver derive repair for `ObservedSchema`, successful workspace check, and successful targeted tests and clippy for the scoped crates.
