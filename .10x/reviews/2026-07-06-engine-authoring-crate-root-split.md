Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-split-engine-authoring-crate-roots.md
Verdict: pass

# Engine and Authoring Crate Root Split Review

## Target

Review of the organization-only split for:

- `crates/cdf-engine/src/lib.rs`
- `crates/cdf-declarative/src/lib.rs`
- `crates/cdf-formats/src/lib.rs`
- `crates/cdf-subprocess/src/lib.rs`

Supporting evidence: `.10x/evidence/2026-07-06-engine-authoring-crate-root-split.md`.

## Assumptions tested

- The change should be file organization only, without semantic rewrites or public API renames.
- Public APIs formerly available from each crate root should remain available through root `pub use` re-exports.
- Tests moved out of crate roots should retain their original fixture content and assertions.
- Internal visibility changes should be limited to what module boundaries require.

## Findings

No blocking findings.

Minor issue found during execution and resolved before review: moving declarative tests out of `lib.rs` mechanically deindented a YAML raw string fixture. The fixture indentation was restored, and the clean-overlay targeted test run then passed.

Internal visibility changes are scoped: `cdf-engine::planning::validate_program` was changed from private to `pub(crate)` so `execution` can reuse the same helper after the split. This does not widen the public crate API.

Integration recheck finding resolved: `FormatRead` could no longer derive `Clone` and `Debug` against the current `ObservedSchema` definition. The fix stays inside `cdf-formats` and preserves the public `FormatRead: Clone + Debug` surface with manual implementations.

## Verdict

Pass. The module split satisfies the ticket acceptance criteria under clean-overlay and scoped live-worktree verification, and no semantic drift was identified in the scoped files.

## Residual risk

The earlier out-of-scope `cdf-python/src/bridge.rs` lifetime error was repaired by the owning dlt/Python work before parent integration closure. No known scoped residual blocker remains. Public API preservation still relies on crate-root re-exports plus compile/test/clippy evidence rather than a formal API-diff artifact for these four crates.
