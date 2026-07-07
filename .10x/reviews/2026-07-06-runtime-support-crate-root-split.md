Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-split-runtime-support-crate-roots.md
Verdict: pass

# Runtime Support Crate Root Split Review

## Target

Review of the organization-only split for:

- `crates/cdf-state-sqlite/src/lib.rs`
- `crates/cdf-http/src/lib.rs`

Supporting evidence: `.10x/evidence/2026-07-06-runtime-support-crate-root-split.md`.

## Assumptions tested

- The split should be file organization only, without semantic rewrites or public API renames.
- Public APIs formerly available from each crate root should remain available through crate-root `pub use` re-exports.
- Tests moved out of crate roots should keep their behavior and assertions.
- Any new helper visibility should remain internal to the crate.

## Findings

No blocking findings.

The final parent recheck cleared the earlier workspace-local verification limits. `cargo fmt --all -- --check`, `cargo check --workspace --all-targets --locked`, targeted locked tests, and targeted locked clippy all passed in the live workspace.

The new helper functions used to create corrupt SQLite/in-memory states remain test-only or crate-internal and do not widen the public API.

## Verdict

Pass. The runtime support crate roots are now compact module indexes, public crate-root surfaces remain re-exported, and the scoped checks plus workspace compile support closure.

## Residual risk

No known scoped residual blocker remains. Public API preservation is not backed by a formal API-diff artifact for these two crates, so the evidence relies on root re-exports and compile/test/clippy coverage.
