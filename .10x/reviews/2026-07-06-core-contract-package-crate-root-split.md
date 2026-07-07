Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-split-core-contract-crate-roots.md
Verdict: pass

# Core Contract Package Crate Root Split Closure Review

## Target

Review of the organization-only split for:

- `crates/cdf-kernel/src/lib.rs`
- `crates/cdf-contract/src/lib.rs`
- `crates/cdf-package/src/lib.rs`

Evidence reviewed: `.10x/evidence/2026-07-06-core-contract-package-crate-root-split.md`.

## Findings

Significant scoped finding repaired: an integration semver check found that `ObservedSchema` in `crates/cdf-contract/src/schema.rs` no longer derived `Clone`, `Debug`, `PartialEq`, and `Eq` compared with the pre-split public surface. Inspection of `HEAD` showed the full original derive set was `Clone`, `Debug`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`; that exact attribute was restored.

The split keeps the scoped crate roots as compact module indexes and re-exports public module contents from the crate roots. Targeted locked tests and targeted locked clippy pass for `cdf-kernel`, `cdf-contract`, and `cdf-package`. No dependency files, workspace manifests, CLI, Python, destination crates, or parent tickets were edited as part of this scoped review.

Minor residual risk: public API preservation was not checked with a formal API-diff tool. The risk is mitigated by preserving crate-root `pub use` exports and by successful compilation, tests, and clippy for the scoped crates.

The earlier out-of-scope workspace fmt limit was rechecked after the semver repair. `cargo fmt --all -- --check` now passes for the current tree, and `cargo check --workspace --all-targets --locked` also passes.

## Verdict

Pass for the scoped core/contract/package crate-root split.

## Residual risk

No known scoped residual blocker remains. Public API preservation still relies on targeted semver feedback and compile/test/clippy evidence rather than a complete API-diff artifact.
