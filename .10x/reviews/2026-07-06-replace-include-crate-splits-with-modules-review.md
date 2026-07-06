Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md
Verdict: pass

# Replace include crate splits review

## Target

Review of `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`, which replaced `include!`-based splits in `firn-project`, `firn-python`, `firn-dest-duckdb`, and `firn-dest-postgres` with ordinary Rust modules.

## Assumptions tested

- Public crate-root APIs stayed available through `pub use` re-exports.
- The change was organization-only and did not alter behavior, dependencies, or unrelated crate roots.
- CodeQL's specific `include` macro expansion failure was removed, even if broader local extractor macro warnings remain.

## Findings

None.

## Evidence checked

- `.10x/evidence/2026-07-06-replace-include-crate-splits-with-modules.md`
- `rg -n "include!" crates/firn-project/src crates/firn-python/src crates/firn-dest-duckdb/src crates/firn-dest-postgres/src -S`: no matches.
- `cargo fmt --all -- --check`: passed.
- `cargo test -p firn-project -p firn-python -p firn-dest-duckdb -p firn-dest-postgres --locked --no-fail-fast`: passed.
- `cargo clippy -p firn-project -p firn-python -p firn-dest-duckdb -p firn-dest-postgres --all-targets --locked -- -D warnings`: passed.
- Final parent CodeQL run produced 0 SARIF findings and 0 active-batch `include` macro expansion failures.

## Verdict

Pass. The ticket acceptance criteria are met. Residual CodeQL extractor macro warnings are not specific to the removed `include!` structure and are tracked separately by the consolidated quality evidence and follow-up owner.

## Residual risk

The local CodeQL Rust extractor still reports generic macro expansion diagnostics for standard macros and third-party macros. Those diagnostics limit CodeQL's confidence, but they do not contradict the refactor ticket's acceptance criteria.
