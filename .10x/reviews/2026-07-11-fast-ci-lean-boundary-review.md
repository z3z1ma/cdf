Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .github/workflows/fast-quality.yml, .github/workflows/slow-quality.yml, .10x/decisions/fast-ci-budget-and-deep-gate-separation.md
Verdict: pass

# Fast CI lean-boundary review

## Findings

- Redundant compilation: resolved. `cargo check --workspace --all-targets` plus workspace all-target Clippy was replaced by one bounded core-library Clippy graph.
- External setup: resolved. Fast CI no longer starts Postgres or installs Node, Go, cargo-audit, cargo-deny, or jscpd.
- Gitleaks false positives: resolved. The scanner consumes an exact tracked tree, so generated/vendor build output cannot enter by path convention or future directory drift.
- Scanner supply chain: pass. The small prebuilt binary is version- and checksum-pinned; scanner removal was rejected.
- Coverage transfer: pass. Generated artifacts moved to slow CI; slow already retains integration/conformance, duplication, supply chain, coverage, benchmark, Semgrep, and CodeQL gates.
- Significant residual: accepted by decision. Fast CI no longer compiles product/destination crates. This is deliberate smoke/deep separation; local change-set verification and scheduled/release gates retain the broader evidence.

## Verdict

Pass. The fast path is small, nonredundant, and mechanically budgeted. Do not accrete deep checks back into it without measured evidence and an update to the active decision.
