Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-cdf-cli-unused-parquet-dependency.md, .10x/tickets/done/2026-07-08-cdf-benchmarks-unused-arrow-csv-dependency.md
Verdict: pass

# Unused dependency cleanup review

## Target

Review of the dependency-hygiene cleanup that removes the unused `cdf-dest-parquet` direct dependency from `crates/cdf-cli/Cargo.toml` and the unused `arrow-csv` direct dependency from `crates/cdf-benchmarks/Cargo.toml`, with `Cargo.lock` refreshed.

## Findings

No blocking findings.

The source search and Cargo metadata checks support the removals: `cdf-cli` has no direct `cdf_dest_parquet` API use, and `cdf-benchmarks` has no direct `arrow_csv` API use. The remaining `arrow-csv` use belongs to `cdf-formats`, not to the benchmark crate.

The compile/test gates are proportionate for this change. Focused package checks, focused tests, clippy, full workspace check, formatting, diff check, full `cargo machete`, supply-chain gates, CodeQL, Semgrep, Gitleaks, direct unsafe scan, Jscpd, rust-code-analysis, scc, and Geiger all completed with no new blocking result.

## Residual risk

The existing project-level residuals remain unchanged: OSV and cargo-audit still surface only the ratified `paste` advisory, and cargo-deny still warns about the ratified duplicate Arrow 58/59 tuple. This cleanup does not address CLI command-module complexity; focused metrics still show existing hotspots in `state_command.rs` and `inspect_run_command.rs`, not new code from this slice.

## Verdict

Pass. The two direct dependencies were unused in their owning crates, the lockfile refresh is consistent with the manifest changes, and the quality gates support closing both tickets.
