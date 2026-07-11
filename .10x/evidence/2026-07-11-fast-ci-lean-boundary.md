Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md, .10x/decisions/fast-ci-budget-and-deep-gate-separation.md

# Fast CI lean-boundary evidence

## What was observed

The prior fast workflow started Postgres, Node, and Go; source-built cargo-audit, cargo-deny, and Gitleaks; compiled the workspace with both `cargo check --all-targets` and `cargo clippy --all-targets`; ran conformance; rebuilt CLI generators twice; ran duplication and supply-chain scanners; and uploaded reports. Hosted run `29143346092` exceeded thirty minutes. Its only failure was twelve false positives under generated `target/` DuckDB/object-store files, proving the source-only scanner boundary was not structural.

The replacement has two independent jobs. Core Rust smoke performs metadata, formatting, one five-library Clippy graph, and four-library tests. Tracked-source secrets downloads Gitleaks 8.18.4, verifies its published Linux x64 SHA-256, and scans only `git archive HEAD`. There is no service container, Node/Go setup, source-built tool installation, redundant workspace compile, or deep/release gate.

Generated CLI/reference freshness was transferred to scheduled/manual slow CI, where conformance, duplication, supply chain, coverage, benchmarks, and CodeQL already live.

## Procedure and results

- Both modified workflows parsed as YAML.
- `cargo metadata --locked --no-deps` passed locally in 0.2 seconds.
- `cargo fmt --all -- --check` passed locally in 2.2 seconds.
- The exact core Clippy command passed locally in 21.3 seconds on a warm dependency cache.
- The exact core library test command passed: 69 contract, 35 formats, 22 kernel, and 34 package tests; total local wall time 89 seconds.
- `git archive` source extraction excluded `target/`; locally installed Gitleaks scanned it in 453 ms with no leaks.
- The pinned release checksum was verified against the official Gitleaks v8.18.4 checksum asset.

## What this supports

This supports the active fast-CI budget decision and that the prior gitleaks failure is fixed without an allowlist or weakened rule. It also supports that comprehensive checks were transferred, not deleted from the project quality system.

## Limits

The replacement hosted duration is intentionally not awaited in this tranche. The workflow's ten- and five-minute timeouts make the budget executable on its next ordinary run.
