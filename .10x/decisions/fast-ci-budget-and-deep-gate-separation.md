Status: superseded
Created: 2026-07-11
Updated: 2026-07-17

Superseded-By: `.10x/decisions/fast-ci-leaf-owner-gates.md`

# Fast CI budget and deep-gate separation

## Context

The first P1 fast workflow recreated a large part of the deep loop on every push: Postgres service startup, Rust/Node/Go toolchains, three source-built scanners, workspace all-target check, workspace all-target Clippy, conformance tests, two generated-reference builds, gitleaks over the post-build `target/` tree, duplication analysis, and supply-chain scans. It routinely spent more than thirty minutes and compiled the same Rust graph several times. A slow gate already owns comprehensive integration, conformance, scanners, supply chain, coverage, benchmarks, and CodeQL.

## Decision

Fast CI has a cold p95 budget of ten minutes and consists of two independent jobs:

1. **Core Rust smoke:** locked metadata parse, formatting, one Clippy compile for kernel/contract/package/formats/engine libraries, then library tests for kernel/contract/package/formats reusing that graph.
2. **Tracked-source secrets:** download the pinned Gitleaks 8.18.4 Linux binary, verify SHA-256 `ba6dbb656933921c775ee5a2d1c13a91046e7952e9d919f9bac4cec61d628e7d`, and scan a `git archive` of `HEAD` so build output cannot enter the source boundary.

Fast CI does not start Postgres; install Node or Go; compile CLI/destinations/conformance/benchmarks; run generated-reference checks, duplication, supply-chain, coverage, benchmarks, or CodeQL; or run both `cargo check` and Clippy over the same graph. Scheduled/manual slow CI and release workflows own those gates. Local change-set verification remains risk-driven under `QUALITY.md` and is not weakened by this CI budget.

## Alternatives considered

- Keeping all checks and adding caches was rejected because it preserves redundant compilation and external-tool setup as permanent latency.
- A path-filter/dependency-graph dispatcher was deferred because maintaining a second crate dependency model would itself be a correctness risk.
- Removing secret scanning was rejected. A pinned prebuilt scanner is faster than building Gitleaks through Go and preserves the trust-boundary check.
- Using generated-directory exclusions was rejected because future build outputs could leak elsewhere; scanning an exact tracked tree makes the boundary structural.

## Consequences

Push/PR feedback is intentionally a smoke gate, not release proof. Deep and release gates may take longer and remain required at their own cadence. If fast CI exceeds ten minutes cold on the hosted class or misses a recurrent defect class that a small nonredundant check would catch, reopen this decision with measured evidence rather than gradually rebuilding the deep loop in fast CI.
