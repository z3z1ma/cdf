Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/specs/conformance-governance-roadmap.md, .10x/specs/project-cli-observability-security.md, .10x/specs/run-orchestration-ledger.md, .10x/decisions/mvp-acceptance-demo-fixture-boundary.md, .10x/tickets/done/2026-07-05-cli-surface.md, .10x/tickets/done/2026-07-07-p0-workstream-c-spine-conformance-harness.md

# Implement MVP acceptance demo fixture harness

## Scope

Implement a conformance-owned MVP acceptance demo foundation harness that composes the already-built CLI/runtime mechanisms into one deterministic proof.

Owns:

- `crates/cdf-conformance/**` demo harness modules and tests.
- Any narrow `cdf-conformance` dev-dependency needed to invoke the CLI library surface.
- Test fixtures needed for a GitHub-Issues-shaped Tier-0 REST source, DuckDB destination(s), package replay, drift quarantine, and state-history assertions.

## Acceptance criteria

- The harness exercises a GitHub-Issues-shaped Tier-0 REST resource and records plan output before bytes move.
- The harness loads accepted issue rows into DuckDB and proves queryability through `cdf sql` or the same local system SQL surface the CLI exposes.
- The harness exercises `cdf contract freeze` and a drift-quarantine beat that proves quarantined rows are recorded while accepted rows continue to package, destination receipt verification, and checkpoint gating.
- The harness proves a crash between destination commit and checkpoint commit, then resumes or recovers without source contact.
- The harness replays the produced package into a second DuckDB database and proves duplicate replay/no-op behavior.
- The harness records state history for the relevant pipeline/resource/scope and asserts committed state is not ahead of durable data.
- Output captured as evidence is deterministic and redacts token-like source values.

## Evidence expectations

Run focused conformance demo tests, relevant CLI tests if the CLI library is used, `cargo fmt --all --check`, relevant clippy/check/test gates, `jscpd` and rust-code-analysis over touched code, and applicable security/supply-chain scans before closure.

## Explicit exclusions

No live GitHub network call, no credentialed GitHub setup, no scheduler, no resident loop, no new production crash-hook CLI flag, no broad CLI redesign, and no new destination/source semantics outside the demo harness.

## Blockers

None. The fixture/live-network boundary is ratified by `.10x/decisions/mvp-acceptance-demo-fixture-boundary.md`.

## Progress and notes

- 2026-07-08: Split from `.10x/tickets/2026-07-05-conformance-chaos-golden.md` after CLI surface closure. The parent remains open for broader conformance and full MVP acceptance after this foundation harness lands.
- 2026-07-08: Implemented `crates/cdf-conformance/src/mvp_acceptance_demo.rs`, wired it into `cdf-conformance`, added narrow `cdf-cli` and `duckdb` dev-dependencies, and reused the existing drift-quarantine helper through a crate-visible conformance module boundary.
- 2026-07-08: Closed with evidence `.10x/evidence/2026-07-08-mvp-acceptance-demo-fixture-harness.md` and review `.10x/reviews/2026-07-08-mvp-acceptance-demo-fixture-harness-review.md`. Focused conformance demo tests, `cdf-conformance`, `cdf-cli`, workspace tests, workspace clippy, formatting, jscpd, rust-code-analysis, Semgrep, source-only gitleaks, cargo deny/audit/vet, OSV, and reusable-DB CodeQL all passed or produced only ratified residuals.
