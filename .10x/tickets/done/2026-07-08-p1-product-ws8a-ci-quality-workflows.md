Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md
Depends-On: QUALITY.md, .10x/specs/versioning-lts-release-policy.md, .10x/knowledge/quality-gate-execution.md

# P1 product WS8A: CI quality workflows

## Scope

Add GitHub Actions workflows for CDF's fast and slow quality phases.

## Acceptance criteria

- Fast CI runs on pull requests and pushes and covers formatting, linting, focused tests, dependency metadata sanity, source-only secret scanning, duplication checks, and fast supply-chain gates.
- Slow CI runs on schedule and manual dispatch and covers the full `QUALITY.md` cadence that is practical in CI, including conformance, golden, supply-chain scanners, Semgrep, CodeQL, duplication, complexity, and benchmark smoke gates.
- CodeQL Rust analysis uses a reusable/cacheable database strategy equivalent to `.10x/knowledge/quality-gate-execution.md` and does not recreate the database when the fingerprint is valid.
- Generated quality reports, CodeQL databases, caches, and scanner outputs are not committed.
- Workflow commands use locked dependency resolution where applicable.

## Evidence expectations

Committed workflow files, local static validation where possible, dry-run or actionlint output when available, and recorded evidence explaining any CI-only checks that cannot be executed locally.

## Explicit exclusions

No release artifact publishing. No installer. No crates.io publication. No broad quality-tool policy changes outside the workflow and evidence.

## Blockers

None.

## Progress and notes

- 2026-07-08: Activated for implementation. Read governing quality policy, release policy, CodeQL reuse knowledge, DataFusion crates.io tripwire, parent ticket, existing CodeQL helper, `deny.toml`, and cargo-vet supply-chain metadata before editing.
- 2026-07-08: Added `.github/workflows/fast-quality.yml` and `.github/workflows/slow-quality.yml`. Fast quality covers PR/push formatting, linting, compile, focused tests, metadata sanity, source-only secret scanning, jscpd, and fast supply-chain gates. Slow quality covers scheduled/manual full practical cadence, including conformance/golden/property/chaos/live-run filters, benchmark smoke, Semgrep, CodeQL with reusable database cache, jscpd, Rust complexity, and slow supply-chain gates.
- 2026-07-08: Recorded validation evidence in `.10x/evidence/2026-07-08-p1-product-ws8a-ci-quality-workflows.md` and closure review in `.10x/reviews/2026-07-08-p1-product-ws8a-ci-quality-workflows-review.md`.
