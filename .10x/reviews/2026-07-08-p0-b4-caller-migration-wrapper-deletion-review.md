Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-p0-b4-caller-migration-wrapper-deletion.md
Verdict: pass

# P0 B4 Caller Migration and Wrapper Deletion Review

## Target

Ticket `.10x/tickets/done/2026-07-07-p0-b4-caller-migration-wrapper-deletion.md`, evidence `.10x/evidence/2026-07-08-p0-b4-caller-migration-wrapper-deletion.md`, and B4 source changes in `crates/cdf-project`, `crates/cdf-cli`, and `crates/cdf-conformance`.

## Findings

- Minor, accepted: `cargo semver-checks` reports public API removals and `RuntimeStage` enum changes. These are the intended B4 break: the removed API was a temporary destination-specialized wrapper surface and the repo is still pre-1.0.
- Minor, accepted: `jscpd` reports 6.819% duplicated lines across the broad B4 runtime/CLI/conformance scope. The implementation deleted 1,364 lines and collapsed the wrapper family; remaining duplication is mostly tests/harnesses and destination-driver helper shape, not a new B4 blocker.
- Minor, accepted: `replay_package_with_runtime` remains the hottest runtime function at cyclomatic 19. It is now the single generic replay spine rather than one of several destination-specialized branches. Further splitting is a maintainability opportunity, not a B4 closure blocker.
- Residual, owned by Workstream C: conformance callers route through the generic path, but the full run-spine matrix, non-DuckDB chaos breadth, per-destination live-run goldens, and property/fuzz targets remain open under `.10x/tickets/2026-07-07-p0-workstream-c-spine-conformance-harness.md`.

## Assumptions Tested

- The old public wrapper names are absent from project, CLI, and conformance Rust source.
- CLI replay/resume now resolve destinations through project-owned runtime resolution before invoking generic replay/recovery.
- Postgres replay validation still fails closed for missing target, unsupported merge dedup, and target mismatch before mutation.
- Package target mismatch is checked before checkpoint proposal in generic replay/recovery.
- Receipt verification still goes through the destination trait-level verify path.
- No unsafe or FFI surface was introduced in the touched runtime/CLI/conformance source.

## Verdict

Pass. B4 completes the caller migration and wrapper deletion promised by Workstream B, with focused tests, feature-matrix checks, security/supply-chain gates, jscpd, rust-code-analysis, CodeQL, and semver evidence.

## Residual Risk

The B4 change intentionally breaks the temporary pre-1.0 public `cdf-project` wrapper API. That is acceptable for this structural-debt program and should not be restored without a new decision superseding Workstream B.
