Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-p0-workstream-a-streaming-commit-session.md
Verdict: concerns

# Streaming CommitSession API Review

## Target

Current Workstream A diff implementing `.10x/decisions/commit-session-segment-write-api.md`.

## Assumptions tested

- `CommitSession` now accepts data through a per-segment write API and returns per-segment acknowledgements.
- `DestinationProtocol::begin` is required, not an error-returning default.
- Receipt verification is available through `DestinationProtocol`, and runtime recovery/checkpoint gates use that trait-level verifier.
- DuckDB, Parquet, and Postgres preserve destination-owned receipt, idempotency, duplicate, and package identity behavior.
- Materialized package replay feeds verified package segments through the same session shape as future streaming commit.

## Findings

- Significant, closure-process: The Workstream A ticket requires recorded tests/quality evidence and adversarial review before closure. At review time, `.10x/evidence/2026-07-07-streaming-commit-session-api.md` had been created but was still untracked, and no matching streaming commit-session review record existed. This blocks marking Workstream A done until the evidence and this review are made durable.
- Minor, residual boundary: Package identity is preserved in the supported runtime/replay path because `cdf-project` verifies the package before feeding `CommitSegment` values through the session API. The trait payload itself does not carry segment content hashes, so destination sessions cannot independently prove arbitrary caller-provided batches match the package. This is not closure-blocking for Workstream A under the ratified API; Workstream B must keep the package-verification boundary explicit in the generic orchestrator, and Workstream C must cover the caller contract in conformance.

## Positive observations

- `crates/cdf-kernel/src/destination.rs` defines the segment-writing API and removes the no-data write shape.
- `DestinationProtocol::begin` is required and `DestinationProtocol::verify` is trait-level.
- DuckDB, Parquet, and Postgres sessions stage or accept segments and finalize only after the expected segment set is accepted.
- `crates/cdf-project/src/runtime.rs` verifies packages before segment feeding and calls receipt verification through `DestinationProtocol::verify`.

## Verdict

Concerns raised. No code defect was found that blocks Workstream A closure once the evidence/review durability concern is resolved. The remaining package-identity boundary risk is assigned to Workstream B and Workstream C, not to Workstream A.

## Residual risk

Workstream B still owns removal of destination-specialized replay/recovery/runtime families and must preserve the verified-package-before-segment-write invariant in the generic path.

Workstream C still owns the conformance matrix, chaos coverage, and property/fuzz tests that prove the segment-session caller contract across source/destination/disposition cells.
