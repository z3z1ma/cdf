Status: active
Created: 2026-07-12
Updated: 2026-07-13
Supersedes: `.10x/decisions/superseded/commit-session-segment-write-api.md` and `.10x/decisions/run-ledger-commit-session-spine.md` only where they require every `DestinationProtocol` implementation to expose finalized `begin`; their segment API, receipt verification, ledger, and commit-gate decisions remain active.

# Destination ingress protocol capability split

## Context

`.10x/specs/destination-receipts-guarantees.md` originally put `begin -> CommitSession` on every `DestinationProtocol`. `.10x/specs/streaming-destination-ingress.md` later introduced an explicit capability split between finalized-package sessions and pre-finalization staged ingress. A staged request necessarily carries output schema, merge keys, scheduling bounds, attempt identity, and exact durable-segment identities that the kernel `DestinationCommitRequest`/`CommitPlan` pair does not contain. DuckDB's current high-throughput path is staged ingress. Keeping a finalized `begin` implementation beside it required either a second unadvertised commit engine or a plan-success/begin-error stub, both contrary to the current-only and no-leaky-abstraction directives.

## Decision

`DestinationProtocol` is the common destination contract for sheet inspection, dry planning, receipt verification, and correction capabilities. It does not claim an ingress session category. `DestinationRuntime::ingress` returns exactly one `DestinationIngress` variant, selected by capability rather than destination identity.

Finalized-package destinations implement `FinalizedPackageIngress`, whose required `begin_prepared_commit` returns the existing incremental `CommitSession`. Staged-ingress destinations implement `StagedSegmentIngress`. The enum variants are `DestinationIngress::FinalizedPackage` and `DestinationIngress::StagedSegments`; the package or segment lifecycle modifies the ingress category, not the destination runtime itself.

`DestinationRuntimeCapabilities::ingress_mode` selects exactly one live category for a prepared bulk path. The generic runtime MUST require `DestinationIngress::FinalizedPackage` when the selected mode is `FinalizedPackageOnly`, and MUST use `DestinationIngress::StagedSegments` plus verified final binding when the selected mode is `StagedDurableSegments`. A capability/ingress mismatch fails before destination mutation. Destination identity MUST NOT select the branch.

## Alternatives considered

- Keep `DestinationProtocol::begin` and return an error for staged destinations. Rejected because planning succeeds against a public protocol that cannot execute, and the active receipt spec explicitly rejects error-hidden session absence.
- Preserve DuckDB's old finalized session beside staged ingress. Rejected because it is a superseded, separately maintained commit engine with different preparation/provenance behavior and no advertised bulk-path authority.
- Expand one kernel session/request into a union containing every finalized and staged input. Rejected because it conflates materially different lifecycle states: staged acknowledgements cannot claim package identity or committed receipts, while finalized sessions require the verified package token from the outset.

## Consequences

Adding a finalized destination requires implementing the common protocol plus `FinalizedPackageIngress` and returning `DestinationIngress::FinalizedPackage` from the runtime adapter. Adding a staged destination requires the common protocol plus `StagedSegmentIngress`, returning `DestinationIngress::StagedSegments`, and implementing verified final binding. Conformance can now prove the advertised category without calling an impossible method. This is a pre-production Rust API break; no compatibility alias or default error shim is retained.

The receipt shape, commit gate, package-token idempotency, incremental segment delivery, and trait-level verification remain unchanged.
