Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a5b-fused-transform-kernel.md, .10x/tickets/done/2026-07-11-p3-a5c-durable-segment-stream.md, .10x/tickets/done/2026-07-11-p3-e2-streaming-manifest-durability.md, .10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md

# P3 A5e: run/replay/destination streaming graph integration

## Scope

Route ordinary run, replay, resume, correction, finalized-only commit, and staged ingress through the compiled graph; remove compatibility materialization from production; integrate bounded metadata/finalization; and close end-to-end overlap, failure, memory, determinism, and extension conformance.

## Acceptance criteria

- Generic integration has no source/format/destination-name branch and a mock external driver passes by registry/capability changes only.
- Slow source/destination, reordered completion, spill, and high segment/file cardinality remain within configured memory/disk bounds.
- Jobs/batch/pressure/destination-speed variations preserve identity, positions, verdicts, lineage, receipts, and checkpoints.
- Every edge failure/cancellation leaves no task, permit, temp draft, or unowned staged attempt.
- Lab evidence shows decode/transform/persist/destination overlap and reports graph overhead; production materialization scans are empty.

## Evidence expectations

End-to-end conformance matrix, architecture scans, high-cardinality/RSS stress, chaos/crash suite, mock extension, replay parity, and before/after profiles.

## Explicit exclusions

No distributed worker protocol, destination-specific bulk encoder, decoder-specific SIMD, or parallel partition scheduler.

## Blockers

Depends on A5b, A5c, E2, and A6.

## Progress and notes

- 2026-07-11: All declared dependencies are closed. Production replay now selects finalized-only versus staged durable-segment ingress exclusively from `DestinationRuntimeCapabilities`. The staged path streams the verified, ledger-accounted package window; validates exact segment hash/schema/ordinal acknowledgements; aborts an owned staging session on every pre-binding failure; and issues a receipt only through exact verified final-package binding. A mock destination composes through the runtime trait without prepare/bind branches or first-party identity checks. This is the finalized-package integration milestone; moving the same durable handoff to segment-persist completion for useful pre-finalization overlap remains open.

## References

- `.10x/specs/streaming-operator-graph.md`
- `.10x/specs/streaming-destination-ingress.md`
- `.10x/specs/package-io-hashing-durability.md`
