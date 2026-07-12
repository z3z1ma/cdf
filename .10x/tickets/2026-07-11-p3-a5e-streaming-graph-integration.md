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
- 2026-07-11: Ordinary run now begins staged ingress under scan-plan authority and synchronously hands each canonical segment to the generic destination session immediately after encode/hash/fsync/rename/directory-sync, using the already-resident Arrow batch rather than rereading the package. Final package verification binds the actual destination commit plan and exact staged identity list before receipt/checkpoint authority. Finalized-only destinations retain their existing path. Conformance covers a two-file live run and a second-segment staging failure: the latter aborts the attempt, leaves the draft at `extracting`, and never proposes a checkpoint.
- 2026-07-11: The staged edge now runs concurrently on the destination-declared blocking lane through the injected execution host. Its bounded channel enforces both segment count and total in-flight bytes; every queued Arrow buffer owns a global queue-memory lease. A pre-manifest join barrier converts worker failure into package-build failure before `Validated`/manifest publication. Conformance proves the destination executes off the run thread, exact final binding remains unchanged, and a background second-segment failure aborts cleanly before checkpoint proposal.
- 2026-07-11: Run-time destination resolution now joins back to the compiled graph before package creation: staged-node presence, executor/lane, byte bound, and writer concurrency must exactly match the resolved capability sheet. Stale plans fail with a rebuild remediation instead of silently executing a different topology.
- 2026-07-11: Package verification is now a single typed consumption authority carried through the graph-to-replay/final-binding transition. Generic orchestration no longer rehashes the complete package at each replay helper or destination boundary, and destination planning receives the same neutral proof through its context. The local TLC staged run measured 1.64 seconds versus the recent 1.89-second median while preserving exact receipt/checkpoint binding.
- 2026-07-11: The durable package finalizer now hands its hash-while-write verification authority directly into ordinary-run final binding. No package content is reopened between finalization and staged commit. Fresh TLC controls measured a 1.58-second median while reopened replay retains independent verification.
- 2026-07-11: Removed the hidden segment-wide Arrow concat from production persistence. Canonical segments now contain deterministic plan-owned microbatches; exact 64k decoder batches cross the package and staged-destination boundary zero-copy, while only fragments spanning a canonical boundary are concatenated. Source-rechunk, fused/unfused, durable-hook, and staged-final-binding laws remain green. TLC package execution fell from 1.240s to 1.212s in direct phase telemetry; median CPU fell from about 1.65s to 1.60s. Evidence: `.10x/evidence/2026-07-11-p3-a5e-zero-copy-canonical-microbatches.md`.
- 2026-07-11: Split package segment encode/hash/durable publication from canonical manifest registration. Cloneable package-owned encoders can now complete uniquely named segments out of order, while only the engine frontier can register receipt/segment journals in canonical order. The direct write API uses the same two-step implementation; no alternate writer remains. This is the package boundary required for parallel segment encode/persist without scheduling-dependent manifests.
- 2026-07-11: Segment encode/hash/fsync now runs through structured injected-host CPU tasks with a bounded, memory-derived, four-task host ceiling and canonical ordinal frontier. Decode/transform continues while prior segments encode; completion order cannot affect journals, durable hooks, destination staging, lineage, positions, or manifests. Drop/error cancels and joins the scope. TLC package execution fell from 1.212s to 1.178s; three four-task runs measured 2.39/1.51/1.53 seconds wall (1.53 median). A nine-task experiment measured a worse 1.81-second median and was rejected as contention. Evidence: `.10x/evidence/2026-07-11-p3-a5e-parallel-segment-frontier.md`.

## References

- `.10x/specs/streaming-operator-graph.md`
- `.10x/specs/streaming-destination-ingress.md`
- `.10x/specs/package-io-hashing-durability.md`
