Status: open
Created: 2026-07-14
Updated: 2026-07-14
Parent: .10x/tickets/done/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d4-parquet-streaming-writer.md, .10x/tickets/done/2026-07-14-p3-d7-persistent-staged-ingress-stream.md, .10x/tickets/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md

# P3 D8: Parquet staged parallel ingress

## Scope

Move the first-party Parquet destination from finalized-package-only serial transcode to the generic staged durable-segment ingress protocol. Encode and persist the plan-fixed segment-to-object work concurrently under the shared CPU, memory, I/O, and destination authorities while preserving deterministic object, receipt, correction, and checkpoint semantics.

## Non-goals

- No destination-specific branch in engine, project runtime, or CLI orchestration.
- No whole-package Arrow materialization, unbounded local spool, or schedule-dependent object layout.
- No compatibility facade for the superseded finalized-package Parquet ingress.
- No change to package identity, receipt verification, correction policy, or commit-gate ordering.

## Acceptance Criteria

- Parquet advertises and implements only `StagedDurableSegments`; generic orchestration can stream each durable segment without reopening the completed package as the ordinary run path.
- Plan time fixes the segment-to-object/row-group mapping, file names, order, and multipart identities. Jobs and completion order cannot change destination receipt semantics.
- Local and object-store writers overlap bounded encode and persistence across independent planned objects under the shared execution services, with exact acknowledgement, abort, retry, duplicate, replace, and correction behavior.
- Peak memory and local/remote staging disk are bounded by declared leases/budgets. Cancellation and failure remove or abort every unpublished object and release every permit.
- Jobs 1/2/auto/N produce identical package and logical destination receipt identities. Replay uses the same ingress capability and does not retain a finalized-package compatibility path.
- The complete FineWeb package-to-Parquet phase materially improves from the C4 jobs=4 control (`33.069s` for 460 segments) and reaches at least 60% of the named local write/encode roofline or names a measured external codec/device limit.

## Evidence expectations

Staged-ingress conformance, jobs/golden hashes, local and multipart abort/crash matrices, memory/disk pressure and cleanup, full-path FineWeb profile, destination roofline ratio, dependency/identity-branch scans, and fresh adversarial destination review.

## Blockers

None after C4 closure. D4 supplies the bounded streaming writer and D7 supplies the generic persistent staged-ingress protocol.

## References

- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/streaming-destination-ingress.md`
- `.10x/decisions/destination-ingress-protocol-capability-split.md`
- `.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md`

## Assumptions

- Record-backed: the generic staged protocol is already the accepted extension boundary; Parquet must enroll through its adapter rather than add orchestration methods or destination identity dispatch.
- Record-backed: destination object layout and logical receipts are identity-bearing, so parallelism may change timing only; all layout decisions are compiled before execution.
- Record-backed: the C4 control attributes 81% of full-run wall to finalized Parquet destination work, making this the next measured destination bottleneck rather than speculative tuning.

## Journal

- 2026-07-14 shaping: C4's 8.59 GB four-partition FineWeb run reached jobs=4 but spent 33.069 of 40.67 wall seconds in finalized Parquet destination write/receipt. Source/package execution was 7.329 seconds. Source inspection confirms both first-party Parquet runtimes still advertise `FinalizedPackageOnly`, while the governing bulk-path spec requires Parquet to stream row groups/data files as batches arrive. D4 proved the individual bounded writer can exceed the 60% roofline; D8 owns composing it through the generic staged protocol and deleting the superseded finalized path. No implementation is included in this shaping change.

## Evidence

None yet. The measured owner input is `.10x/evidence/2026-07-14-p3-c4-fineweb-local-scaling.md`.

## Review

Pending implementation and fresh adversarial review.

## Retrospective

Pending execution.
