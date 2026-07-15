Status: active
Created: 2026-07-14
Updated: 2026-07-15
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d4-parquet-streaming-writer.md, .10x/tickets/done/2026-07-14-p3-d7-persistent-staged-ingress-stream.md, .10x/tickets/done/2026-07-11-p3-c3-engine-ffi-parallel-integration.md

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

None. D4 supplies the bounded streaming writer, D7 supplies the generic persistent staged-ingress protocol, and C3 supplies the scheduler substrate. C4 consumes D8's full-path result; D8 does not depend on C4 closure.

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
- 2026-07-15 activation: Fresh C4 review correctly identified that C4 cannot claim its full-path roofline while D8's measured serialized ingress deficit remains. The graph is inverted: D8 depends on the completed C3 scheduler substrate, and C4 now waits on D8's result. The WS-D parent is reopened because this is a destination composition defect, not a program-only reporting tail.
- 2026-07-15 bounded-window foundation: the generic staged ingress contract now permits multiple exact unacknowledged segment readers within the declared segment/byte scheduling bounds and restores canonical ordinal order at snapshot/final binding. Live readers retain their existing Arrow memory leases after transfer to a destination task; verified-package replay permits multiple independently ledger-accounted windows instead of enforcing the superseded one-window process rule. DuckDB remains a valid serial consumer without a special case. Package (56), runtime (68), and project (195) library tests pass.
- 2026-07-15 staged Parquet implementation: the first-party filesystem/object-store adapter now exposes only `StagedDurableSegments`. It requests a bounded two-segment window, encodes independent deterministic objects on the shared `parquet.encode` lane, retains local output under spill reservations, publishes remote output under attempt-scoped staging keys, acknowledges exact staged identities, and binds them to the verified final package before manifest/receipt publication. The finalized-package Parquet ingress, session, validation, and runtime branches were deleted rather than retained as compatibility code. Local final install is atomic; remote final install uses create-only server-side copy with collision verification and explicit staging cleanup.
- 2026-07-15 focused verification: 25 runnable Parquet adapter tests pass (one release roofline test intentionally ignored), including filesystem/object-store commit, duplicate replay, deterministic multi-segment order, replace, receipt tamper detection, schema failures, and correction behavior. The real generic project run and artifact-replay paths both pass through staged Parquet ingress. Strict Clippy passes for `cdf-dest-parquet`, `cdf-project`, `cdf-package`, and `cdf-runtime`; `rg` finds no finalized-package ingress surface in `cdf-dest-parquet`.
- 2026-07-15 scale falsification and repair: the exact 8.59 GB control exposed three architectural defects before closure: background staging discarded a worker's exact error when the producer next observed channel closure; writer working memory failed instead of backpressuring; and retaining every locally staged object until final binding made output size consume the entire CDF spill budget. The generic worker now retains exact failure evidence and uses a rendezvous boundary with no hidden second queue. Parquet writer memory waits on the shared coordinator. Every completed local/remote object moves immediately to isolated destination-owned attempt staging, releasing CDF spill; local final publication is create-only hard-link promotion with a batched directory durability barrier, while remote publication is create-only server-side copy.
- 2026-07-15 verification critical path: receipt verification was rereading and hashing all 14.37 GB immediately after those exact bytes had been hash-while-written, durably staged, and create-only published. The generic outcome contract now permits exact commit-bound receipt verification; orchestration still validates receipt/state structure and exact receipt id before the checkpoint, while duplicate and recovered commits retain independent verification. The final binding/receipt phase fell to 0.251 seconds without weakening the gate.
- 2026-07-15 full-path evidence: the conservative final jobs=4 run completed 4.23 million rows and 460 segments in 21.72 seconds versus 40.67 seconds, with 1.124 GB peak RSS and no staging residue. Its combined 31.782 GB source/package/destination path reached 1.463 GB/s, 0.639x the recorded local device roofline. A preceding repetition took 20.63 seconds. Four writers and Snappy were both measured, regressed or failed to improve the path, and were removed; the measured useful bound remains two uncompressed writers. Evidence: `.10x/evidence/2026-07-15-p3-d8-parquet-staged-ingress.md`.

## Evidence

- `.10x/evidence/2026-07-15-p3-d8-parquet-staged-ingress.md` records the exact full-path before/after, phase metrics, memory, roofline ratio, falsification history, rejected tunings, and evidence limits.
- Adapter conformance: 26 runnable Parquet tests pass (one explicit release benchmark ignored), including exact commit-bound versus duplicate-independent verification and local/object-store abort cleanup.
- Integration: all 195 `cdf-project` and 68 runnable `cdf-runtime` library tests pass; strict affected Clippy and the release build pass with 12 build jobs.

## Review

Pending implementation and fresh adversarial review.

## Retrospective

Pending execution.
