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
- 2026-07-15 fresh adversarial rejection: review correctly rejected closure on two critical and four significant findings. The criticals were a synthetic commit-bound verification result over overwrite/non-durable metadata and an input-plus-writer memory wait cycle. Significant findings covered unjoined sibling encodes, process-loss staging cleanup, biased roofline accounting, and destination physical-plan timing. The capability boundary itself passed: no destination identity branch or finalized-Parquet compatibility path remains.
- 2026-07-15 publication/memory repair: local object-store writes now enable file/directory fsync; local staging no longer syncs the same encoded file twice. Immutable provenance selects one authoritative manifest (including commit time) under racing same-token writers; package manifests and per-package replace settlements are create-only; mutable target-current state is separate and exact-read-back verified. A private `CommittedParquetPublication` proof is now the only construction path for commit-bound verification. Tight memory uses a non-waiting combined-budget admission and returns one typed remediation instead of waiting on input leases it owns. Error paths cancel and structurally join every pending encode, with cooperative cancellation checks between batches. Exact rollback/redrive attempts are removed before reuse and an in-process claim prevents live sibling deletion.
- 2026-07-15 repair evidence: all 29 runnable Parquet tests pass (one release benchmark ignored), including new same-token concurrent publication, process-loss exact-attempt redrive, and constrained-memory no-deadlock regressions. Strict all-target Parquet Clippy passes; 19 focused project Parquet tests pass. The isolated staged-ingress roofline, explicit destination physical-plan-version authority, and retained-attempt lifecycle proof remain before closure.
- 2026-07-15 physical plan and retention: `arrow_ipc_to_parquet@2` is now the plan-time policy authority for exact writer count, row/byte batch bounds, ordering, and key derivation. The adapter records those settings in attempt metadata before mutation and validates final package-token-derived keys against the same policy. Successful/aborted attempts clean immediately; a one-minute heartbeat protects active work and exact attempt prefixes older than seven days are collected without touching an in-process sibling. Focused conformance records the exact prepared policy and process-loss retention sweep.
- 2026-07-15 final-binding plan regression: the first integration run caught generic replay re-preparing the destination path after staged mutation began. That changed Parquet's recorded two-writer path to the no-host fallback of one writer and correctly failed authority equality. Final binding now consumes the live attempt's exact `PreparedBulkPath`; only artifact-only replay prepares from verified package inputs. All three general-run/recovery/replay Parquet regressions and the complete 19-test focused project slice pass.
- 2026-07-15 honest roofline: the latest jobs=4 FineWeb run completed in 19.04 seconds (53.2% below the 40.67-second control), with package execution at 18.471 seconds, overlapped destination ingress at 17.906 seconds, final binding at 0.243 seconds, and 1.281 GB peak RSS. Jobs=8 took 19.29 seconds and did not move the knee. Output-only destination throughput is 765.4 MiB/s, 0.481x the contemporaneous 1,591.8 MiB/s direct writer. The median isolated staged replay path measured 642.0 MiB/s (0.372x raw), while the direct writer measured 1,591.8 MiB/s (0.839x raw). This replaces the rejected mixed-byte 0.639 claim and leaves the throughput criterion honestly open: the remaining deficit is the 460-object staged/durability composition, not the Arrow writer or device.
- 2026-07-15 second fresh review and lifecycle repair: review rejected closure because mutable replace publication was not fenced across processes, attempt ownership was destination-instance-local, heartbeat advanced only after segment completion, duplicate verification buffered whole objects, prepared writer settings were ceremonial, post-promotion failures could strand unbounded final objects, and the isolated benchmark mislabeled logical bytes as physical. The destination store now exposes one generation-CAS control-object primitive: filesystem writers serialize through a persistent advisory lock and durable atomic rename, while remote writers use provider generation preconditions. Current replace publication monotonically installs the newest total pointer order and advertises no exclusive-writer requirement. Exact attempt claims are process-wide per configured store/target/attempt; a periodic worker refreshes staging and active-publication markers independently of segment completion. Verification hashes local files as streams and remote objects in ledger-accounted 64 MiB ranges. Prepared row/byte bounds now drive writer memory, write batches, pages, and row groups. Heartbeated package markers protect active final binding and permit seven-day collection of manifest-less publications after process loss. The isolated benchmark now sums `parquet_byte_count` and enforces the real 0.60 threshold. Throughput remains open pending object coalescing and a new measurement.

## Evidence

- `.10x/evidence/2026-07-15-p3-d8-parquet-staged-ingress.md` records the exact full-path before/after, phase metrics, memory, roofline ratio, falsification history, rejected tunings, and evidence limits.
- Adapter conformance: 34 runnable Parquet tests pass (two explicit release benchmarks ignored), including generation-CAS replace replay, process-wide attempt exclusion, heartbeat progress without segment completion, bounded orphan-publication collection, exact commit-bound versus duplicate-independent verification, local/object-store abort cleanup, physical-plan execution, retained-attempt collection, concurrent publication, and tight-memory termination.
- Integration: all 195 `cdf-project` and 68 runnable `cdf-runtime` library tests pass; strict affected Clippy and the release build pass with 12 build jobs.

## Review

Second fresh adversarial review verdict: **fail** with one critical publication-fencing finding and significant lifecycle/benchmark/throughput findings. The lifecycle and benchmark defects are repaired and journaled above; the output-only destination roofline remains below the ticket threshold and must not be waived by aggregate accounting. A new fresh review is required after the throughput repair.

## Retrospective

Pending execution.
