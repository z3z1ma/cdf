Status: done
Created: 2026-07-11
Updated: 2026-07-19
Parent: .10x/tickets/done/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a8-drain-epoch-executor.md

# P3 A9: watermark, late-data, and epoch conformance closeout

## Scope

Build shared conformance for typed watermark claims, partition aggregation/idleness/resumption, operator propagation/invalidation, all late-data verdicts, drain lifecycle, serialization, memory bounds, replay, and performance overhead; update VISION 6.5–6.6 coverage from evidence.

## Acceptance criteria

- Claims cannot regress or become stronger through an undeclared operator.
- New/idle/resumed partitions and each late-data action have fail-closed deterministic tests.
- No accepted/quarantined/recaptured row disappears across epoch barriers or recovery.
- Epoch control overhead remains within the P3 total overhead budget and metadata stays bounded.
- Coverage rows 6.5–6.6 cite implementation, conformance, and evidence rather than the future supervisor alone.

## Evidence expectations

Property/state-machine tests, chaos matrix, package/quarantine/checkpoint inspection, jobs-invariance hashes, memory/performance reports, and adversarial correctness/performance review.

## Explicit exclusions

No concrete CDC source, resident supervisor, or general windowing engine.

## Blockers

None. A8 is done.

## References

- `.10x/specs/stream-epochs-watermarks.md`

## Journal

- 2026-07-19: Activated immediately after A8 closure. This ticket consumes the implemented epoch, continuation, watermark-aggregation, replay-retention, and crash-recovery authorities; it will add total late-data/idleness/operator-propagation conformance without reopening A8's runtime architecture.
- 2026-07-19: Corrected the first resumed-partition defect before adding row verdicts. `PartitionWatermarkTracker` now carries a receipt-gated effective floor: a missing, newly eligible, or resumed partition can make its rows late but can never retract completeness already committed by an earlier epoch. The tracker still refuses partition-local claim regression and advances only to the minimum of all eligible claims. Focused runtime coverage proves both new-partition and resumed-partition behavior.
- 2026-07-19: Made event-time completeness durable rather than process-local. The current v1 checkpoint delta and its package preimage now carry `output_watermark`; the project writes the exact deterministic epoch-frontier watermark, restores it into a new controller, and the next epoch seeds both aggregation and late-data comparison from that receipt-gated value. This deliberately updates the sole current artifact shape with no compatibility reader. Kernel serde and runtime restart tests pass, as does an all-target check of kernel/package-contract/runtime/engine/project.
- 2026-07-19: Added one engine-owned, destination-neutral typed late-data classifier across signed/unsigned integer widths, decimal128/256, date32/64, and all timestamp units. The ordinary fused path pays no classification cost for bounded or watermark-disabled plans. For enabled drains it compares transformed rows against the prior effective global watermark before admitting the batch's new claim, retains exact source ordinals, and writes identity-bearing `stats/late-data.json`. Quarantine now removes the row, emits the named `cdf.late_data` quarantine verdict, and preserves typed lateness evidence; admit-with-annotation retains the row and the same evidence. A two-epoch test proves both actions against an event-time regression while source offsets continue monotonically. Recapture classification is implemented but deliberately fails before dropping a payload until durable next-epoch carryover lands in the next tranche.
- 2026-07-19: Completed the durable recapture calculus. A recaptured normalized batch is now an LZ4 Arrow IPC identity artifact beneath the closed `carryover/` package layout; the checkpoint records its package/path/content identity, source position, row count, and conservative memory bound. The next epoch opens that object only through prior-package verification authority, reserves its encoded-plus-decoded working set from the shared ledger, validates schema/rows/memory, and feeds it through canonical segmentation or package dedup without re-contacting the source or re-running semantic transforms. Consumption records one non-advancing control position so a carryover-only epoch is not mistaken for an empty no-op, while source record/byte counters are not charged twice. Focused engine conformance proves quarantine, annotation, and recapture across a verified package boundary; a recaptured row withheld in epoch two is emitted in epoch three and the outgoing checkpoint carryover is cleared. Package and checkpoint fixture identities were intentionally advanced in place because there is no legacy artifact reader.
- 2026-07-19: Replaced the O(partitions) aggregation scan on every source batch with ordered eligible-claim and idle-deadline indexes. The tracker retains one state/deadline per planned partition, performs O(log P) admission/expiry work, preserves the receipt-gated floor when claims are missing, and exercises a real idle-to-resumed transition. The release probe processed 1,000,000 updates across 100,000 partitions at 355.38 ns/update and 2.814 million updates/second.
- 2026-07-19: Closed the first adversarial review's data-loss findings. Quarantine and recapture now preserve the exact post-contract/post-redaction row in LZ4 Arrow IPC identity artifacts; the compact evidence record points to artifact ordinal and row ordinal. Both the row evidence and payload catalog are hash-while-streamed, removing row- and batch-cardinality resident vectors. Late quarantine mutates the existing verdict total instead of merging a second fictitious input summary, so input and accepted rows remain conserved.
- 2026-07-19: Closed watermark-state escape hatches at every authority boundary. `DrainEpochController` normalizes missing observations to its monotone floor; execution validates every ordered header claim so a valid tail cannot hide an earlier regression; `WatermarkClaim` owns same-domain successor validation; and both checkpoint stores validate disappearance/regression at propose and again at commit against the current head. A two-proposal race test proves a stale proposal cannot retract a newer committed watermark.
- 2026-07-19: Added enabled-watermark jobs invariance and project recovery joins. Jobs 1 and 8 produce identical package hashes, segments, and epoch evidence for the same captured interval while jobs 8 activates parallel source work. Project recovery reopens carryover only from a verified package whose content hash equals the committed checkpoint head and fails closed on conflicting authority.
- 2026-07-19: Measured the common no-late typed classifier in a fat-LTO release build: 65,536 rows x 1,024 iterations completed in 0.074906 seconds, or 6.675 GiB/s over the event-time column. Focused kernel/package/runtime/engine/state/project tests and strict affected-crate Clippy passes are recorded in `.10x/evidence/2026-07-19-p3-a9-watermark-late-data-conformance.md`.
- 2026-07-19: The broad affected-package run was attempted rather than implied green. It reached 155 passing engine tests but exposed 23 failures from concurrently landed external-task/discovery authority changes (noncanonical fixture hashes, discovery binding, batch accounting, and schedule migration), plus an existing runtime-ownership lint. None is hidden or weakened here; the exact command, categories, and limit are preserved in the evidence record for their owning workstream.
- 2026-07-19: Closed the independent review's remaining lifecycle findings. Explicit source-authored idleness replaces scheduler-timing inference; receipt-gated per-partition watermark state now survives epoch and process boundaries; FileManifest batches retain their claims; and a real injected receipt-to-checkpoint crash recovers the verified carryover object (`2363f411`, `09d25d7c`).
- 2026-07-19: Grouped late-row evidence by source batch so partition, source-position, and watermark authority are retained once rather than cloned per late row (`f99d28a9`). Five fat-LTO no-late probes measured a 5.944 GiB/s median. An allocation-avoidance candidate that regressed the median was discarded. The complete focused test tranche and strict `-D warnings` Clippy gate over all seven affected crates pass.

## Evidence

- Claims cannot regress or become stronger through an undeclared operator: `2fc9ccac`; runtime controller/tracker tests, the engine all-claims regression test, checkpoint-store race test, and existing operator graph preservation/invalidation tests in `.10x/evidence/2026-07-19-p3-a9-watermark-late-data-conformance.md`.
- New/idle/resumed partitions and all late-data actions: `bdd86245`, `62c4d451`, `dfdf9dc1`; focused tracker and three-action package tests in the evidence record.
- No accepted/quarantined/recaptured row disappears: exact output counts, exact quarantine Arrow payload inspection, and verified next-epoch recapture emission in `late_rows_are_quarantined_or_admitted_with_identity_evidence`; committed-head project loader conformance in `committed_head_reopens_only_its_verified_late_data_carryover`.
- Performance and bounded metadata: O(log P) indexed tracker, hash-while-streamed evidence/catalogs, 2.814 million tracker updates/second, and 6.675 GiB/s no-late classification in the evidence record.
- Coverage rows 6.5–6.6: the coverage matrix cites the closed ticket, implementation commits, and durable evidence record.

## Review

The independent adversarial review initially failed the tranche with two critical and seven significant findings: process-local partition floors, scheduler-inferred idleness, discarded FileManifest claims, simulated recovery, unbounded/repeated row evidence, incomplete performance evidence, weak artifact joins, and admitted rows without exact output identity. The repair tranche addressed each correctness finding with focused tests and strict Clippy. The remaining scale boundary is explicit rather than hidden: the inline tracker is O(active planned partitions); C5 owns external high-cardinality task/result metadata and spill. Performance evidence is deliberately scoped to the two added hot kernels, both well above their program floors; Z1 owns the host-class whole-pipeline overhead cell. Verdict: pass for A9's finite-epoch scope, with those two already-owned integration limits.

## Retrospective

The first implementation made the correct policy decision but retained insufficient row authority and put some invariants only in the ordinary controller path. The repair was to make exact row payloads identity artifacts, move monotonicity into kernel/store transition validation, persist partition authority, and stream or group evidence rather than arguing that epochs would usually be small. Total verdicts require total payload disposition, and receipt-gated facts need enforcement at persistence boundaries as well as orchestration boundaries. Performance candidates must be measured before retention: the allocation-avoidance experiment looked attractive but was deleted after the benchmark showed a regression. The retained typed classifier remains a 5.944 GiB/s median path, while the indexed tracker removes the prior partition-count multiplier.
