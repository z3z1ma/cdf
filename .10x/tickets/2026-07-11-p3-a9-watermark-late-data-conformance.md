Status: active
Created: 2026-07-11
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
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
