Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Adaptive batch and canonical segment determinism audit

## Question

How can CDF adapt processing batch sizes and coalesce tiny inputs for throughput without making package identity depend on source page size, runtime pressure, job scheduling, or terminal timing?

## Sources and methods

Traced batch-size options across formats, REST, Python, engine execution, package writing, positions, and manifest identity. Inspected the batch-sizing triage, package/golden determinism contract, P3 adaptive/parallel requirements, and current trace identity behavior.

## Findings

Local formats default to 1,024 rows. Format reads collect all output batches. REST uses the entire response page length as one Arrow batch. Python uses its own configured dict batch rows. The engine writes exactly one IPC segment per accepted output batch and assigns global sequential `seg-000001` identifiers in encounter order.

Therefore source/adapter chunking directly determines file count, fsync/hash/event/destination fixed costs and package hash. Tiny REST pages or files create tiny segments; a large page can create an oversized segment. Parallel partition scheduling would make global encounter-order segment ids nondeterministic.

Adaptive processing driven by live pressure is inherently timing-sensitive. Recording those transient sizes inside identity-participating `profile.json` or `trace.jsonl` would preserve replay observations but violate unchanged-input package determinism. Package trace currently participates manifest identity, so wall-pressure telemetry cannot be placed there.

The output state position is currently one optional position per segment. Coalescing is safe only within a logical partition and only when constituent positions have a typed deterministic join. Splitting a source batch is safe only when the source supplies row/slice position authority; inventing an end cursor from row values would violate source semantics.

Package-wide exact/keyed dedup introduces a global order dependency. Parallel upstream work must retain canonical `(partition ordinal, row ordinal)` order or use an equivalent first-occurrence algorithm before canonical segment assembly.

## Conclusion

Separate adaptive **execution microbatches** from deterministic **canonical package segments**. Internal decode/validate/encode chunks may adapt under memory/pressure and are telemetry only. A canonical segment assembler consumes the ordered admitted row stream under a plan-recorded policy and produces artifact boundaries independent of scheduling and source batch/page boundaries.

Canonical segments never cross logical partition boundaries. They coalesce input chunks up to plan-recorded row/byte targets and flush when a typed source-position join is unavailable. Splitting is allowed only with explicit slice-position support. Segment identifiers derive from plan partition ordinal plus canonical segment ordinal, not runtime arrival order.

Exact initial row/byte targets must be selected from WS-L format/schema measurements within the ratified 8k-64k rows and 1-32 MiB range. The controller may reduce execution microbatch sizes or effective concurrency under pressure, but canonical segment policy cannot change after planning. Manifest segment boundaries are the replay record.

## Limits

The audit does not define every `SourcePosition` join; A3 must implement/falsify position algebra per variant and fail/flush conservatively. It also does not solve global dedup spill/parallel first-occurrence, which needs its own child after A2.
