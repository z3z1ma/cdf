Status: done
Created: 2026-07-26
Updated: 2026-07-26

# Restore stage-local admission and destination CPU saturation

## Scope

Coordinate the bounded runtime and Parquet changes required to stop a staged destination queue
depth from collapsing run-wide CPU admission and to make prepared Parquet writer concurrency
represent actual independent object-group encoding.

This is a parent plan, not an executable ticket.

## Children and sequence

1. `.10x/tickets/done/2026-07-26-runtime-stage-local-destination-pressure.md` removes the incorrect
   run-wide scheduler join while preserving destination-stage backpressure and evidence.
2. `.10x/tickets/done/2026-07-26-parquet-parallel-object-encoding.md` depends on the first child and
   implements measured, bounded N-way Parquet group encoding.

The runtime child lands first so the destination child measures the intended graph rather than a
hidden global clamp.

## Integration points

- `cdf-runtime` owns neutral run-wide versus stage-local admission vocabulary.
- `cdf-cli` and `cdf-project` pass effective execution authority without destination identity
  branches.
- `cdf-dest-parquet` alone owns grouping, encoding, publication, and adaptive writer preparation.
- `cdf-benchmarks` or a destination-owned ignored release benchmark provides the bounded
  before/after measurement.

## Non-goals

- No reopening of the broader P3 program. The user subsequently required repetition of the exact
  prior 1 TiB acceptance run as this parent's final integration gate.
- No source-, Parquet-, or destination-id branch in generic orchestration.
- No package, receipt, manifest, checkpoint, or artifact-version change.
- No unbounded queues, private executors, or hard-coded replacement writer count. Compression
  became in scope only after release evidence proved uncompressed output was the remaining
  physical bottleneck; every choice is compiled into the prepared path.
- No claim that this one change alone reaches the theoretical 2.5--5.1 GB/s extrapolation.

## Acceptance Criteria

- Both child tickets close with their own evidence and adversarial review.
- Default run-wide admission is work-conserving within source, CPU, memory, transport, and scope
  authorities; destination pressure remains bounded locally.
- Parquet demonstrates more than one simultaneous object encoder and a material focused release
  throughput improvement without changing canonical or destination identity.
- Focused ordinary-schema evidence shows no material regression and managed-memory balance
  returns to zero.

## References

- `.10x/decisions/stage-local-destination-pressure.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/specs/execution-host-structured-runtime.md`
- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-1t-process-time.txt`
- `.10x/evidence/2026-07-15-p3-d8-parquet-staged-ingress.md`

## Assumptions

- User-ratified: prioritize CPU saturation, use efficient falsification rather than another long
  stress run, and proceed with implementation.
- Record-backed: package and object identity are jobs-invariant; memory and executor admission
  remain shared authorities.

## Journal

- 2026-07-26: Activated from the measured 1 TiB CPU-utilization deficit. Source inspection
  confirmed the default `staged_destination_in_flight` global jobs join and Parquet's singular
  active encoder. Historical four-writer evidence changed only declared capacity and therefore
  did not exercise the proposed N-way object topology.
- 2026-07-26: The runtime child closed. Default staged destinations now keep upstream jobs
  work-conserving while their item/byte/lane pressure remains enforced and reported locally.
- 2026-07-26: The runtime child was reopened when the dependent EC2 run exposed two hidden
  topology assumptions previously masked by the global jobs clamp: decode-unit planning sampled
  transient free memory, and engine admission budgeted one staged segment while concurrent
  Parquet object writers could collectively retain one each. The first now uses stable total
  budget authority. The second is repaired by one exact destination-global item window plus
  engine admission for the compiled staged-node concurrency; writer CPU admission remains
  independent.
- 2026-07-26: The Parquet child removed the destination-global payload-retention window entirely
  from physical encoding. Every multi-object worker consumes immutable durable segment
  capabilities, admits memory per real object demand, and assembles by deterministic ordinal.
  Release evidence then identified uncompressed output—not worker admission—as the remaining
  device bottleneck. Compiled codec paths selected Zstd level 1 from interleaved measurements.
- 2026-07-26: Final revision `e74bb2fd` completed the exact one-TiB acceptance in `8:19.07`,
  sustaining `2.222 GB/s`, `10.892 million rows/s`, and `678%` CPU with 16 prepared writers.
  This is `5.40x` faster than the `44:54.863` / `411.5 MB/s` baseline. Managed memory ended at
  zero, spill and OOM remained zero, and independent verification reproduced the package hash.

## Blockers

None.

## Evidence

- `.10x/tickets/done/2026-07-26-runtime-stage-local-destination-pressure.md`: focused neutral
  scheduler, stable-budget, staged-handoff, and nested-fan-out evidence.
- `.10x/tickets/done/2026-07-26-parquet-parallel-object-encoding.md`: actual N-way overlap,
  serial control, cancellation/identity, codec, memory, and release retention evidence.
- `.10x/evidence/2026-07-26-parquet-parallel-one-tib-rerun.md`: exact comparative one-TiB
  acceptance with immutable host/binary/run/package evidence.

## Review

Closure review: **pass**. Both children satisfy their mapped acceptance criteria and carry
fresh-hat adversarial passes. The final topology keeps run CPU, destination worker memory,
staged transfer pressure, and physical codec identity as separate authorities. No generic layer
branches on Parquet identity, no fixed two-segment product cap remains in physical encoding,
and the long product run verifies performance, constant memory, receipt/checkpoint semantics,
and package identity together.

## Retrospective

The parent was necessary because the original symptom looked like one cap but spanned scheduler,
memory-ownership, source-frontier, and destination-physical-path boundaries. Treating each
authority independently prevented the final solution from becoming a Parquet exception in the
runtime. Live falsification was more valuable than additional synthetic assertions: every
masked ownership bug appeared only after the preceding bottleneck was removed.
