Status: active
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
2. `.10x/tickets/2026-07-26-parquet-parallel-object-encoding.md` depends on the first child and
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

- No reopening of P3 or repetition of the 1 TiB run.
- No source-, Parquet-, or destination-id branch in generic orchestration.
- No package, receipt, manifest, checkpoint, or artifact-version change.
- No unbounded queues, private executors, hard-coded replacement writer count, or default
  compression change.
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

## Blockers

None.

## Evidence

Pending child closure.

## Review

Pending child closure.

## Retrospective

Pending.
