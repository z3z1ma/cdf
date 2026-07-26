Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: .10x/tickets/2026-07-26-stage-local-cpu-saturation.md
Depends-On: .10x/tickets/2026-07-26-runtime-stage-local-destination-pressure.md

# Encode deterministic Parquet object groups concurrently

## Scope

Replace Parquet's singular active object encoder with bounded independent object-group tasks.
Compile group membership and ordinals from the existing deterministic layout, execute eligible
groups concurrently on the shared `parquet.encode` lane, keep durability/publication on its
appropriate authority, and assemble results by object ordinal.

Prepare the writer count from effective run CPU authority, accounted per-writer memory, schema
and path safety, and the explicit run ceiling. Remove the arbitrary two-writer and two-segment
product ceilings where they are acting as tuning policy; retain exact item/byte bounds derived
for the prepared attempt. Do not retain the current single-active encoder as a fallback or
compatibility path.

## Non-goals

- No package or destination artifact-version change.
- No alternate Parquet codec, compression default, row-group size, or object-layout change.
- No private thread pool, destination branch in generic orchestration, unbounded retained
  segments, or package-sized materialization.
- No 1 TiB rerun or universal throughput claim.

## Acceptance Criteria

- A focused deterministic probe observes at least two simultaneous object-group encoders when
  the prepared plan admits them; writers=1 remains serial.
- Group completion order cannot change object ordinals, segment membership, immutable keys,
  manifests, acknowledgements, receipts, package identity, or final checkpoint semantics.
- Cancellation or one group failure stops admission, joins every sibling, rolls back exact
  attempt state, and releases all memory, CPU/lane, spill, staging, and content claims.
- The session reserves exactly the prepared writer working sets and bounds retained segment
  handles/bytes through the existing staged protocol; the managed-memory ending balance is zero.
- Writer preparation has no arbitrary fixed default ceiling. Explicit run jobs is an upper
  bound; host CPU, memory, and destination safety can lower it without user tuning.
- A bounded release-mode, CPU-heavy, multi-object fixture records writers=1 and automatic/N
  results. Automatic/N must improve median wall by at least 1.5x when the host admits at least
  four useful writers; an ordinary-schema control must not regress by more than 5%. If the
  threshold is falsified, do not select the slower concurrency as the default and record the
  null result.
- Focused Parquet/runtime/project tests, strict Clippy for touched crates, formatting, diff, and
  graph refresh checks pass. No full workspace suite is required.

## References

- `.10x/decisions/stage-local-destination-pressure.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/streaming-destination-ingress.md`
- `.10x/tickets/done/2026-07-14-p3-d8-parquet-staged-parallel-ingress.md`
- `.10x/evidence/2026-07-15-p3-d8-parquet-staged-ingress.md`
- `.10x/tickets/done/2026-07-25-p0-staged-writer-memory-headroom.md`

## Assumptions

- User-ratified: real N-way encoding is worth a bounded implementation and performance
  falsification; no long stress rerun is needed.
- Record-backed: durable canonical segments are safe task inputs, staged acknowledgements may
  complete out of order, and final Parquet objects are already stored in ordinal-keyed maps.
- Record-backed: the prior four-writer result did not execute multiple row encoders and therefore
  is not the retention gate for this topology.

## Journal

- 2026-07-26: Inspection confirmed `stage_stream` owns one `active` group and waits for each
  segment to be consumed before progressing. `pending` contains only groups already sent
  `Finish`, so current concurrency overlaps encode finalization/publication with the next active
  group but does not execute several groups' row encoding concurrently.

## Blockers

Depends on the runtime child so the benchmark observes the intended default graph.

## Evidence

Pending execution.

## Review

Pending fresh adversarial review.

## Retrospective

Pending.
