Status: active
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
- No universal throughput claim. The user subsequently required the exact prior 1 TiB acceptance
  run as the final promotion gate; that run is now in scope as comparative evidence.

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
- User-ratified, superseding the preceding bounded-only assumption: rerun the exact prior 1 TiB
  acceptance workload on the 16-vCPU EC2 host, permit all 16 CPUs, compare it with the recorded
  411.5 MB/s / 44:54.863 baseline, and tear the host down after evidence capture.
- Record-backed: durable canonical segments are safe task inputs, staged acknowledgements may
  complete out of order, and final Parquet objects are already stored in ordinal-keyed maps.
- Record-backed: the prior four-writer result did not execute multiple row encoders and therefore
  is not the retention gate for this topology.

## Journal

- 2026-07-26: Inspection confirmed `stage_stream` owns one `active` group and waits for each
  segment to be consumed before progressing. `pending` contains only groups already sent
  `Finish`, so current concurrency overlaps encode finalization/publication with the next active
  group but does not execute several groups' row encoding concurrently.
- 2026-07-26: Execution began after the runtime dependency closed. The implementation will
  transfer each acknowledged canonical segment into its retained `DurableLocalFileAccess`,
  compile deterministic object groups without retaining decoded batches, and decode those
  files inside independently admitted writer tasks. This reuses the destination-neutral staged
  capability already consumed by DuckDB and avoids a second Parquet-specific buffering protocol.
- 2026-07-26: The first focused test exposed an important host-boundary bug before executing any
  payload: declaring the representational `u16::MAX` lane ceiling caused the standalone host to
  try creating that many fixed workers (`os error 35`). The host now sizes a lane's physical
  worker pool to `min(declared ceiling, host slots / slot cost)` while retaining the declared
  capability for admission. This is the non-arbitrary host clamp required by the decision and
  prevents a capability ceiling from becoming eager resource allocation.
- 2026-07-26: The bounded topology probe passed for both sides of the invariant: nine canonical
  segments (deterministic `8+1` objects) reached two simultaneous encoders with automatic
  admission, while the same package under a one-job execution authority observed a peak of one.
  The complete Parquet library suite passed `40` tests with one release benchmark ignored;
  touched Engine/Parquet strict Clippy and formatting also passed. Graph refresh could not run
  because the required `graphify` executable is absent from this environment.
- 2026-07-26: The first dedicated-host retention comparison falsified the initial file-reread
  implementation. Against the true parent baseline (`702212b8`), a one-object 1 GiB logical
  fixture regressed from a `2,407 ms` median to `3,171 ms` (`+31.7%`). The same draft improved
  the ten-object 10 GiB fixture from `18,480 ms` to `12,921 ms` and was `2.55x` faster than its
  own forced-serial path, so independent object tasks were valuable but the redundant canonical
  IPC decode was not.
- 2026-07-26: Reworked each task input to own the existing destination-neutral
  `StagedSegmentRequest`. Its live Arrow batches and memory leases now transfer intact into the
  admitted worker; verified package replay continues through the same reader abstraction. This
  deletes the Parquet crate's added direct `arrow-ipc` dependency, preserves exact ledger
  ownership, and removes both the reread and second decode rather than adding a single-object
  special case. The release retention comparison must be rerun before selection.
- 2026-07-26: The first exact post-transfer multi-object `--jobs 1` falsification found a
  forward-progress defect rather than a throughput result. The session retained twelve live,
  accounted canonical segments while waiting to complete a group; the encoder was only submitted
  after all eight segments in that group had accumulated, so the managed ledger could fill before
  the consumer able to release those leases began. The run was stopped and excluded from evidence.
- 2026-07-26: Replaced whole-group batch retention with one deterministic, bounded command stream
  per object. The worker now starts when its group starts, receives at most the layout's recorded
  `max_segments` requests, and consumes the existing accounted readers while later segments arrive.
  Acknowledgement follows successful transfer into the rollback/redrive scope; no payload copy,
  reread, unaccounted queue, compatibility path, or destination-private executor was introduced.
  Finished groups still join and assemble by object ordinal, and failure closes/cancels/joins both
  active and completed siblings.
- 2026-07-26: The repaired forced-serial cell completed in `26.181 s`, but the first automatic
  16-writer cell exposed that bounding each queue by object membership was still too permissive:
  sixteen workers could collectively retain enough accounted Arrow payloads to leave zero memory
  for the next source decode unit. No destination corruption occurred; the run failed before that
  partition decoded and was excluded. The queue now holds only the residual of the compiled staged
  item window after the worker's current request, additionally capped by object membership. Writer
  count remains independently host/memory/jobs-admitted, while retained requests can no longer
  multiply into complete per-writer object groups.
- 2026-07-26: The bounded destination queue alone did not eliminate the automatic failure:
  partition 16 failed at the same generic file decode-planning boundary before opening its unit.
  Control-flow tracing proved the caller sampled instantaneous free ledger bytes while sibling
  stages were active. The runtime dependency was reopened for the stable-budget correction; no
  further Parquet-specific memory heuristic was added.
- 2026-07-26: Stable decode admission allowed the automatic ten-object run past partition 16 and
  exposed the remaining exact mismatch at canonical assembly: every object writer owned its own
  residual queue, so writer count could multiply the compiled two-segment stage window, while
  engine pipeline admission reserved only one maximum segment for staged handoff. The destination
  now applies the compiled item window once across every writer. Each transferred request carries
  an RAII permit until its existing accounted reader is exhausted or dropped. Engine admission
  independently reserves `maximum_segment_bytes × staged-node maximum_concurrency`. This changes
  neither writer count nor a product tuning knob; it makes the existing graph authority true.

## Blockers

None.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=6 cargo test -p cdf-dest-parquet
  object_groups_use_prepared_parallelism_and_one_writer_remains_serial --
  --nocapture --test-threads=1`: passed; proves actual overlap and explicit one-job serialization.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=6 cargo test -p cdf-dest-parquet
  staged_writer_window_is_reserved_before_input_and_not_charged_again --
  --nocapture --test-threads=1`: passed; proves exact pre-reservation and zero ending balance.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=6 cargo test -p cdf-engine
  representational_lane_ceiling_allocates_only_host_admitted_workers --
  --nocapture --test-threads=1`: passed; proves large declared ceilings do not eagerly allocate
  beyond host admission.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=6 cargo test -p cdf-dest-parquet --lib --
  --test-threads=2`: passed `40`, ignored the one explicit release benchmark.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=6 cargo clippy -p cdf-engine
  -p cdf-dest-parquet --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.
- EC2 `c7i.4xlarge`, production release profile, median-of-three initial draft:
  parent baseline control `2,407 ms`; draft control `3,171 ms`; parent baseline multi-object
  `18,480 ms`; draft automatic multi-object `12,921 ms`; draft forced-serial multi-object
  `32,944 ms`. This proves actual parallel benefit while falsifying the reread implementation
  against the ordinary-schema guard.
- Post-falsification `cargo test -p cdf-dest-parquet --lib --locked -- --test-threads=2`: passed
  `40`, ignored the explicit release-only roofline test.
- Post-falsification strict Clippy for Engine, Parquet destination, and Project: passed.
- Final release retention comparison after accounted-reader transfer: pending dedicated-host
  execution.
- Post-forward-progress repair: all `40` runnable Parquet library tests passed, including actual
  multi-group overlap, explicit one-writer serialization, exact writer-window accounting, grouped
  identity, abort, duplicate, and receipt paths; one explicit release benchmark remained ignored.
  Strict all-target Parquet Clippy, formatting, and diff checks passed.
- Final retained-window repair:
  `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=6 cargo test -p cdf-dest-parquet --lib --offline`
  passed `41`, with the one explicit release benchmark ignored. The new cross-writer test proves
  the compiled two-item authority blocks a third request and releases to zero. The actual
  multi-object overlap/one-writer test and the engine staged-handoff admission test passed.
  `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=6 cargo clippy -p cdf-dest-parquet -p cdf-engine
  --tests --all-features --offline -- -D warnings`, formatting, and diff checks passed.

## Review

Pending fresh adversarial review.

## Retrospective

Pending.
