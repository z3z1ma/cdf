Status: active
Created: 2026-07-26
Updated: 2026-07-26
Parent: .10x/tickets/2026-07-26-stage-local-cpu-saturation.md

# Keep staged destination pressure out of run-wide jobs

## Scope

Make the neutral scheduler distinguish run-wide leaf-work admission from destination-stage
capacity. Delete the default path that joins `max_in_flight_segments` into effective upstream
jobs, preserve item/byte/lane bounds in the compiled graph and staged-ingress protocol, and keep
those stage capacities visible in run evidence.

Remove superseded scheduler fields, comments, tests, and benchmark expectations that encode the
old global-pressure interpretation rather than leaving a compatibility surface.

## Non-goals

- No Parquet group-encoder change.
- No destination-specific branch in runtime, engine, project, or CLI.
- No removal of staged item/byte backpressure.
- No change to explicit `--jobs`, source/cgroup CPU, memory, source-lane, transport, or scope
  ceilings.
- No full workspace suite or 1 TiB rerun.

## Acceptance Criteria

- With 12 partitions, 16 host slots, 16-way source capability, sufficient memory, no explicit
  jobs, and a staged destination window of two, effective upstream jobs resolve to 12 rather than
  two.
- The same destination still reports its two-item/tuned-byte stage capacity and the graph and
  staged stream enforce both bounds.
- Explicit jobs remains authoritative and still tightens the one shared run-work authority
  before payload execution.
- No generic runtime type or serialized report describes a stage-local item window as a
  run-wide jobs ceiling.
- Decode-unit tuning uses stable managed-budget authority rather than transient free bytes;
  actual reservations continue to wait on the shared memory coordinator, so restored upstream
  concurrency cannot turn ordinary stage pressure into a planning failure.
- Focused runtime/CLI/benchmark-policy tests pass, strict Clippy passes for touched crates, and
  formatting/diff checks pass.

## References

- `.10x/decisions/stage-local-destination-pressure.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/specs/execution-host-structured-runtime.md`
- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md`

## Assumptions

- User-ratified: the destination queue depth is not the default global CPU ceiling and efficient
  focused tests are sufficient for this repair.
- Record-backed: byte/item channels, graph-node concurrency, destination lanes, and the memory
  ledger already enforce stage-local pressure.

## Journal

- 2026-07-26: Cold-start inspection found the obsolete join in
  `resolve_runtime_scheduler`, its `AdmissionCeilings` candidate, a protecting unit test, and a
  benchmark-policy assertion. `run_command` then tightened the shared run-work authority to that
  result. No destination identity branch is required to repair it.
- 2026-07-26: Activated execution. Removed both destination writer count and staged retained-item
  window from the run-wide `AdmissionCeilings` vocabulary and candidates. Destination writer and
  retained-segment capacities remain separate fields in `RuntimeSchedulerResolution` and remain
  compiled into the destination graph/staged scheduling context.
- 2026-07-26: The focused runtime test now resolves 12 partitions on a 16-slot host to 12
  upstream jobs. The destination jobs matrix exercises both DuckDB and Parquet at jobs
  1/2/auto/4; auto now resolves four run-wide jobs while package and receipt identities remain
  invariant. The staged protocols continue to consume their unchanged item/byte bounds.
- 2026-07-26: The first benchmark-test invocation reached the linker but correctly exposed the
  absent local dynamic DuckDB library. Repeating only that focused test with the repository's
  supported `DUCKDB_DOWNLOAD_LIB=1` local mode passed; CI's static DuckDB policy is unchanged.
- 2026-07-26: Reopened during the dependent EC2 retention gate. With stage-local admission
  restoring sixteen active file partitions, partition 16 reproducibly failed before decode because
  `stream_registered_format` passed the ledger's instantaneous free-byte snapshot into
  `resolve_decode_unit_concurrency`. That value is transient runtime pressure, not a stable
  planning authority; the Parquet decoder already performs cancellable async reservations and
  waits for release. The old global destination cap merely hid this defect.
- 2026-07-26: `resolve_decode_unit_concurrency` now names and consumes a stable managed-budget
  authority. The registered-format caller passes total managed budget rather than free bytes at
  that scheduling instant; actual Parquet batch allocations still use cancellable async
  `cdf-memory` reservations, so live contention waits and releases rather than bypassing the
  ledger. A focused regression fills the ledger completely with a sibling lease and proves decode
  planning still sees the unchanged budget authority.

## Blockers

None.

## Evidence

- `cargo fmt --all -- --check` — passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime
  effective_jobs_keep_staged_destination_pressure_stage_local --lib --locked -j 12` — passed,
  1/1.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks
  destination_ingress_categories_preserve_jobs_identity --test lab_runners --locked -j 12` —
  passed, 1/1 in 3.97 seconds. The real prepared-file path preserved identical package and
  receipt identities across both staged destination categories.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime --all-targets --locked -j 12 --
  -D warnings` — passed.
- `rg -n "destination_writers|staged_destination_in_flight" crates --glob '*.rs'` — no
  matches, proving the superseded run-wide vocabulary is absent from product and test source.
- `git diff --check` — passed.
- Reopened repair: the exact runtime decode-unit resolver test and source-files transient-pressure
  regression passed; strict all-target Clippy for both touched crates and formatting/diff checks
  passed. EC2 automatic multi-partition proof remains pending.

## Review

Fresh-hat adversarial review: **pass**. The diff deletes both destination-derived candidates
rather than special-casing Parquet, leaves the explicit jobs/CPU/memory/source/lane/scope
authorities intact, and leaves `destination_writer_concurrency` plus
`destination_in_flight_segments` as report-only stage facts. Graph-node validation and
`StagingSchedulingContext` still bind the destination item/byte/lane limits. The jobs matrix
would fail if either destination's bounded stream stopped composing or if canonical identity
changed. Residual risk is limited to host-scale throughput, deliberately owned by the dependent
Parquet ticket rather than this neutral admission repair.

## Retrospective

The bug survived because one field named “in flight” represented two different topologies: a
destination queue and the global leaf-work pool. Deleting the ambiguous global candidates is
safer than adding an exception. Stage capacity already had complete graph, channel, lane, and
telemetry authorities; reusing those boundaries made the repair small and generic.
