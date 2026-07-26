Status: open
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

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending fresh adversarial review.

## Retrospective

Pending.
