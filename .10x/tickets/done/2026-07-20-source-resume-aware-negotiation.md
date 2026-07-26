Status: done
Created: 2026-07-20
Updated: 2026-07-26
Parent: .10x/tickets/done/2026-07-25-stabilization-steady-state-program.md

# Source-neutral resume-aware negotiation

## Scope

Move committed-frontier binding ahead of expensive source task planning through a source-neutral
`QueryableResource` negotiation seam. Iceberg `append_snapshots` must prove ancestry before
loading manifests and plan only admitted append manifests; an unchanged snapshot must avoid
manifest-list/task-set planning entirely. Preserve the existing post-plan rebind seam only for
cheap inline partition rebinding if it remains necessary after the migration.

## Non-goals

No Iceberg/Glue identifiers in generic engine/project/CLI code, no checkpoint-store access from a
source adapter, no weakening of plan/package identity, and no source-owned state store.

## Acceptance Criteria

- The engine planner accepts an optional typed committed frontier and invokes one source-neutral
  resume negotiation method before task authority is materialized.
- Project run and plan commands obtain the applicable frontier through ordinary state authority;
  preview without a state binding remains explicit.
- Unchanged Iceberg runs do not open manifest lists or manifests; append runs load only manifests
  added by admitted append snapshots.
- File-manifest and ordinary partition resume laws remain green; no source kind appears in generic
  orchestration.
- Before/after local and FQ12 evidence records planning wall time, metadata objects/bytes, and
  task-store peak memory.

## References

- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/iceberg-source.md`
- `.10x/tickets/done/2026-07-19-iceberg-i2-scan-execution.md`
- `.10x/tickets/done/2026-07-19-iceberg-i3-incremental-product-conformance.md`

## Assumptions

- Record-backed: committed state belongs to project/checkpoint authority, while source-specific
  frontier interpretation belongs behind `QueryableResource`.

## Journal

- 2026-07-25: Added a typed optional committed frontier to engine-plan input and a
  source-neutral `QueryableResource::negotiate_with_committed_frontier` seam. CLI `run` resolves
  the selected pipeline's checkpoint head before compilation; `plan`/`explain` resolve the
  ordinary `cdf-run` head; preview explicitly has no state binding. Generic orchestration now
  accepts an exactly matching prebound frontier, retains the cheap inline fallback, and rejects
  stale or externally materialized unbound authority rather than silently executing it.
- 2026-07-25: Iceberg now proves unchanged selection or append ancestry before task planning.
  Unchanged selection returns an inline empty scan before opening the manifest list or creating a
  planning index/task writer. Append planning parses the selected manifest list but admits and
  opens only manifests added by the proven append snapshots. The superseded full-task-set
  read/filter/rewrite path and its post-plan Iceberg resume override were deleted.
- 2026-07-25: Live read-only FQ12 planning for `flolake.transactions` returned zero partitions,
  zero rows, and zero bytes in 4.84 seconds with 438,779,904 process peak RSS. The external
  planner-artifact file count remained exactly four before and after. The prior I3 evidence was
  3.44 seconds plus 68,157,455 peak task-writer bytes and 9,502,720 planning-index bytes after
  full task planning; the total wall figures are not directly comparable because this command
  also compiles the 2,000-column destination DDL, but the task-store/planning-index elimination is
  exact.
- 2026-07-25: The first workspace behavioral gate found that `plan`/`explain` opened SQLite merely
  to check for a frontier, creating `.cdf/state.db` in a fresh project. Planning now checks the
  configured ledger path without mutation and returns no frontier when it does not exist; the
  failing no-write explain scenario and the existing-ledger frontier scenario both pass.
- 2026-07-25: The no-fail-fast workspace run then exposed a drain recovery composition error.
  Runtime FileManifest/epoch rebinding had been allowed to overwrite the new field whose sole
  meaning is “frontier consumed during compiler task planning.” The method now leaves that
  compiler evidence unchanged; scan authority still records the cheap runtime rebind. The exact
  receipt-to-checkpoint recovery test passes again without weakening the stale compiled-authority
  check.
- 2026-07-25: Execution resumed under the stabilization program. The control-flow trace confirms
  that CLI planning currently materializes the complete source task authority before project
  runtime opens checkpoint state, after which `rebind_initial_committed_frontier` filters work
  post hoc. The repair will make the typed committed frontier an engine-plan input, add one
  source-neutral resume-aware negotiation seam, resolve state before `run`/`plan` compilation,
  and let Iceberg prove append ancestry before opening manifest lists or manifests. The
  superseded Iceberg external-task filtering path will be deleted rather than retained as a shim.
- 2026-07-20: I3 proved exact append filtering through the existing post-plan rebind seam. Live
  unchanged-snapshot evidence still spent 3.44 seconds in full task planning before clearing the
  scan, and the filtered append path necessarily materializes the full current task set first.
  This ticket owns that source-neutral lifecycle/performance debt; it is not an Iceberg correctness
  blocker and must not be solved by injecting state access or Iceberg branches into generic code.
- 2026-07-18: The closed `PartitionAuthority` migration removed generic access to resident task
  vectors. Complete external drain epochs rebind through `ResourceStream`; partial external epochs
  now fail explicitly because their continuation requires source-owned task slicing. This ticket's
  preplanning resume seam owns that slicing contract as well as the unchanged-run optimization.

## Blockers

None.

## Evidence

- Planner contract: `cargo test -p cdf-engine
  tier_b_planning_binds_the_committed_frontier_during_source_negotiation --locked` passed. The
  source observes the typed frontier during negotiation, the resulting task starts at that
  frontier, and the plan records the same value.
- State authority: `cargo test -p cdf-cli
  compiler_planning_frontier_comes_from_the_default_pipeline_head --locked` passed. It proves the
  compiler obtains the resource/scope-specific position from the checkpoint head.
- Zero-open and append-selectivity laws: `CARGO_BUILD_JOBS=12 cargo test -p
  cdf-source-iceberg --lib --locked` passed all 41 tests. The unchanged test deletes the selected
  manifest list before negotiation and still produces an inline zero-task plan; therefore
  task-planning metadata reads are exactly zero objects/zero bytes and task-store peak is zero.
  The append test deletes the historical manifest and still plans only the newly admitted
  manifest, proving old manifests are not opened.
- Resume compatibility: the bounded unchanged-snapshot, multi-partition drain continuation, and
  FileManifest append/no-op tests in `cdf-project` passed independently. The seven FileManifest
  regression tests also passed together. The full no-fail-fast workspace run executed all 1,847
  tests and found only the drain recovery composition issue above; its other 1,846 tests passed.
  The repaired drain recovery test and the unchanged-snapshot test then passed together, followed
  by the complete `cdf-project` suite: 214 passed, zero failed.
- Live FQ12: read-only `cdf --json plan flolake.transactions` returned
  `partition_count=0`, `rows=0`, and `bytes=0`; planner-artifact file count was `4 -> 4`;
  `/usr/bin/time -lp` recorded `real 4.84`, `user 2.84`, `sys 0.19`, and 438,779,904 maximum RSS.
  Limit: total command wall/RSS includes catalog resolution, schema handling, and wide DuckDB DDL,
  so it is not presented as a pure source-planner microbenchmark.
- Static/build coverage: `cargo check --workspace --all-targets --locked`, focused strict clippy
  across kernel/runtime/engine/project/Iceberg/CLI, full-workspace all-features check and strict
  clippy, `cargo fmt --all`, and `git diff --check` passed. The graph was refreshed with
  `graphify update .`.

## Review

Fresh-hat adversarial control-flow review, 2026-07-25. Verdict: pass. No critical or significant
correctness, performance, determinism, or extension-boundary finding remains. The checkpoint store
stays in project/CLI authority; generic code sees only `SourcePosition`; Iceberg owns ancestry and
manifest interpretation; stale state between compilation and execution fails before package
creation. Registry and window wrappers forward the semantic negotiation override rather than
silently falling back to the trait default. Residual risk: the FQ12 wall measurement is dominated
by work beyond source task planning and therefore supports live product correctness, not a
fine-grained roofline claim.

## Retrospective

Moving the state read earlier was not sufficient by itself: every semantic wrapper around
`QueryableResource` had to forward the new negotiation override, or the optimized source method
was silently hidden behind the trait default. A test through the registry wrapper caught that
composition defect. The simplest complete design is one typed preplanning input, one
source-neutral negotiation method, exact runtime state-race validation, and deletion of the
source-specific post-plan rewrite.
