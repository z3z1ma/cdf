Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 C2 canonical source frontier

## Observation

Partition opening and batch polling now share one source-neutral, scheduler-owned canonical frontier in `cdf-runtime`. Each admitted ordinal owns at most one open/poll future or one retained batch behind the canonical head. The engine no longer carries its prior `FuturesOrdered` open-only path, and executable package runs require the compiled partition schedule's per-partition working-set bounds.

The frontier distinguishes sources that pre-account decoded batches from sources whose batches require a frontier reservation. Frontier-reserved sources reserve the compiled maximum before polling, then reconcile the lease to Arrow allocations plus retained pre-contract evidence. Pre-accounted sources must prove that their batch retention covers those same bytes. A stalled head therefore cannot cause unaccounted later batches to materialize.

Exact limited execution checks a satisfied limit before the next source poll. The serial partial path terminates and joins the current invocation without contacting later partitions or claiming EOF/completion. Retry probing returns the first successful batch to the invocation-bound stream through `OpenedPartitionStream::prepend_batch`; batch ownership is not hidden in engine metadata.

The same run also exposed and repaired two cross-boundary invariants:

- processed cursor observations already carry their window-closed safe position, so package/state aggregation selects the maximum without applying cursor lag again;
- a growing HTTP spool may issue an independent generation-bound range only for a bounded suffix ending at exact EOF. Prefix and interior ranges wait for the sequential spool, so transport behavior no longer depends on whether the downloader happens to win a scheduling race.

The first closure review falsified five boundary assumptions. The repaired design cancels a stalled canonical head on a later failure; computes admitted jobs from the compiled maximum rather than optimistic minimum working set; requires retryable sources to pre-account batches before the scheduler's first-item probe; refuses checkpoint assembly without complete processed-observation evidence; and validates already-closed cursor evidence against the resource descriptor and Arrow schema while joining the prior checkpoint without applying lag twice.

A second fresh review then found five deeper lifecycle and evidence gaps. The frontier now passes its exact cancellation authority into every opener and continues polling admitted opening futures until they terminate and join. It admits only the canonical head initially, reserves that head's maximum before arming later work, and marks every partial partition as checkpoint-ineligible even when an earlier partition completed. Intrinsic cursor artifact aggregation includes the prior checkpoint in its monotone join, and terminal errors are retained and selected by canonical partition ordinal among observed failures.

The final closure review found four remaining gaps. Pre-accounted sources now delay speculative producer opening until the canonical head has materialized its first fully accounted outcome, and the frontier rejects a retained lease above the compiled maximum even when the Arrow payload itself is smaller. Exact limit slicing records a non-slice-invariant cursor only as an attempted partial observation and leaves output segment positions empty. Oversized positioned batches with no row-slice authority emit one conservative exact segment rather than failing or inventing cursors. Structured failure drain retains all observed non-cancellation source failures plus join failures and renders each set by ordinal, while preserving one explicit primary trigger.

## Procedure

Focused regressions:

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel closed_cursor_observations_aggregate_without_reapplying_lag --locked
CARGO_BUILD_JOBS=12 cargo test -p cdf-project general_project_run_window_closes_inexact_numeric_rest_cursor --locked
CARGO_BUILD_JOBS=12 cargo test -p cdf-project trust_ring_explicit_anomaly_fact_demotes_sampled_fast_path --locked
CARGO_BUILD_JOBS=12 cargo test -p cdf-project zero_segment_processed_package_recovers_after_receipt_without_source_or_data_mutation --locked
CARGO_BUILD_JOBS=12 cargo test -p cdf-project general_project_run_rejects_unsupported_postgres_schema_before_writes --locked -- --nocapture
CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files growing_spool_serves_prefix_and_bounded_tail_while_body_transfer_continues --locked
for i in {1..10}; do CARGO_BUILD_JOBS=12 cargo test -p cdf-project http_parquet_auto_pin_plan_preview_and_run_use_file_runtime --locked --quiet || exit 1; done
```

All focused commands passed. The ten HTTP repetitions passed after the suffix-only repair. Before that repair, the broad project suite produced an interior `bytes=4-45` range only under concurrent load, demonstrating that the old predicate was timing-dependent rather than a stable policy.

Complete affected suites:

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel -p cdf-runtime -p cdf-engine -p cdf-project -p cdf-source-files -p cdf-source-rest -p cdf-source-postgres --lib --locked
CARGO_BUILD_JOBS=12 cargo test -p cdf-project -p cdf-source-files --lib --locked
CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel -p cdf-runtime -p cdf-engine -p cdf-source-rest -p cdf-source-postgres --lib --locked
```

The first aggregate run passed the engine and kernel suites, then falsified the HTTP timing bug in one project test. After repair, the complete project and file-source suites passed: 193 project tests and 56 file-source tests. The final remaining affected graph also passed: engine 146 passed with 6 intentional release/slow ignores, kernel 54 passed, runtime 63 passed with 1 performance-lab ignore, Postgres source 11 passed, and REST source 7 passed.

Static and lint verification:

```text
CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets --locked
CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-runtime -p cdf-engine -p cdf-source-files -p cdf-source-rest -p cdf-source-postgres -p cdf-project --all-targets --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
```

All commands passed.

Closure-repair verification:

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel -p cdf-runtime -p cdf-engine -p cdf-project -p cdf-source-files -p cdf-source-rest -p cdf-source-postgres --lib --locked
CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --lib source_frontier --locked
CARGO_BUILD_JOBS=12 cargo test -p cdf-engine --lib limited_ --locked
CARGO_BUILD_JOBS=12 cargo test -p cdf-engine --lib positioned_oversize --locked
CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets --locked
CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-runtime -p cdf-engine -p cdf-project -p cdf-source-files -p cdf-source-rest -p cdf-source-postgres --all-targets --locked -- -D warnings
cargo fmt --all -- --check
git diff --check
```

The complete affected graph passed after every final review repair: 537 tests with 7 intentional release/slow ignores (engine 147/6, kernel 55/0, project 194/0, runtime 67/1, file source 56/0, Postgres source 11/0, and REST source 7/0). The seven focused source-frontier cases cover in-flight opening cancel/join, both source memory contracts, retained-byte ceilings, stalled-head progress, and canonical terminal-error rendering. Focused limit/segmentation cases cover file-manifest and cursor partial attempts plus conservative positioned oversize. Workspace all-target compilation, strict affected-crate all-target Clippy, formatting, and diff validation also passed.

## What this supports

- One reusable frontier owns partition open, one-step polling, canonical release, and cleanup without source/destination identity branches.
- Stalled-head retention is bounded before source polling and returns to zero after canonical consumption or cancellation.
- A live later invocation and a still-opening lifecycle are cancelled and joined exactly once when the frontier fails.
- Jobs two polls later work while the head stalls, but produces the jobs-one manifest identity, lineage, and statistics.
- Zero limit performs no source contact; a nonzero exact limit performs no extra batch poll and records a partial, non-checkpointing observation.
- Missing or widened compiled scheduler/source authority fails before source contact.
- Strong remote Parquet execution retains one sequential payload transfer and permits only exact-EOF bounded range overlap.
- Cursor lag closes once across engine observation, package evidence, replay, and checkpoint state.
- Partial/limited source execution cannot fall back to unsliced segment positions to advance durable checkpoint state.
- Scheduler memory concurrency is bounded by the maximum retained source working set for every admitted ordinal, using the ledger's current available bytes.
- Frontier-reserved execution gives the canonical head reservation priority before later work is admitted, avoiding a bounded-memory deadlock caused by speculative consumers winning the ledger first.
- Closed cursor evidence joins the prior checkpoint monotonically in both state and intrinsic artifact aggregation without applying lag twice.
- Pre-accounted producers cannot start speculative work before the head's first accounted outcome, and cannot hide a larger retained lease behind a smaller Arrow payload.
- A limited cursor batch carries no unsliced output-segment position; its original value remains only in explicitly partial attempt evidence.
- Oversized positioned input remains processable as one exact conservative segment, deleting the former `oversized positioned batch requires exact slice-position authority` failure mode.
- Every observed non-cancellation source failure and join failure is drained and rendered in canonical ordinal order before the frontier returns.

## Limits

This record supports the C2 frontier implementation boundary. The cross-archetype jobs 1/2/auto/N performance and scaling matrix remains owned by P3 C4; unbounded drain epochs remain owned by P3 A8. Mid-stream source retry remains fail-closed unless a source provides exact resume or attempt-local staging.
