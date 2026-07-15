Status: active
Created: 2026-07-11
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md
Depends-On: .10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md

# P3 C3: DataFusion, Python, and native parallelism integration

## Scope

Join DataFusion task execution, codec/native internal threads, DuckDB lanes, Python GIL/free-threaded modes, and subprocess/FFI work to shared CPU-slot admission; eliminate hidden oversubscription while preserving semantic equivalence.

## Acceptance criteria

- Profiles prove DataFusion/native work does not bypass claimed CPU authority.
- GIL builds interleave safely; free-threaded builds parallelize; packages/evidence match.
- Native thread settings and lane affinity are capability-driven and observable.
- CPU-bound paths saturate effective cores without runaway runnable threads/context switches.

## Evidence expectations

Thread/CPU profiles, DataFusion hook/confinement tests, Python build matrix, native library settings, context switches, cancellation/panic, and dependency review.

## Explicit exclusions

No distributed DataFusion/Ballista or WASM implementation.

## Blockers

None. C2 and A4 are complete.

## Assumptions

- Record-backed: `cpu_slot_cost` is the host-visible CPU demand of one task and `native_internal_parallelism` is the total native worker demand within that task. Admission therefore reserves their maximum, not their product; the standalone host already enforces this algebra, while the scheduler currently diverges.
- Record-backed: production DataFusion/native/Python/FFI work must enter through neutral `ExecutionServices`; engine orchestration may join capabilities but must not branch on source or destination identity.
- Record-backed: the P3 lab and C4 own broad roofline/scaling matrices. C3 owns the admission/accounting integration and focused saturation/oversubscription evidence needed to make those matrices trustworthy.

## Journal

- 2026-07-14 activation: C2 closed with 537 affected-graph tests and a fresh pass review. Re-ranking the P3 graph selected C3 because it is the sole remaining dependency of C4, which gates the permanent jobs matrix and multiple performance-envelope closeouts.
- 2026-07-14 inspection: the neutral host, shared CPU-slot pool, declared blocking lanes, DataFusion memory bridge, Python GIL/free-threaded detection, and capability-driven source executor classes already exist. Two concrete gaps remain at activation: scheduler source admission multiplies `cpu_slot_cost * native_internal_parallelism` while the actual host reserves `max`, and DataFusion resource execution ignores its task context and has no explicit CDF CPU-admission evidence. The implementation will reconcile the algebra once in `cdf-runtime`, then join DataFusion/native/foreign execution through capabilities rather than driver-name branches.
- 2026-07-14 boundary correction: registered format decode was still submitted as I/O work and `resolve_decode_unit_concurrency` capped every codec at the host's four default I/O workers. Added one compiled `CpuTaskSpec` to the format descriptor, a neutral asynchronous CPU-future submission/stream surface, and one canonical decode-unit path for both one and many units. Parquet, CSV, JSON/NDJSON, and Arrow IPC now run on the fixed CPU executor; their concurrency joins claimed CPU slots, source usefulness, units, and managed memory without an I/O-worker ceiling or single-unit bypass. The host and scheduler now share `max(cpu_slot_cost, native_internal_parallelism)` through one helper.
- 2026-07-14 focused proof: the new host test keeps an asynchronous Parquet-shaped task on `cdf-cpu-*` before and after a Tokio timer await and reports the declared two-slot native demand. The scheduler regression proves a 12-slot host admits six two-slot decode units despite four I/O workers. The 478-test affected library graph passed with seven intentional release/slow ignores; affected all-target check and strict Clippy also passed.
- 2026-07-14 measured production comparison: controlled warm local FineWeb (2,147,509,487-byte Parquet, 1,059 row groups, 1,058,640 rows) to DuckDB completed in 6.04 seconds after C3 versus 6.12 seconds at exact parent commit `3a5e1802`; additive decode time fell from 2.958 to 2.812 seconds and source-read time from 0.853 to 0.724 seconds. The end-to-end gain is intentionally modest because segment encode and the single-writer destination dominate this workload. The fixed async CPU path measured 1.059x the synchronous fixed path in a separate release run while both remained bounded to 18 workers/slots; the process reported zero voluntary and 1,212 involuntary context switches across all three 1,152-task comparison arms. Evidence: `.10x/evidence/2026-07-14-p3-c3-shared-cpu-admission.md`.
- 2026-07-14 scope reconciliation: CDF's identity-bearing package path remains native by the active DataFusion identity guardrail, so there is no hidden DataFusion execution pool to admit in C3. DataFusion analysis/query adapters retain their engine-owned task context while CDF resources enter their injected host; J5 owns future `ExecutionPlan` marshaling/metrics. Managed Python already selects `python.gil` concurrency one or `python.free_threaded` bounded by host slots, with the strict 3.14/3.14t identical-hash matrix closed under P1 WS7C; H2 owns replacing materialized Python output with an incremental boundary. Subprocess workers remain H3. These are explicit ticket boundaries, not compatibility paths.
- 2026-07-14 reproducibility audit: the A4 executor-comparison source existed under `crates/cdf-benchmarks/src/bin/` but was accidentally ignored by the repository's broad `bin/` rule and never tracked, despite the A4 evidence calling it committed. C3 narrows the root-only ignore to `/bin/`, adds the real runner, and extends it with the async CPU arm used above. This removes the cause rather than force-adding another invisible benchmark.
- 2026-07-14 adversarial correction: fresh review falsified the claim that the first async CPU implementation was work-conserving. It used `block_on` inside a fixed worker, so an awaited transport, memory lease, or backpressured output could retain both a worker and its CPU slots. Replaced it with wake-driven single-poll admission: every poll is queued onto the fixed executor, and `Pending` releases the worker and slot claim immediately. A gated two-worker regression proves a fully pending async frontier cannot starve a runnable two-slot task.
- 2026-07-14 DataFusion correction: the table-provider adapter no longer accepts or creates an execution path without `ExecutionServices`. DataFusion still schedules and polls the returned Arrow stream, but CDF resource opening, source polling, Arrow conversion, projection, and limit slicing execute inside a bounded CDF CPU stream keyed by the DataFusion session/task context. Focused execution records every mock-source poll on `cdf-cpu-*`; all nine runnable DataFusion-focused tests passed, with one intentional release-only ignore. The remaining closure gap is actual GIL/free-threaded concurrent-work evidence, not another engine fallback.
- 2026-07-14 Python matrix correction: centralized the production interpreter-mode-to-lane mapping in `python_execution_lane_spec` and replaced the synthetic-report/precomputed-hash test with actual work on the standalone host. The GIL 3.14.6 build admitted two Python computations through `python.gil`, observed peak one, and emitted `sha256:2531d1b7e36c1752d42882279bf23d157c4cc9a64e5234eace480951b7c993b3`. A separately linked uv CPython 3.14.6t build admitted the same work through `python.free_threaded`, observed peak two, and emitted the identical hash. The targeted CI matrix now runs this real admission test and is triggered by changes to the executor implementation as well as the Python boundary.
- 2026-07-14 failure-path proof: wake-driven async CPU futures now have direct cancellation and panic regressions. Cancellation wakes an indefinitely pending future, joins it, and reports the cancelled task without leaking its two-slot claim. A panic during an async poll becomes the named internal error, and a subsequent two-slot scope completes on the same host, proving slot recovery.
- 2026-07-14 consolidated verification: `cargo test -p cdf-runtime -p cdf-engine -p cdf-python --lib --locked` passed 239 tests with seven intentional release/slow ignores (151+21+67 runnable). Workspace all-target check, strict affected Clippy, formatting, lockfile use, and diff hygiene passed with `CARGO_BUILD_JOBS=12`. Broad performance-envelope work remains with C4 and the format/destination owners; C3 evidence is specifically bounded admission, work conservation, semantic identity, and the measured FineWeb non-regression.

## References

- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/specs/execution-host-structured-runtime.md`
- `.10x/decisions/standalone-cpu-executor-v1.md`
- `.10x/evidence/2026-07-14-p3-c3-shared-cpu-admission.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws7c-python-interpreter-ci-matrix.md`
- `.10x/tickets/2026-07-11-p3-h2-python-incremental-arrow-boundary.md`
- `.10x/tickets/2026-07-11-p3-h3-subprocess-stream-supervision.md`
- `.10x/tickets/2026-07-12-p3-j5-execution-plan-marshaling-metrics.md`
