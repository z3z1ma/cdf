Status: active
Created: 2026-07-11
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

# P3 H2: incremental Python Arrow/row boundary

## Scope

Replace `PythonBatchRead`/C-stream materialization in production with the neutral incremental producer, live memory/backpressure/cancellation/lane integration, real Arrow C lifetime/copy evidence, adaptive direct row conversion, shared reconciliation, and GIL/free-threaded measurement.

## Acceptance criteria

- Python resources can exceed memory by orders of magnitude while resident boundary memory remains budgeted.
- Real PyArrow C Array/Stream paths cover type/lifetime/error/cancellation cases and verified copy labels.
- Dict rows retain only one accounted conversion window and are never whole-resource JSON collections.
- Schema variance uses the shared contract policy; no Python-local competing schema truth remains.
- GIL/free-threaded concurrency uses declared execution-host lanes and produces deterministic fixed-input packages.

## Evidence expectations

Real interpreter/PyArrow matrices, memory traces, copy probes, backpressure/cancellation/error tests, package hashes, before/after throughput, and adversarial FFI/lifetime review.

## Explicit exclusions

No arbitrary Python execution inside engine operators or untrusted sandbox claim.

## Blockers

None. H1, A4, and A2 are done; this ticket is executable.

## References

- `.10x/specs/foreign-stream-interop.md`

## Journal

- 2026-07-18 — Activated after H1 closeout. First slice migrated the internal Python iterator emission boundary onto IX1 `ForeignBatchOutcome` values. Existing materializing collectors now wrap the neutral outcome stream instead of owning a second Python-local semantics, and production `PythonResource::execute_stream` consumes neutral outcomes before sending ordinary runtime batches.
- 2026-07-18 — Removed the now-dead `visit_python_iterable` internal wrapper instead of suppressing dead-code warnings. The remaining internal boundary is `visit_python_foreign_iterable`; compatibility materialization still exists only through public collector APIs and remains to be burned down in later H2 slices.
- 2026-07-18 — Migrated direct JSON/dict-row helpers to the same neutral outcome visitor. `batches_from_json_dict_rows` is now a collector wrapper over `visit_json_dict_rows`; dict-row conversion has one batch-to-foreign-outcome conversion path shared with Python iterator execution.
- 2026-07-18 — Bounded audit of `pyo3-arrow 0.19.0` found import/export C Data surfaces and buffer-protocol comments, but no ready-made allocation/copy-proof signal. H2 must keep Arrow C outcomes `copy_unknown` until a real pointer/lifetime/allocation probe is implemented; do not infer zero-copy from use of the crate alone.
- 2026-07-19 — Removed the production `PythonBatchRead`, `BoundaryChannel`, DLT batch collectors, and deterministic batch-vector hash helpers. The bridge now emits one neutral outcome at a time and returns metadata-only `PythonStreamSummary` values; capped collection exists only in tests. Python compiled plans now retain the shared effective-schema runtime and policy allowances, and differing physical schemas cross as separate observations for ordinary engine reconciliation instead of failing in a Python-local one-schema rule.
- 2026-07-19 — Made dict compatibility conversion directly incremental over one NDJSON byte window, including the transient serialized row and decoded Arrow allocation in the boundary peak. The byte knob can flush a window before the row-count target, while exponential bounded capacity growth avoids a reallocation per row. `PythonStreamSummary.peak_boundary_bytes` exposes the observed retained peak. Objects implementing both Arrow protocols now take bounded C Array import first; tables/readers remain incremental C Stream imports.
- 2026-07-19 — Added a dedicated ignored PyArrow 25 matrix so fast checks do not acquire a heavyweight Python dependency. Against real PyArrow 25.0.0 on CPython 3.12, C Array and C Stream imports retained aliasing producer/imported buffer ranges across primitives, strings/slices, lists, structs, dictionary arrays, decimal128, and timezone timestamps; imported values survived producer deletion/GC; mixed C Array schemas remained distinct observations; stream callback cancellation stopped after one producer pull; and an exception between stream batches propagated after exactly one emitted outcome. Production labels remain conservatively `copy_unknown`; the matrix records the verified PyArrow cells without adding pointer-probe overhead to the hot path.
- 2026-07-19 — Made the two boundary controls compiled source inputs: `dict_batch_rows` and `max_boundary_bytes`. Both reject zero, serialize into the physical plan and redacted compiled options, inform execution capabilities and memory admission, and name both remedies when a producer batch exceeds the configured window. There is no hard product ceiling; the 64 MiB byte value is a configurable default. Direct project references use the same defaults, while source declarations can tune both values.
- 2026-07-19 — Measured the dict compatibility curve in release mode over one million two-field rows: 1,024 rows/window took 472 ms (2.117 M rows/s, 977 outcomes, 68,894-byte peak); 8,192 took 466 ms (2.142 M rows/s, 123 outcomes, 549,150-byte peak); 65,536 took 464 ms (2.154 M rows/s, 16 outcomes, 4,391,198-byte peak). Ratified 8,192 as the default because it removes 87.4% of downstream outcomes for a measured 1.2% throughput gain without paying the 8x retained-window increase of 65,536 for only another 0.6%. The one-million-row constant-memory case retained less than 8 MiB while total emitted bytes exceeded the peak by more than 100x.
- 2026-07-19 — Repaired a registry-boundary regression exposed by the Python product tests: lazily compiled direct project references disappeared from `cdf doctor` resource counts. `SourceRegistry::health_checks` now receives generic configured references, resolves their owning driver, deduplicates them with compiled plan ids, and gives every driver the same configured-resource inventory. The CLI contains no Python-specific counting branch. Compiler-time Python reference handling no longer validates execution-host interpreter options; bounded health and resolution remain the authorities for execution-environment compatibility.
- 2026-07-19 — Completed the interpreter matrix. CPython 3.12/GIL used `GilSerialized` with effective parallelism 1 and observed peak 1; CPython 3.14.6 free-threaded used `FreeThreadedParallel` with effective parallelism 2 and observed peak 2. Both produced fixture hash `sha256:a8882bfa14934b40fbdc4ac36b8eefe0139bc78a3666d40a15c23a65ccb46f65`. Full CLI plan→preview→run→replay passed under both runtimes and both produced canonical package-segment data hash `sha256:6203b3b40a1af3c6f6220032a1f79dfecce2040644321569ef3f1a92c6d34e06`. Full manifest hashes are not the comparison authority because receipt/lifecycle artifacts vary by run.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-python bridge --lib --locked -j 12` — passed. Covers neutral foreign outcome emission for dict rows and verifies downstream stop still halts the Python generator before exhaustive materialization.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-python direct_dict_rows_emit_neutral_outcomes_without_collecting_all_batches --lib --locked -j 12` — passed. Confirms direct dict-row compatibility exits through the neutral outcome visitor and can stop after the first bounded row window.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-python --lib --tests --locked -j 12 -- -D warnings` — passed. This caught and forced removal of the superseded internal batch wrapper.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-python --all-targets --locked -j 12` — passed: 24 fast tests, 3 deliberate PyArrow evidence tests ignored.
- `PYTHONPATH=$PWD/target/cdf-python/pyarrow-cp312-site CARGO_BUILD_JOBS=12 cargo test -p cdf-python real_pyarrow --lib --locked -j 12 -- --ignored` — passed all 3 real PyArrow 25.0.0 C Array/Stream lifetime, alias, cancellation, error, type, and schema-variance cases.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-python --all-targets --locked -j 12 -- -D warnings` — passed after replacing over-wide constructors with the existing `CompiledSourcePlanInput`/`CompiledSourcePlan` parameter authorities rather than adding an H2-specific argument bag.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-python million_row_dict_stream_keeps_boundary_memory_constant --release --lib --locked -j 12 -- --ignored --nocapture` — passed for one million rows with more than 900 incremental outcomes and peak boundary retention below the configured 8 MiB window; cumulative output exceeded peak retention by more than 100x.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-python dict_row_batch_curve_reports_throughput_without_changing_defaults --release --lib --locked -j 12 -- --ignored --nocapture` — passed and produced the 1K/8K/64K throughput/outcome/retention curve recorded in the Journal.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli python --lib --locked -j 12` — passed all 11 Python front-door, doctor, plan, preview, run, and replay tests. In particular, a configured-but-lazily-compiled Python reference again causes a precise doctor failure when the interpreter is absent.
- `PYO3_PYTHON=.../python3.14t CARGO_TARGET_DIR=target/h2-cp314t CARGO_BUILD_JOBS=12 cargo test -p cdf-python admitted_python_work_is_mode_correct_and_fixture_hash_stable --lib --locked -j 12 -- --nocapture` — passed under CPython 3.14.6 free-threaded with effective parallelism and observed overlap 2; the fixture hash exactly matched the CPython 3.12/GIL run.
- `PYO3_PYTHON=.../python3.14t PYTHONPATH=$PWD/target/cdf-python/pyarrow-site CARGO_TARGET_DIR=target/h2-cp314t CARGO_BUILD_JOBS=12 cargo test -p cdf-python real_pyarrow --lib --locked -j 12 -- --ignored` — passed all 3 PyArrow 25.0.0 lifetime, alias, cancellation, error, type, and schema-variance cells under the free-threaded interpreter.
- `CDF_PYTHON_PACKAGE_DATA_HASH_OUTPUT=... cargo test -p cdf-cli python_resource_plan_preview_run_and_replay_use_the_product_spine --lib --locked -j 12` under the ordinary CPython 3.12 build and the isolated CPython 3.14.6 free-threaded build — both passed full plan→preview→run→replay and emitted identical canonical segment-data hash `sha256:6203b3b40a1af3c6f6220032a1f79dfecce2040644321569ef3f1a92c6d34e06`.

## Review

Implementation complete; adversarial review pending. Production materializing APIs and Python-local schema authority are gone, real PyArrow C Array/Stream matrices pass under GIL and free-threaded interpreters, one-million-row retention is constant and budgeted, tuning is compiled and configurable, and canonical segment data is identical across interpreter modes. Verified PyArrow alias cells remain evidence-only so no per-batch introspection was added to the production hot path.

## Retrospective

The production resource path was already closer to the target than the ticket text suggested: it streamed batches through the injected blocking lane. The architecture gap was that the internal bridge still spoke Python-local `Batch`/`PythonYieldKind` rather than the neutral foreign contract. Migrating the internal emission point first kept FFI/copy proof source-agnostic and made deletion of the old collectors straightforward. Two review-oriented checks paid for themselves: real producer buffer ranges prevented an unearned zero-copy product claim, and compiling the same fixture against an installed free-threaded interpreter found the true lane behavior without mocks. The batch curve also reinforced the performance rule: expose byte/row knobs, choose the default from measured marginal gain, and do not turn a default into a hard cap. The only unrelated-looking failure was a real generic source-registry regression, and fixing configured-resource inventory at that boundary avoided a Python-specific CLI branch.
