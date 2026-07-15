Status: recorded
Created: 2026-07-14
Updated: 2026-07-14
Relates-To: .10x/tickets/2026-07-11-p3-c3-engine-ffi-parallel-integration.md, .10x/decisions/standalone-cpu-executor-v1.md

# P3 C3 shared CPU admission

## Observation

Registered file codecs previously executed through the host I/O stream and their decode-unit concurrency included `io_workers` as a ceiling. On the standard 18-logical-CPU host, the default four-thread I/O reactor therefore limited Parquet, CSV, JSON/NDJSON, and Arrow IPC decode to four units even though CPU and memory admitted more work.

The corrected path gives every format driver a compiled `CpuTaskSpec`, submits every decode unit through the fixed CPU executor, and uses the same claimed-slot algebra in scheduling and execution: `max(cpu_slot_cost, native_internal_parallelism)`. The I/O reactor continues driving transport futures; CPU-dominant future polling and native decode remain on bounded `cdf-cpu-*` workers. One-unit and multi-unit sessions use the same canonical frontier.

## Procedure and results

Focused conformance:

- `cargo test -p cdf-engine asynchronous_cpu_work_stays_on_the_bounded_cpu_executor_across_awaits --locked -- --nocapture` passed. It records the thread name before and after an awaited host timer, requires both to be `cdf-cpu-*`, and observes two claimed slots for `cpu_slot_cost=1, native_internal_parallelism=2`.
- `cargo test -p cdf-engine pending_cpu_futures_release_workers_and_slots_for_runnable_work -- --nocapture` passed. Two async futures occupy the complete logical frontier while pending on gates; a later runnable task claiming every CPU slot completes before either gate is released. This specifically guards against fixed-worker/slot retention across `Pending`.
- `cargo test -p cdf-engine datafusion_ -- --nocapture` passed nine runnable tests with one intentional release-only ignore. The registered-table test records every CDF resource-stream poll and requires `cdf-cpu-*`, proving DataFusion polling cannot move CDF opening/conversion/projection work outside shared admission.
- `cargo test -p cdf-engine pending_cpu_future_cancellation_wakes_and_joins_without_leaking_slots -- --nocapture` and `asynchronous_cpu_future_panic_is_reported_and_releases_slots` passed. They prove a pending task is awakened by scope cancellation and a panicking poll releases its slots for a subsequent full-capacity task.
- `cargo test -p cdf-runtime decode_unit_concurrency_joins_unit_cpu_io_source_and_memory_bounds --locked -- --nocapture` passed. A 12-slot/four-I/O-worker host admits six two-slot decode units when CPU is limiting, proving I/O-worker count is no longer a codec-CPU ceiling.
- `cargo test -p cdf-runtime -p cdf-engine -p cdf-source-files -p cdf-project -p cdf-format-parquet -p cdf-format-json -p cdf-format-delimited -p cdf-format-arrow-ipc --lib --locked` passed 478 tests with seven intentional release/slow ignores.
- Affected all-target `cargo check`, strict `cargo clippy ... --all-targets --locked -- -D warnings`, formatting, and diff hygiene passed with `CARGO_BUILD_JOBS=12`.
- After the review repairs, `cargo test -p cdf-runtime -p cdf-engine -p cdf-python --lib --locked` passed 239 tests with seven intentional release/slow ignores (151 engine, 21 Python, 67 runtime). `cargo check --workspace --all-targets --locked` also passed.
- After the second review repairs, the engine/runtime graph passed 220 runnable tests with seven intentional ignores (152 engine, 68 runtime); strict all-target Clippy passed. `cancellation_future_unregisters_its_unique_waiter_on_drop` proves independent registration and exact deregistration even when two futures share one waker. `slot_ineligible_head_does_not_occupy_worker_or_starve_eligible_tail` proves admission retains a two-slot ineligible head in the queue while a later one-slot task uses the available worker/slot.
- After the third review repairs, 21 focused standalone-host tests and two run-work/cancellation tests passed. `self_waking_cpu_future_repolls_only_after_release_and_publishes_last` crosses 64 immediate wake boundaries, requires peak usage to remain one slot, then joins with complete slot restoration, an empty fairness registry, and immediate host destruction. `mixed_cost_queue_bounds_bypasses_before_reserving_the_expensive_head` proves exactly eight one-slot bypasses before a two-slot head reserves capacity. `shared_slot_reservation_prevents_cross_lane_starvation` proves the same global bound between the CPU pool and a two-slot native lane. The scope supervisor independently rejects completion while usage accounting remains nonzero. The complete focused host set passed 20 consecutive runs after the final release-order correction.
- The consolidated post-repair command `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime -p cdf-engine -p cdf-python --lib --locked` passed 244 runnable tests with seven intentional ignores (155 engine, 21 Python, 68 runtime). Strict affected all-target Clippy and formatting passed immediately afterward.

Release executor comparison:

```text
logical CPUs                         18
tasks per arm                     1,152
SHA-256 rounds per task           2,048
fixed synchronous CPU path    11.067 ms
fixed async CPU path          11.717 ms
Tokio spawn_blocking path     10.136 ms
async / sync fixed ratio          1.059x
process user / real time       0.50 / 0.03 s
voluntary / involuntary switches  0 / 1,212
```

Command: `/usr/bin/time -l target/release/executor-compare` after `CARGO_BUILD_JOBS=12 cargo run -p cdf-benchmarks --bin executor-compare --release --locked`. The async arm deliberately crosses one wake boundary before the same CPU kernel.

Production before/after smoke used the exact 2,147,509,487-byte local FineWeb Parquet fixture in `/Users/alexanderbut/code_projects/tmp/cdf-perf`, a clean temporary project/state/destination per arm, the same 1,059 row groups and 1,058,640 rows, a warm filesystem cache, and `/usr/bin/time -l`. The baseline binary was built from parent commit `3a5e1802` in a detached worktree; the candidate was built from the C3 tree.

| Measure | Parent | C3 | Change |
|---|---:|---:|---:|
| End-to-end wall | 6.12 s | 6.04 s | -1.3% |
| Additive source-read phase | 0.853 s | 0.724 s | -15.1% |
| Additive decode phase | 2.958 s | 2.812 s | -4.9% |
| Package-execution wall | 5.823 s | 5.733 s | -1.5% |
| Peak RSS | 1.577 GB | 1.660 GB | +5.3% |

Both runs produced 115 canonical segments and a verified DuckDB receipt/checkpoint. Segment encode and single-writer destination ingress dominate the remaining wall time; C4 and the destination/package workstreams own their scaling envelope.

Managed Python admission was executed against two separately linked CPython builds with the same two computations and Arrow fixture:

| Interpreter | Declared lane | Observed peak | Combined fixture/work hash |
|---|---|---:|---|
| CPython 3.14.6, GIL enabled | `python.gil` | 1 | `sha256:2531d1b7e36c1752d42882279bf23d157c4cc9a64e5234eace480951b7c993b3` |
| CPython 3.14.6t, GIL disabled | `python.free_threaded` | 2 | `sha256:2531d1b7e36c1752d42882279bf23d157c4cc9a64e5234eace480951b7c993b3` |

The GIL command used the default target and Homebrew interpreter. The free-threaded command used `uv python install 3.14t`, `PYO3_PYTHON=$(uv python find 3.14t)`, and an isolated `CARGO_TARGET_DIR=target/c3-python314t`, preventing PyO3 link-state reuse. `cmp` over the emitted files passed byte-for-byte.

## Architecture boundaries

- DataFusion may analyze/query/schedule but does not produce CDF identity-bearing package bytes. Its CDF table-provider adapter now requires injected `ExecutionServices`; CDF resource opening, polling, conversion, projection, and limit work run on the shared CPU executor while the DataFusion `TaskContext` remains the query-scheduling context. J5 owns future package-pipeline `ExecutionPlan` marshaling and unified metrics.
- Managed Python declares either a one-worker `python.gil` lane or a `python.free_threaded` lane bounded by host CPU slots through one production helper, and actual work now proves both modes plus identical evidence. H2 owns the remaining incremental boundary and its throughput measurements.
- DuckDB/Postgres/native adapter work remains on capability-declared lanes. No source/destination identity branch or new dependency was introduced.

## What this supports

The evidence supports C3's native-codec, DataFusion, and managed-Python admission criteria. It proves removal of a generic four-core codec ceiling, consistent nested-native accounting, work-conserving bounded async CPU execution across waits, cancellation/panic recovery, explicit DataFusion adapter confinement, GIL serialization, real free-threaded parallelism with identical output evidence, no regression in the measured production path, and the absence of a new runtime island.

## Limits

The FineWeb case is an end-to-end single-file/single-writer workload, not a CPU-only codec roofline; its small wall gain does not establish aggregate Parquet, JSON, or multi-partition scaling. Phase durations are additive operation telemetry and can exceed wall time under overlap. `/usr/bin/time` context-switch counters cover process startup and all three executor arms together. C4 must run jobs 1/2/N invariance and scaling; B2/B5/G3/F4 retain their format, overlap, and constant-memory envelope targets.
