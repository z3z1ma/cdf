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
- `cargo test -p cdf-runtime decode_unit_concurrency_joins_unit_cpu_io_source_and_memory_bounds --locked -- --nocapture` passed. A 12-slot/four-I/O-worker host admits six two-slot decode units when CPU is limiting, proving I/O-worker count is no longer a codec-CPU ceiling.
- `cargo test -p cdf-runtime -p cdf-engine -p cdf-source-files -p cdf-project -p cdf-format-parquet -p cdf-format-json -p cdf-format-delimited -p cdf-format-arrow-ipc --lib --locked` passed 478 tests with seven intentional release/slow ignores.
- Affected all-target `cargo check`, strict `cargo clippy ... --all-targets --locked -- -D warnings`, formatting, and diff hygiene passed with `CARGO_BUILD_JOBS=12`.

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

## Architecture boundaries

- DataFusion may analyze/query/schedule but does not produce CDF identity-bearing package bytes. The production package path therefore has no hidden DataFusion pool; real CDF resource opens still enter injected execution services. J5 owns future DataFusion `ExecutionPlan` marshaling and unified metrics.
- Managed Python already declares either a one-worker `python.gil` lane or a `python.free_threaded` lane bounded by host CPU slots. P1 WS7C owns identical deterministic fixture hashes across the 3.14/3.14t matrix; H2 owns the remaining incremental boundary and its throughput measurements.
- DuckDB/Postgres/native adapter work remains on capability-declared lanes. No source/destination identity branch or new dependency was introduced.

## What this supports

The evidence supports closing C3's shared-admission integration and activating C4's permanent jobs-invariance/scaling matrix. It proves removal of a generic four-core codec ceiling, consistent nested-native accounting, bounded async CPU execution across waits, no regression in the measured production path, and the absence of a new runtime/dependency island.

## Limits

The FineWeb case is an end-to-end single-file/single-writer workload, not a CPU-only codec roofline; its small wall gain does not establish aggregate Parquet, JSON, or multi-partition scaling. Phase durations are additive operation telemetry and can exceed wall time under overlap. `/usr/bin/time` context-switch counters cover process startup and all three executor arms together. C4 must run jobs 1/2/N invariance and scaling; B2/B5/G3/F4 retain their format, overlap, and constant-memory envelope targets.
