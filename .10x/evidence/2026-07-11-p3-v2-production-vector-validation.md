Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md, .10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md

# V2 production vector-validation milestone

## What was observed

Engine preview and package execution now create one `VectorValidationEvaluator` per run/preview. The evaluator caches the schema-bound plan and rebinds only when the Arrow schema changes, preserving multi-observation physical-provenance behavior without an unbounded schema cache. Both fused no-residual execution and residual-present execution call this evaluator. The scalar evaluator remains in `cdf-contract` solely as a differential oracle; a production architecture test scans the engine execution source and forbids its call token.

## Procedure

- `cargo test -p cdf-contract --lib` — 83 passed, two explicit performance tests ignored.
- `cargo test -p cdf-engine --lib` — 91 passed, four explicit performance/stress tests ignored.
- `cargo clippy -p cdf-contract -p cdf-engine --all-targets -- -D warnings` — passed.
- `CDF_A5_FUSION_BENCH_ITERATIONS=200 cargo test --release -p cdf-engine fused_transform_hot_path_benchmark --lib -- --ignored --nocapture` — unfused `2.183 GiB/s`, fused `15.658 GiB/s`, `7.171x` fused/unfused ratio. The prior recorded fused baseline for this same 64k/200-iteration benchmark was `3.912 GiB/s`.

## What this supports or challenges

This proves that the V1 kernel is on ordinary production preview/run paths and that no scalar validation loop remains callable from engine execution. Existing fused/unfused package identity, quarantine, residual, reject, freshness, source-name, effective-schema, jobs/segment, and phase-telemetry tests remain green.

## Limits

V2 is not closed. Selected quarantine candidates are still materialized as a batch-wide vector before the package writer transcodes them through an in-memory Parquet byte buffer. The transform lease is conservative but does not yet give exact ownership to those allocations. V2/D4 must stream selected evidence through an accounted Parquet sink and add high-failure spill/memory proof. Macro TLC/package profiles also remain.
