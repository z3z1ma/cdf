Status: recorded
Created: 2026-07-13
Updated: 2026-07-13

# FX1 benchmark registry-path migration

## Observation

`cdf-benchmarks` no longer directly depends on or calls the monolithic `cdf-formats` facade. Its CSV, JSON, NDJSON, and Parquet package workloads now compile an ordinary declarative file resource, resolve a benchmark-scoped explicit `FormatRegistry`, prepare declared physical-schema evidence through the project compiler front end, and execute the resolved `cdf-source-files::FileResource`.

The former `trend.cdf_arrow_ipc_stream_to_package.medium` case was not a native Arrow IPC stream-driver measurement. It eagerly decoded through `cdf-formats::read_arrow_ipc_stream`, materialized the result as an in-memory resource, and then labeled the downstream package step as the CDF path. That case and its now-orphaned `MemoryResource::from_batches` helper were deleted. The coverage matrix remains explicitly deferred to P3 B3 rather than presenting a compatibility reader as current performance evidence.

The startup file-to-DuckDB benchmark also now resolves the same registry-backed file runtime and compiler observation evidence. This repaired a stale direct `CompiledResource` execution path that failed after the dependency-free file-runtime shim was removed.

## Procedure

```text
rg -n "cdf[-_]formats|read_arrow_ipc_stream|FileFormat\b|FileSource\b" crates/cdf-benchmarks -g 'Cargo.toml' -g '*.rs'
```

No monolithic-format dependency or compatibility reader remains in `cdf-benchmarks`; the only `FileFormat`-like name is the unrelated `ExternalFileFormat` reference-runner vocabulary.

```text
CARGO_BUILD_JOBS=12 cargo check -p cdf-benchmarks --all-targets
```

Passed. The first run rebuilt the DataFusion-heavy benchmark graph and completed in 4m16s.

```text
CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks \
  --test fixtures --test lab_runners -- --test-threads=1
```

Passed: 7 fixture tests and 8 lab-runner tests. The registry-backed prepared worker and the startup end-to-end case both produced observed measurements. A parallel test invocation exposed the known harness contention behavior in the 5-second isolated-reference timeout; the product-path cases passed, and serial execution supplied uncontended evidence without changing test assertions or timeout policy.

## What it supports or challenges

This supports FX1's requirement that the performance lab measure current registry-selected codec execution rather than a closed compatibility dispatcher. It also removes one direct parser-aggregation dependency edge and one false benchmark claim.

It challenges any claim that `cdf-formats` is ready for deletion. `cdf-benchmarks` still links it transitively through the REST source path, and direct consumers remain in REST, subprocess, and conformance property/fuzz code. Those migrations and the project-level remote external-codec law remain open.

## Limits

This does not implement the native Arrow IPC stream driver, remove the remaining `cdf-formats` crate, change benchmark timeout policy, prove remote external-codec composition, or measure a before/after throughput ratio.
