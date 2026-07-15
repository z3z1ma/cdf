Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 G2 source-I/O controller telemetry

## Observation

One invocation-local, transport-neutral byte-source wrapper now records source wait time, logical bytes, physical bytes, and physical request count for sequential, exact-range, and coalesced-range reads. File execution publishes the snapshot only at its EOF completion barrier. The engine records it as `source_read` phase telemetry with physical bytes as input, logical bytes as output, and physical requests as operations. The CLI names physical/logical bytes, derived prefetch waste, request count, and sub-second duration explicitly.

The measurement path is outside plan, package, manifest, and source-position identity. Package identity remains unchanged with phase metrics enabled. Sequential measurement accumulates only time awaiting open/body polls and excludes consumer backpressure between polls. Concurrent range waits may overlap, so their accumulated wait time is work time rather than partition wall time.

The same change consolidated duplicated local/remote, compressed/uncompressed source opening behind one file byte-source boundary. Hashing, transforms, full/growing spools, range fallback, and codecs all consume the resulting neutral source without destination, format, or provider dispatch in orchestration.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime observed_byte_source -- --nocapture`
  - Passed. A sequential read plus two overlapping logical ranges preserved exact payloads and reported 18 logical bytes, 16 physical bytes, two physical requests, and zero waste.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel partition_completion_evidence_is_eof_bound_and_single_use -- --nocapture`
  - Passed. Correctness evidence and operational metrics remain inaccessible before EOF and single-use at completion.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine phase_telemetry_is_additive_and_preserves_manifest_identity -- --nocapture`
  - Passed. Telemetry does not change manifest identity, package hash, or signature.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project general_project_run_records_bounded_complete_phase_telemetry -- --nocapture`
  - Passed. A real file-resource run emitted nonzero `source_read` physical/logical bytes and request count through the project ledger.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli source_read_metric_names_physical_logical_waste_and_requests -- --nocapture`
  - Passed. Rendering names physical/logical/waste/request semantics and retains sub-second precision.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel -p cdf-runtime -p cdf-engine -p cdf-source-files`
  - Passed: kernel 38; runtime 47 plus one ignored benchmark; engine 125 plus six ignored slow/release tests; source-files 46; build-graph and doc tests passed.
- Strict Clippy for kernel/runtime/engine/source-files/project/CLI passed with explicit allowances only for five pre-existing workspace findings (`needless_lifetimes`, `too_many_arguments`, `manual_noop_waker`, `large_enum_variant`, `items_after_test_module`).

## What this supports or challenges

This supplies the controller's missing neutral observations without making Parquet, HTTP, object stores, or file sources part of engine policy. It exposes the evidence needed to distinguish range latency, coalescing reuse, prefetch waste, and sequential transfer behavior, and it makes redundant local hash-sweep I/O visible rather than hiding it inside decode bytes.

## Limits

This slice does not yet implement adaptive BDP feedback, gap prefetch, retry/throttle counters, cache promotion, monotone spool eviction, or bounded rolling replay spools. Failed or intentionally partial invocations do not publish EOF-bound source metrics. The G2 ticket remains active.
