Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# P3 G2 generation-bound object-store streaming

## Observation

`ObjectStoreByteSource::open_sequential` previously implemented strong-generation sequential reads as a series of exact ranged GETs, one per preferred chunk. This contradicted the source API, inflated high-BDP latency, and made the new source-I/O request metric under-report physical provider requests.

Strong and weak S3/GCS/Azure byte sources now open one full-object `get_opts` stream. Strong requests carry ETag/version preconditions and verify returned generation metadata before publishing body chunks. Weak requests verify initial metadata and reattest with terminal `HEAD`. Content-addressed objects hash the one stream and verify the declared SHA-256. Provider frames reserve the shared source-memory ledger before polling, are rejected above the declared 32 MiB provider envelope, and are zero-copy sliced to the caller's smaller preferred chunk size while retaining the complete frame lease. Empty generations are verified by the same conditioned GET without issuing an invalid zero-length range.

The superseded `ExactSequentialState` range loop was deleted; selective reads still use the explicit generation-bound exact-range path.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files object_store_byte_source -- --nocapture`
  - Passed five focused tests covering streamed plus selective reads on one generation, empty provider frames under one lease, caller-owned downstream chunk shape, generation precondition failure, and mutated empty-object rejection.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files`
  - Passed all 46 source-file tests and doc tests.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-files --all-targets -- -D warnings ...`
  - Passed with allowances only for the known upstream workspace lint set inherited from kernel/project.

## What this supports or challenges

This removes a provider-specific latency multiplier from every full object-store scan and restores the invariant that one sequential byte-source invocation represents one physical provider request. It preserves exact ranged reads for oversized seekable objects and selective scans, so the disk-constant fallback remains available without making ranges the unconditional full-scan strategy.

## Limits

The object-store dependency controls raw response-frame allocation before CDF sees it. CDF admits and rejects frames against a 32 MiB envelope immediately on receipt and retains exact ledger ownership thereafter, but transport-specific conformance still needs live S3/GCS/Azure profiles and cancellation/throttle chaos. Adaptive gap prefetch and per-origin BDP feedback remain open in G2.
