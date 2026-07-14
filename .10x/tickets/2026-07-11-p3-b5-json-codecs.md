Status: active
Created: 2026-07-11
Updated: 2026-07-13
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-07-rest-json-to-arrow-performance-triage.md

# P3 B5: tape-based NDJSON, JSON, and REST page decode

## Scope

Replace production DOM/full-byte JSON paths with streamed tape/SIMD-class physical Arrow decoding for NDJSON, JSON document selectors, and REST pages on the CPU executor; retain bounded DOM only for discovery flexibility.

## Acceptance criteria

- Compressed/uncompressed NDJSON and JSON do not materialize full inputs/documents.
- REST I/O overlaps CPU decode without blocking the I/O executor.
- Fatal windows publish no partial accepted batch; recoverable record errors preserve exact quarantine/residual evidence.
- Depth/token/record limits, selector framing, random rechunking, and jobs are deterministic.
- JSON meets the 3x-current and aggregate envelope targets with dependency evidence.

## Evidence expectations

arrow-json/simd candidate comparison, dependency gate, malformed/fuzz corpus, selector/REST parity, compressed profiles, memory, and reference goldens.

## Explicit exclusions

No XML/MessagePack/CBOR parsing.

## Blockers

Depends on transforms, FX1, L5, and absorbs the REST triage.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`

## Progress and notes

- 2026-07-12: Added `cdf-format-json::NdjsonFormatDriver` using Arrow JSON's incremental tape decoder directly over accounted `ByteSource` chunks. Discovery is bounded and no-copy across retained source chunks; decode is incremental with pre-admitted output leases and 64k/byte targets. Product composition, local discovery, and gzip object-store discover/pin/run are green; the old source NDJSON fallback is fail-closed. The ticket remains open for JSON documents, malformed/fuzz/oversized-row memory proof, row-local quarantine parity, and the 3x/300–500 MB/s envelope.
- 2026-07-12: Added `JsonDocumentFormatDriver`: a bounded, 256-depth state machine streams a top-level object or array of objects into the same Arrow tape decoder, preserves nesting/string delimiters across arbitrary chunk boundaries, and charges framing output to the ledger. Bounded discovery stops only after complete records and reports consumed source bytes. The legacy JSON execution/discovery paths and the final generic source decoder fallback were deleted. B5 remains open for pull-based/spill-backed discovery inference, selectors, REST, oversized-record admission, fuzz/property and quarantine/residual parity, and throughput evidence. Evidence/review: `.10x/evidence/2026-07-12-p3-b5-streaming-json-document-driver.md`, `.10x/reviews/2026-07-12-p3-b5-streaming-json-document-driver-review.md`.
- 2026-07-13: Current registry-resolved MVP drift conformance reproduced the open row-local parity blocker exactly: mixed NDJSON containing two declared UTF-8 `event_type` values and one numeric `event_type: 42` aborts `Decoder::flush` with `expected string got 42`. The superseded monolithic reader converted this mismatch into a pre-contract residual candidate so conforming values continued and the offending value was governed. The native streaming driver must regain that behavior without DOM/full-file buffering: fatal framing remains fail-closed, while record-local type mismatches emit exact residual/quarantine evidence and do not poison conforming records in the same input window.
- 2026-07-13: Restored row-local drift semantics without reviving the DOM decoder. The Arrow tape fast path now runs strict so unknown/type-conflicting fields cannot disappear; it retains accounted source-chunk spans without copying and invokes a bounded, ledger-admitted raw-value recovery only for a failed window. Recovery replays conforming values, nulls only the typed projection backed by an exact residual candidate, preserves scalar types and raw nested JSON bytes, rejects duplicate fields, and records global source-row ordinals. `AccountedPhysicalBatch` now includes pre-contract evidence arrays in its retained-memory charge. The exact DuckDB drift fixture proceeds through package, quarantine, dedup, commit, receipt, and checkpoint; its stale v1 inline-dedup assertion was replaced with the v2 Parquet provenance reader.
- 2026-07-14: SA2's final transport review confirmed file metadata now uses the shared asynchronous execution host, but the concrete Reqwest provider still lazily owns a blocking client solely for the synchronous REST surface. This ticket's existing requirement that REST I/O overlap CPU decode owns removing that final blocking client/runtime and making the provider async end to end; it must not survive B5 closure as a compatibility shim.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-json -j 12`: 4 passed, including mixed unknown/numeric drift, exact residual values, typed null projection, and zero retained ledger bytes after release.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion -j 12 -- --nocapture`: 1 passed; the original `expected string got 42` failure is gone and the full governed run closes.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime physical_batch_retains_its_memory_lease_after_entering_kernel_stream -j 12`: 1 passed.
- `CARGO_BUILD_JOBS=12 cargo check -p cdf-format-json -p cdf-runtime -p cdf-kernel -j 12`: passed.

## Review

- Self-review found no destination/source identity branch and no return of the superseded full-file decoder. Normal records remain on Arrow's tape decoder; only drift windows pay recovery cost. Residual evidence is now part of the same memory authority as the physical batch. Remaining ticket scope is unchanged: byte-bound oversized-record admission, selectors/REST, fuzz/rechunking, and measured throughput.
