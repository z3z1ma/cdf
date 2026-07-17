Status: active
Created: 2026-07-11
Updated: 2026-07-16
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-07-rest-json-to-arrow-performance-triage.md

# P3 B5: tape-based NDJSON, JSON, and REST page decode

## Scope

Replace production DOM/full-byte JSON paths with streamed tape/SIMD-class physical Arrow decoding for NDJSON, JSON document selectors, and REST pages on the CPU executor; retain bounded DOM only for bounded discovery flexibility, and implement explicit `full_content` discovery through constant-memory streaming inference rather than enlarging the bounded collector.

## Acceptance criteria

- Compressed/uncompressed NDJSON and JSON do not materialize full inputs/documents.
- REST I/O overlaps CPU decode without blocking the I/O executor.
- Fatal windows publish no partial accepted batch; recoverable record errors preserve exact quarantine/residual evidence.
- Depth/token/record limits, selector framing, random rechunking, and jobs are deterministic.
- Explicit `full_content` discovery observes every record/value with fixed memory, records truthful full-content evidence, and crosses each selected source invocation once through SA3's retained-window/continuation handoff; bounded discovery remains the default.
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
- `.10x/specs/schema-discovery-and-stream-admission.md`

## Progress and notes

- 2026-07-12: Added `cdf-format-json::NdjsonFormatDriver` using Arrow JSON's incremental tape decoder directly over accounted `ByteSource` chunks. Discovery is bounded and no-copy across retained source chunks; decode is incremental with pre-admitted output leases and 64k/byte targets. Product composition, local discovery, and gzip object-store discover/pin/run are green; the old source NDJSON fallback is fail-closed. The ticket remains open for JSON documents, malformed/fuzz/oversized-row memory proof, row-local quarantine parity, and the 3x/300–500 MB/s envelope.
- 2026-07-12: Added `JsonDocumentFormatDriver`: a bounded, 256-depth state machine streams a top-level object or array of objects into the same Arrow tape decoder, preserves nesting/string delimiters across arbitrary chunk boundaries, and charges framing output to the ledger. Bounded discovery stops only after complete records and reports consumed source bytes. The legacy JSON execution/discovery paths and the final generic source decoder fallback were deleted. B5 remains open for pull-based/spill-backed discovery inference, selectors, REST, oversized-record admission, fuzz/property and quarantine/residual parity, and throughput evidence. Evidence/review: `.10x/evidence/2026-07-12-p3-b5-streaming-json-document-driver.md`, `.10x/reviews/2026-07-12-p3-b5-streaming-json-document-driver-review.md`.
- 2026-07-13: Current registry-resolved MVP drift conformance reproduced the open row-local parity blocker exactly: mixed NDJSON containing two declared UTF-8 `event_type` values and one numeric `event_type: 42` aborts `Decoder::flush` with `expected string got 42`. The superseded monolithic reader converted this mismatch into a pre-contract residual candidate so conforming values continued and the offending value was governed. The native streaming driver must regain that behavior without DOM/full-file buffering: fatal framing remains fail-closed, while record-local type mismatches emit exact residual/quarantine evidence and do not poison conforming records in the same input window.
- 2026-07-13: Restored row-local drift semantics without reviving the DOM decoder. The Arrow tape fast path now runs strict so unknown/type-conflicting fields cannot disappear; it retains accounted source-chunk spans without copying and invokes a bounded, ledger-admitted raw-value recovery only for a failed window. Recovery replays conforming values, nulls only the typed projection backed by an exact residual candidate, preserves scalar types and raw nested JSON bytes, rejects duplicate fields, and records global source-row ordinals. `AccountedPhysicalBatch` now includes pre-contract evidence arrays in its retained-memory charge. The exact DuckDB drift fixture proceeds through package, quarantine, dedup, commit, receipt, and checkpoint; its stale v1 inline-dedup assertion was replaced with the v2 Parquet provenance reader.
- 2026-07-14: SA2's final transport review confirmed file metadata now uses the shared asynchronous execution host, but the concrete Reqwest provider still lazily owns a blocking client solely for the synchronous REST surface. This ticket's existing requirement that REST I/O overlap CPU decode owns removing that final blocking client/runtime and making the provider async end to end; it must not survive B5 closure as a compatibility shim.
- 2026-07-14: SA3 review found `full_content` was representable in discovery evidence but not by a format driver, while the current NDJSON discovery collector retains every sampled chunk and therefore cannot honestly scan giant inputs under constant memory. The runtime capability now admits `FormatDiscoveryKind::FullContent` and preserves one live source invocation; this ticket owns the remaining operator configuration and streaming inference engine. It MUST NOT implement full content by setting byte/record bounds to object size or materializing all chunks.
- 2026-07-14: Live `redpajama.documents` cold discovery exposed an envelope-boundary defect: a final accounted transform chunk that straddled the exact content-sampling limit caused NDJSON to reject the entire candidate. Added a neutral byte-limited view over retained accounted chunks; NDJSON and CSV inference now see exactly the configured prefix while the full upstream allocation stays ledger-owned and SA3's capture spool preserves already-fetched continuation bytes for same-command extraction. This removes codec assumptions about producer chunk alignment without copying or weakening the byte bound.
- 2026-07-14: The live RedPajama extraction then proved row-only batching could build a 152.9 MiB Arrow outcome behind a 16 MiB lease. NDJSON/JSON now request a flush when either the row target or configured input-byte target is reached, split arbitrary transport chunks at the first complete record boundary after that target, retain incomplete records across chunks, and pre-admit the output share of the driver's declared 64 MiB total decode working set. This removes the 64k-row-only accumulation without making chunk boundaries semantic or deadlocking a tight ledger. The rebuilt CLI loaded 26,545 rows from the live compressed RedPajama object into nine 8.7–11 MiB segments, finalized an 83 MiB package, verified the DuckDB receipt, and committed its checkpoint in 8.11 wall seconds. B5 still owns adaptive observed output-byte feedback and clean oversized-single-record admission before closure.
- 2026-07-16: Closed the oversized-record allocation hole and adaptive-feedback residual without adding source-runtime knowledge. Both JSON drivers now canonicalize explicit plan evidence for record limits; the JSON-document driver also records its nesting limit. The ratified default is 16 MiB per record, configurable through `format_options.maximum_record_bytes` up to 32 MiB; that upper bound fits a declared 96 MiB worst-case decoder working set while preserving the 64 MiB ordinary path. Input is rejected with both remedies before Arrow receives an over-limit window, so a fatal window publishes no partial batch. Healthy NDJSON boundary search now uses `memchr`'s vectorized implementation. Each accepted batch feeds its already-computed accounted retained bytes back into a deterministic input-window controller; expansion shrinks the next window and compact records recover up to, but never beyond, the plan's byte target. The controller performs no second Arrow memory walk.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-json --lib -j 12`: 6 passed, including byte-target flushing across an input chunk split inside a JSON string, mixed unknown/numeric drift, exact residual values, typed null projection, and zero retained ledger bytes after release.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance drift_quarantine_duckdb_conformance_asserts_unsupported_mirror_exclusion -j 12 -- --nocapture`: 1 passed; the original `expected string got 42` failure is gone and the full governed run closes.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime physical_batch_retains_its_memory_lease_after_entering_kernel_stream -j 12`: 1 passed.
- `CARGO_BUILD_JOBS=12 cargo check -p cdf-format-json -p cdf-runtime -p cdf-kernel -j 12`: passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-json --lib --locked -j 12`: 9 passed; new assertions cover canonical limit evidence, pre-publication oversized-record failure with zero retained ledger bytes, and deterministic adaptive feedback.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-format-json --all-targets --locked -j 12 -- -D warnings`: passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --lib --locked -j 12`: 75 passed and the intentional million-entry slow gate remained ignored; registry compilation, local/remote JSON-family streaming, retained discovery continuation, compression, and transport boundaries stayed green with explicit canonical codec limits.

## Review

- Self-review found no destination/source identity branch and no return of the superseded full-file decoder. Normal records remain on Arrow's tape decoder; only drift windows pay recovery cost. Residual evidence is now part of the same memory authority as the physical batch. Remaining ticket scope is unchanged: byte-bound oversized-record admission, selectors/REST, fuzz/rechunking, and measured throughput.
- 2026-07-16 slice review: the record-limit policy is format-plan data carried by the decode session, not a generic runtime branch. The ordinary 16 MiB path retains its prior 64 MiB reservation; only an explicitly larger record limit raises the admitted working set. Feedback reuses `AccountedPhysicalBatch`'s reconciled lease instead of remeasuring Arrow buffers. Remaining B5 closure scope is selector framing, asynchronous REST ingestion, constant-memory full-content inference, rechunk/fuzz conformance, and envelope measurement.
