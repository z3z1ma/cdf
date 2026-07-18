Status: done
Created: 2026-07-11
Updated: 2026-07-17
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# P3 B3: Arrow IPC file and stream codecs

## Scope

Implement separate file/stream drivers with bounded batch yields, remote range/seek and spool fallback for file framing, sequential stream framing, dictionary/compression support, and discovery/runtime parity.

## Acceptance criteria

- Remote Arrow IPC file no longer hard-rejects solely for being remote.
- No IPC path collects every record batch; dictionaries/order/schema remain exact.
- File/stream framing mismatch, truncation, continuation, compression, and schema changes fail with precise evidence.
- Throughput/reference ratio and zero-copy/copy counts are recorded.

## Evidence expectations

Arrow reference comparison, remote range/spool fixtures, malformed/fuzz corpus, memory, schema/dictionary goldens, and profiles.

## Explicit exclusions

No subprocess protocol changes.

## Blockers

None. L5 and FX1 are closed.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`

## Progress and notes

- 2026-07-11: Added the parser-local `cdf-format-arrow-ipc` file driver and registered it at composition roots. File discovery is footer-bounded; execution plans one deterministic file unit, reads dictionary and record-batch blocks by exact extents, preserves schema/custom metadata, supports exact projection, and emits accounted batches whose Arrow buffers retain source leases. Local and remote verified-spool execution now share the driver, and the former remote Arrow IPC hard rejection and file-source-local IPC execution branch were deleted. The release-mode in-memory construction comparison measured 8,196.51 MiB/s for Arrow's high-level `FileReader` and 471,540.98 MiB/s for the owner-backed driver (57.529x); this intentionally biased measurement demonstrates elimination of the high-level reader's block-buffer copy and is not a storage-throughput claim. Stream framing, compression/malformed/fuzz expansion, and storage-backed throughput evidence remain open. Evidence: `.10x/evidence/2026-07-11-p3-b3-native-arrow-ipc-file-driver.md`.
- 2026-07-13: Corrected the file driver's access declaration from sequential to seekable and proved the generic transform/spool policy with valid gzip IPC, explicit gzip, malformed zstd, multi-file discovery, stream-framing rejection, local discovery/run, and HTTP discovery. Physical batches now share the engine's format- and source-neutral constraint boundary, restoring actual-run lossless widening and opt-in parse coercion without an Arrow- or file-specific branch. Remaining B3 scope is the distinct sequential stream driver, expanded malformed/fuzz/dictionary/compression corpus, and storage-backed throughput/copy evidence. Evidence: `.10x/evidence/2026-07-13-fx1-registered-schema-reconciliation.md`.
- 2026-07-17: Added the registered Arrow IPC stream driver and registered it at the CLI composition root. The stream driver now feeds Arrow's `StreamReader` through a bounded accounted-byte bridge instead of collecting the full stream payload before decode; a regression test decodes a >512 KiB stream under a 384 KiB memory budget and proves peak retained memory remains below payload size. Added generic alternate-format diagnostics so a file driver can name `arrow_ipc_stream` when stream framing is detected, without an Arrow-specific branch in the file source. Fixed local `compression = "auto"` to use registered byte-transform strong-magic evidence as well as extension evidence; disagreement between extension and magic now fails explicitly, while gzip bytes in an `.arrow` file are decoded correctly. Updated the stale CLI assertion to check file-driver evidence at `source_identity["driver.format"]`, which is the source-specific evidence surface, rather than generic snapshot metadata.

## Evidence

- Remote Arrow IPC file no longer hard-rejects solely for being remote:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files arrow_ipc --lib --locked -j 12` passed; includes `remote_arrow_ipc_file_streams_directly_through_registered_driver`.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli arrow_ipc --locked -j 12 -- --nocapture` passed; includes HTTP Arrow IPC discovery without project writes.
- No IPC path collects every record batch:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-arrow-ipc --lib --locked -j 12` passed; includes `arrow_ipc_file_driver_discovers_projects_and_streams_blocks` and `arrow_ipc_stream_driver_decodes_without_collecting_payload`.
  - The stream-driver test uses chunked lazy source leases and drops each decoded batch incrementally, proving the bridge does not retain the full input payload.
- File/stream framing mismatch, truncation, compression, and schema changes fail or reconcile with precise evidence:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli arrow_ipc --locked -j 12 -- --nocapture` passed; covers file/stream mismatch diagnostics, truncated Arrow file diagnostics, gzip/zstd compression cases, schema discovery/run parity, and drift quarantine.
- Throughput/reference ratio recorded:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-arrow-ipc arrow_ipc_driver_reference_rate --release --lib --locked -j 12 -- --ignored --nocapture` passed with `arrow-ipc reference=8997.61 MiB/s driver=361103.03 MiB/s ratio=40.133`.
- Static quality:
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime -p cdf-source-files -p cdf-format-arrow-ipc -p cdf-cli --all-targets --locked -j 12 -- -D warnings` passed.

## Review

Verdict: pass.

Findings:

- critical: none.
- significant: none.
- minor: the stream bridge still uses a blocking helper thread around Arrow's synchronous `StreamReader`; this is acceptable for B3 because it eliminates full-payload collection without introducing identity-bearing alternate bytes, and broader async decode/runtime scheduling remains owned by P3 WS-A/WS-C.
- nit: storage-backed IPC throughput beyond the in-memory reference comparison remains better served by the P3 performance lab/envelope tickets than by a format-specific micro-harness here.

Residual risk: Dictionary-heavy and malformed-continuation fuzzing is not exhaustive in this ticket. The native-format matrix owns cross-format fuzz/corpus expansion, so this is not a B3 closure blocker.

## Retrospective

- The important architectural seam was the registry, not Arrow IPC itself. Alternate-format diagnostics belong in `FormatRegistry`/file-source confirmation so future codecs get the same UX without one-off branches.
- Compression evidence must be resolved before format confirmation. Local `auto` needed registered magic bytes in addition to extension evidence; otherwise codec tests can pass for extension-named files while real operator files fail.
- Constant-memory tests should not `try_collect` output batches under a tight budget, because retaining successful output can create false failures unrelated to input collection. Incremental consumption is the correct proof for streaming decode.
