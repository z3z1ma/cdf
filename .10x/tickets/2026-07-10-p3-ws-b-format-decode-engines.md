Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/2026-07-07-rest-json-to-arrow-performance-triage.md

# P3 WS-B: format decode engines

## Scope

Make each source format stream into Arrow efficiently: row-group-parallel Parquet with pushdown, chunk-parallel CSV where safe, tape-based JSON/NDJSON, streamed gzip/zstd windows, and REST CPU-pool page decode. Preserve fail-closed decoding, P2 schema reconciliation, residual capture, and deterministic partition output.

Split by format and shared decompression seam before implementation. Any `simd-json` evaluation is research/dependency-gate work, not an assumed addition.

## Acceptance criteria

- No production row-format path requires full decompressed input or whole-page DOM materialization except bounded discovery samples.
- Parquet projection/predicate pushdown and deterministic row-group concurrency meet the envelope.
- Malformed-input property/fuzz tests prove no partial accepted batch escapes.
- Every changed engine has same-harness before/after evidence.

## Blockers

Blocked until WS-L baseline evidence exists.
