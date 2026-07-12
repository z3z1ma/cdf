Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-source-files/src/object_store_byte_source.rs; crates/cdf-source-files/src/transport.rs; crates/cdf-source-files/src/runtime.rs; crates/cdf-kernel/src/position.rs
Verdict: concerns

# Object-store byte-source review

## Findings

- No critical/high finding: source bytes are generation-bound, accounted, cancellation-aware, and format-neutral; direct execution preserves the existing manifest and reconciliation pipeline.
- Significant, fixed before verdict: provider version was previously folded into ETag. The implementation now carries version independently through metadata, planning, observation binding, attestation, and `GetOptions::version`.
- Significant: `object_store` controls streaming response chunk sizes. CDF pre-admits the requested chunk envelope and fails if a provider emits a larger chunk; provider-specific live fixtures and telemetry must prove this bound before G1 closure.
- Significant: object-store listing remains `try_collect::<Vec<_>>()` followed by sort, violating G1's million-entry constant-memory criterion.
- Significant: HTTP has no async byte-source provider and continues through the correct but non-overlapped sequential spool.

## Verdict

Concerns raised. The implementation is a coherent, safe G1 increment and removes object-store full-spool execution, but G1 remains open for the listed provider/listing/HTTP work and performance evidence.

## Residual risk

Cloud-provider differences in ETag/version semantics, response chunking, and retries are covered only by the in-memory object-store fixture in this slice. Nightly live provider conformance remains mandatory.
