Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: HTTP/object-store sequential transfer boundary, remote Parquet full-scan routing, and file-manifest batch slicing
Verdict: pass

# HTTP Parquet sequential spool review

## Findings

- No critical or significant correctness finding remains in this slice. Full/unknown Parquet execution selects the transport's sequential-transfer capability; discovery still owns bounded ranges. The format crate no longer exposes the superseded unconditional range-execution entry points.
- Strong HTTP identities apply `If-Match`; transferred length and returned identity are verified. Weak HTTP identities use one sequential transfer followed by metadata reattestation and reject change. Object-store streaming applies the planned ETag precondition and writes incrementally through the injected I/O host.
- The slice-position relaxation is deliberately restricted to `FileManifest`. It does not invent intermediate cursor/log/page positions, and permanent tests preserve that refusal.

## Residual risk

The current spool budget is a checked logical disk ceiling rather than a preallocated filesystem reservation. Object version IDs are still represented through the existing transport identity model. These are existing G1/G2 contract tails, not reasons to retain the pathological Parquet execution path. B2 still owns removal of collected `FormatRead` batches and selective row-group execution.
