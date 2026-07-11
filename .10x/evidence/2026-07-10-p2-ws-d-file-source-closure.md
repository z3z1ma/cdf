Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md

# P2 WS-D file-source closure evidence

## Observation

Local, HTTP-template, and object-store globs plan deterministic per-file logical partitions. Append runs aggregate and commit `FileManifest` positions, skip unchanged identities, load changed/new identities, and report a fast no-op. Gzip/zstd are incrementally decoded/spooled under explicit budgets; binary format inference confirms magic; schema variance produces evolve/freeze verdicts and file quarantine rather than crashes. Preview uses the same resolved partition and front-end authority as run.

Large-N optimization is resolved as executor task packing over unchanged logical partitions. Zip is explicitly unsupported until archive-member identity and ledger/security prerequisites exist.

## Procedure

- `cargo test -p cdf-conformance p2_ --locked`: 9/9 passed with S2/S3/S8 covered.
- Exact S2 production-path CLI test passed in the H6 verification session.
- Full CLI suite (271 tests) and strict CLI lint passed after the last file-source-integrating changes.
- Child evidence records D1-D5 plus A10/E3-E6/G2-G3/I2-I5 provide focused manifest, compression, discovery, schema-variance, diagnostics, and parity proof.

## Limits

P3 owns channel-level constant-memory decoding and executor task packing. Those performance mechanisms may not alter the P2 logical partition/manifest contract.
