Status: open
Created: 2026-07-11
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/remote-local-io-overlap.md

# P3 G1: async streaming local/HTTP/cloud byte sources

## Scope

Implement injected async `ByteSource` providers for local, HTTP(S), S3, GCS, Azure; streaming bodies/listings; pooled clients; generation/precondition/reattest; typed retries; and migrate private runtime/mutex/Vec APIs through compatibility adapters.

## Acceptance criteria

- No production transport constructs a runtime or materializes full list/response solely for its API.
- Every ranged/parallel read is generation-bound; weak identity chooses sequential spool or fails.
- Millions-entry listing remains bounded/deterministic.
- Secret/egress/redaction/retry/cancellation laws pass across providers.
- Mock transport addition requires no source/format/scheduler branch.

## Evidence expectations

Recorded provider fixtures, generation-change/conditional request tests, high-cardinality RSS, connection reuse, dependency review, static runtime gates, and before/after transport benchmarks.

## Explicit exclusions

No adaptive range controller or codec optimization.

## Blockers

Depends on L5, SX1, FX1, and A4.

## References

- `.10x/decisions/generation-bound-overlapped-io.md`
- `.10x/research/2026-07-11-remote-io-overlap-audit.md`
- `.10x/specs/remote-local-io-overlap.md`
- `.10x/specs/datafusion-currency-bridges.md`

## Progress and notes

- 2026-07-11: Implemented the first native provider, `LocalByteSource`, directly against the neutral FX1 contract. Sequential chunks and independent exact ranges reserve before allocation, transfer `Vec` into lease-owned `Bytes` without copying, execute on the injected I/O runtime, honor cancellation, and pre/post reattest strong Unix path/device/inode/size/mtime/ctime generation. Neutral content identity now records strength; weak providers cannot enter Parquet random-access mode. The leaf test graph also dropped its `cdf-engine`/DataFusion dependency in favor of a focused neutral test host. Production registry composition, remote providers, listings, and synchronous-facade deletion remain open. Evidence: `.10x/evidence/2026-07-11-p3-g1-local-accounted-byte-source.md`.
- 2026-07-11: Removed the process-wide file-transport mutex. `FileTransport` and `HttpFileTransport` are now concurrency-safe shared contracts (`Send + Sync` with `&self` operations); Reqwest, object-store, local, and recording implementations conform without engine/source branches. This deletes false serialization but does not yet replace blocking Reqwest with the final async byte-source provider. Evidence: `.10x/evidence/2026-07-11-p3-g2-concurrent-transport-spool.md`.
- 2026-07-11: Removed the parallel false lock from REST. Neutral `cdf-http::HttpTransport` is now a shared `Send + Sync` contract, `RestRuntimeDependencies` owns `Arc<dyn HttpTransport>` directly, discovery/runtime/CLI/conformance/benchmark consumers use shared references, and only mutable auth-refresh/fixture queues retain narrow interior locks. The permanent concurrency law reaches two simultaneous requests through one dependency; source, HTTP policy, and real project REST execution pass. Native async response streaming remains open. Evidence/review: `.10x/evidence/2026-07-11-p3-g1-concurrent-rest-transport.md`, `.10x/reviews/2026-07-11-p3-g1-concurrent-rest-transport-review.md`.
- 2026-07-12: J2 will adapt G1's final injected provider registry into DataFusion sessions. G1 remains transport authority: DataFusion may not create a parallel credential/client/generation path, and adding a provider must not require an engine match branch.
- 2026-07-12: Added the first remote neutral provider, `ObjectStoreByteSource`, behind `FileTransport::open_byte_source`. Untransformed S3/GCS/Azure objects now stream or serve exact ranges directly to any registered format with ledger admission, cancellation, response range/length checks, ETag `If-Match`, and provider-version selection. ETag and version are distinct authorities throughout planning/attestation. Direct Arrow IPC and ranged Parquet complete with a one-byte spool budget; generation mutation fails. HTTP async bodies, paged bounded listing, retries/telemetry, and live cloud fixtures remain open. Evidence/review: `.10x/evidence/2026-07-12-p3-g1-object-store-byte-source.md`, `.10x/reviews/2026-07-12-p3-g1-object-store-byte-source-review.md`.
- 2026-07-12: Extracted Reqwest into `cdf-transport-http` and added strong-ETag async HTTP sequential/range byte sources with pooling, ledger admission, exact response/identity checks, cancellation, and redirect rejection. Weak HTTP keeps verified spool. A new neutral `FormatSourceAccess` join sends sequential formats direct while adaptive full scans (Parquet) retain one verified spool; selective range mode awaits propagated pushdown evidence. HTTP auth/retry/telemetry/live fixtures and paged listing remain. Evidence/review: `.10x/evidence/2026-07-12-p3-g1-async-http-byte-source.md`, `.10x/reviews/2026-07-12-p3-g1-async-http-byte-source-review.md`.
- 2026-07-12: Strong HTTP/object-store adaptive full scans now carry the byte source into the I/O scope and build their verified spool asynchronously with shared spill reservation and hash/length checks. The synchronous transport download API remains only for weak/legacy providers and transformed compatibility paths. Evidence/review: `.10x/evidence/2026-07-12-p3-g2-accounted-async-full-scan-spool.md`, `.10x/reviews/2026-07-12-p3-g2-accounted-async-full-scan-spool-review.md`.
