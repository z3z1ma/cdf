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
- 2026-07-12: Strong injected local/HTTP/object-store sources now compose with any registered transform before format execution. Compressed sequential formats stream through without disk, while adaptive transformed formats retain one accounted output spool. Provider and transform additions still require no generic runtime match branch. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-streaming-transform-product-composition.md`, `.10x/reviews/2026-07-12-p3-b1-streaming-transform-product-composition-review.md`.
- 2026-07-12: Registered remote discovery now uses injected byte sources directly and has no Parquet/project branch. A bounded range adapter covers only strong older transports with reserve-before-read and pre/post generation reattestation; final native providers remain authoritative and G1 owns deleting the adapter after migration. Evidence/review: `.10x/evidence/2026-07-12-fx1-registry-driven-remote-discovery.md`, `.10x/reviews/2026-07-12-fx1-registry-driven-remote-discovery-review.md`.
- 2026-07-12: Regressed HTTP Parquet full-scan spooling failed with `byte payload requires 0 accounted bytes but lease holds 4194304`. Root cause was the strong-generation HTTP provider passing a legal empty Reqwest/hyper transport frame into the nonempty accounted graph envelope added by commit `682ed316`; the original loopback fixture emitted only one nonempty Content-Length body. HTTP and object-store sequential providers now retain one pre-admitted receive lease while polling past empty provider frames, emit only the first nonempty payload, and drop the lease at true EOF. `AccountedBytes` and zero-byte lease invariants remain strict. Focused synthetic provider tests own the empty-empty-data-EOF sequence, exact transferred bytes, one-window peak, and zero residual ledger bytes: `CARGO_BUILD_JOBS=12 cargo test -p cdf-transport-http http_sequential_source_skips_empty_transport_frames_under_one_lease --locked` and `cargo test -p cdf-source-files object_store_sequential_source_skips_empty_provider_frames_under_one_lease --locked` each passed 1/1. The existing `remote_parquet_full_scan_uses_verified_sequential_spool` integration law also passed 1/1 after the repair. Fresh review pending in this active repair slice.
- 2026-07-12 fresh adversarial review of the empty-frame repair: **Findings:** no significant or critical findings. Both providers reserve once before body polling, retain that lease across any number of provider-produced empty frames, check cancellation around polling/publication, exclude empty frames from transfer counts and `AccountedBytes`, reconcile a nonempty payload to its exact length, and release on EOF/error by RAII. HTTP EOF checks the exact planned length; object-store EOF preserves exact length/identity verification and the weak-generation `HEAD` reattestation. The focused fixtures exercise empty-empty-3-byte-data-EOF, require the published lease to reconcile to 3 bytes, require exactly one 4 MiB peak, and require zero current bytes after EOF; root reports both focused tests and the HTTP Parquet regression law passed. `AccountedBytes::new` and `MemoryLease::reconcile` still reject zero-byte payloads. **Verdict:** pass for this repair slice. **Residual risk:** the fixtures use synthetic streams and therefore do not establish which live Reqwest/hyper or cloud-provider versions emit empty frames; existing G1 live-provider/chunk-bound conformance remains open, but no repair-specific closure blocker was found.
