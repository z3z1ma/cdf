Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-11-p3-g1-streaming-transport-byte-sources.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-b2-parquet-codec.md

# Generation-bound object-store byte source

## What was observed

`FileTransport` now has one provider-neutral `open_byte_source` hook. `FileTransportFacade` implements it for local/file and S3/GCS/Azure object-store resources; unknown/custom transports return `None` and retain the verified spool fallback. No format or provider match was added to runtime decode.

`ObjectStoreByteSource` streams bodies and exact ranges through the neutral accounted `ByteSource` contract. It reserves before polling each response chunk or issuing an exact-range body collection, pins ETag with `If-Match`, pins provider versions through `GetOptions::version`, validates response range/length/generation, reattests weak sequential generations, honors cancellation, and exposes exact-range concurrency only for enforceable generations. The former metadata model that collapsed provider version into ETag was deleted; `FilePosition`, plan bindings, and partition attestation now preserve both authorities separately.

Untransformed object-store objects no longer require a local spool. Registered Arrow IPC streams directly and Parquet performs generation-bound exact ranges with a one-byte spool budget, proving the spool path was not used. Local inputs also enter the same generic driver function as a `ByteSource` rather than being reconstructed from a path inside it.

## Procedure

- `cargo test -p cdf-source-files` — 26 tests passed, including direct object stream/range, generation mutation rejection, direct remote Arrow IPC, direct remote Parquet, transformed spool fallback, HTTP policy, local identity, and extension laws.
- `cargo clippy -p cdf-source-files -p cdf-declarative -p cdf-project -p cdf-cli --all-targets -- -D warnings`
- Direct remote Arrow IPC and Parquet fixtures set `max_spool_bytes = 1`; both objects exceed that bound and complete, establishing that execution used the byte-source provider.

## What this supports

Remote provider behavior is now a transport extension point consumed uniformly by every registered format. Object-store random access cannot join different generations, and sequential readers have constant ledger-controlled buffering.

## Limits

This is not G1 closure. HTTP still uses the blocking verified-spool adapter; object-store listing still materializes and sorts all results; response retry/telemetry and live S3/GCS/Azure fixtures remain. Compressed remote inputs still stage through source and transformed spools pending B1/G3 capability-driven transform streaming.

## Subsequent architecture correction

The next G1 slice added typed `FormatSourceAccess`. The direct Parquet fixture in this observation proved generation-bound ranges, but production full/unknown adaptive scans now deliberately select the verified sequential spool; only a future plan with demonstrated selective pushdown may select remote ranges. Sequential formats continue to use direct object-store streams.
