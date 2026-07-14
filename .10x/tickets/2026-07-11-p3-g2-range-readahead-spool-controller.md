Status: open
Created: 2026-07-11
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md
Depends-On: .10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md, .10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

# P3 G2: per-origin range, readahead, spool, and cache controller

## Scope

Implement accounted per-origin connection/range admission, low-gain BDP feedback, coalescing/readahead, exact range retries, bounded spool with early decode where safe, and opt-in cryptographic cache promotion/revalidation.

## Acceptance criteria

- High-BDP fixtures saturate without unbounded buffers; throttle/pressure reduces concurrency cleanly.
- Coalescing/prefetch never changes logical bytes/order and reports waste.
- Spool/cache disk/security/identity/cleanup laws pass; cache state cannot change packages.
- Fixed/auto/manual settings are observable and outside package identity.

## Evidence expectations

Controller simulations, high-BDP/throttle benchmarks, memory/disk/cancel chaos, cache/spool identity/security tests, and network profiles.

## Explicit exclusions

No format-specific range planning.

## Blockers

Depends on G1, C1, and memory ledger.

## References

- `.10x/specs/remote-local-io-overlap.md`
- `.10x/specs/schema-discovery-and-stream-admission.md`

## Progress and notes

- 2026-07-11: Removed the serialized 8 MiB range-loop spool. `FileTransport` now exposes one sequential transfer into the bounded spool; HTTP streams one GET with `If-Match` where available and weak identities reattest, while object stores stream through the injected I/O host. Full/unknown remote Parquet execution uses this path. Controller feedback, preallocation, cache, and high-BDP range work remain open. Evidence: `.10x/evidence/2026-07-11-http-parquet-sequential-spool-and-positioned-slicing.md`.
- 2026-07-11: Removed the remaining global transport mutex and moved remote partition validate/spool/decode forwarding into one injected I/O scope per partition. Engine jobs can now overlap independent HTTP/object-store opens; each remote forwarding edge is bounded to two batches, while local native files retain their direct stream with no added channel. Shared transport concurrency reaches two simultaneous callers in the permanent test, all 17 source tests pass, and the real HTTP-Parquet project fixture passes. Adaptive BDP/range/cache policy and the fully async HTTP provider remain open. Evidence/review: `.10x/evidence/2026-07-11-p3-g2-concurrent-transport-spool.md`, `.10x/reviews/2026-07-11-p3-g2-concurrent-transport-spool-review.md`.
- 2026-07-11: Follow-up review found that creating the remote scope inside the returned open future could let an immediately ready first `FuturesOrdered` entry delay polling later admissions. Remote scope creation now occurs eagerly during `ResourceStream::open`, so filling C2's bounded frontier starts every admitted remote transfer; the returned future only exposes the already-started bounded stream. Source and real HTTP-Parquet tests plus strict lint remain green.
- 2026-07-12: Added the first explicit access-policy join. Format drivers declare sequential/seekable/adaptive access; sequential HTTP/object-store formats stream directly, while adaptive full/unknown scans use one verified spool. This prevents capability availability from accidentally selecting pathological ranges. Selectivity propagation, controller feedback, retries, cache, and overlap remain open. Evidence/review: `.10x/evidence/2026-07-12-p3-g1-async-http-byte-source.md`, `.10x/reviews/2026-07-12-p3-g1-async-http-byte-source-review.md`.
- 2026-07-12: Migrated strong-generation full-scan spooling from synchronous transport download to the injected async byte source. The spool reserves the shared spill budget before transfer, streams accounted chunks with async writes, hashes/verifies once, and retains disk authority through decode. Growing-spool overlap, weak providers, transform staging, chaos, cache, and controller feedback remain. Evidence/review: `.10x/evidence/2026-07-12-p3-g2-accounted-async-full-scan-spool.md`, `.10x/reviews/2026-07-12-p3-g2-accounted-async-full-scan-spool-review.md`.
- 2026-07-12: Removed provider-input plus transformed-output double spooling for injected sources. Unknown transformed lengths now grow the shared spill reservation before disk writes; sequential transformed codecs avoid the spool entirely, while adaptive codecs retain one bounded transformed-output spool. Growing-spool decode overlap remains open. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-streaming-transform-product-composition.md`, `.10x/reviews/2026-07-12-p3-b1-streaming-transform-product-composition-review.md`.
- 2026-07-13: The fixed-schema discovery/stream-admission model makes G2's cache/spool controller the payload-reuse owner, not schema discovery. A materially downloaded schema observation must continue through the same live stream or exact spool; a small unspooled bounded probe may be reread within its recorded budget. Cache keys include source generation plus codec/options/normalizer/contract identity. G2 must expose this reusable source session without embedding schema or format policy.
- 2026-07-13: The corrected lifecycle permits rereading a small unspooled bounded probe but requires same-command reuse of any fully downloaded/decompressed payload spool. G2 owns the neutral reusable spool/session handoff; schema discovery owns neither transport caching nor format-specific extraction.
