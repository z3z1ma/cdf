Status: open
Created: 2026-07-11
Updated: 2026-07-11
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

## Progress and notes

- 2026-07-11: Removed the serialized 8 MiB range-loop spool. `FileTransport` now exposes one sequential transfer into the bounded spool; HTTP streams one GET with `If-Match` where available and weak identities reattest, while object stores stream through the injected I/O host. Full/unknown remote Parquet execution uses this path. Controller feedback, preallocation, cache, and high-BDP range work remain open. Evidence: `.10x/evidence/2026-07-11-http-parquet-sequential-spool-and-positioned-slicing.md`.
- 2026-07-11: Removed the remaining global transport mutex and moved remote partition validate/spool/decode forwarding into one injected I/O scope per partition. Engine jobs can now overlap independent HTTP/object-store opens; each remote forwarding edge is bounded to two batches, while local native files retain their direct stream with no added channel. Shared transport concurrency reaches two simultaneous callers in the permanent test, all 17 source tests pass, and the real HTTP-Parquet project fixture passes. Adaptive BDP/range/cache policy and the fully async HTTP provider remain open. Evidence/review: `.10x/evidence/2026-07-11-p3-g2-concurrent-transport-spool.md`, `.10x/reviews/2026-07-11-p3-g2-concurrent-transport-spool-review.md`.
- 2026-07-11: Follow-up review found that creating the remote scope inside the returned open future could let an immediately ready first `FuturesOrdered` entry delay polling later admissions. Remote scope creation now occurs eagerly during `ResourceStream::open`, so filling C2's bounded frontier starts every admitted remote transfer; the returned future only exposes the already-started bounded stream. Source and real HTTP-Parquet tests plus strict lint remain green.
