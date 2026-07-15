Status: active
Created: 2026-07-11
Updated: 2026-07-14
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
- Full/growing spools admit only finite known lengths reserved in full before transfer; over-budget objects use generation-bound ranges or proven monotone prefix eviction, otherwise fail cleanly.
- Unbounded sources never enter the finite spool path; rolling replay retention is bounded and checkpoint-evicted under the stream-epoch owner.

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
- 2026-07-14: Live FineWeb execution followed Hugging Face's allowlisted CAS redirect and completed one 2,147,509,487-byte full scan without returning to range fan-out. End-to-end debug time was 92.58 seconds; extraction/package took about 90.8 seconds, DuckDB/final gate about one second, and an in-flight sample showed 2.0 GiB RSS while the package had reached 1.1 GiB. Raw sequential `curl` of the exact URL completed in 19.116 seconds at 112,341,173 bytes/s, so CDF's 4.84x wall ratio is not a network roofline. G2 still owns growing-spool decode overlap and transfer progress telemetry; B2 owns separating local decode/package cost from the now-measured download floor.
- 2026-07-14: Added the neutral strong-generation growing-spool substrate. It reserves the complete finite object length before transfer, exposes only already-written local ranges plus a bounded generation-bound tail window, joins transfer completion with partition completion, and leaves weak/unversioned sources on the verified full-spool path. The same slice removed hashing work when no source checksum can consume it. A deterministic gated-source test proves prefix/tail overlap, middle-range backpressure, one bounded upstream range, and zero retained memory/spill after completion. Live FineWeb evidence is `.10x/evidence/2026-07-14-p3-g2-fineweb-growing-spool-overlap.md`. Controller telemetry, adaptive tail/range policy, cache, large-object prefix eviction, and chaos remain open.
- 2026-07-14: Made full/growing spool admission an atomic policy choice rather than a mandatory prerequisite. If a strong, known-length, exact-range source exceeds the per-object spool ceiling or cannot reserve its complete length from the shared spill coordinator, execution retains the original generation-bound byte source and the registered format driver reads ranges directly. The same Parquet fixture now proves admitted spool, configured-over-budget fallback, and concurrent-budget-contention fallback with identical row counts and exact spill accounting. Weak and transformed inputs retain their safer paths. Evidence: `.10x/evidence/2026-07-14-p3-g2-large-object-range-fallback.md`. Coalescing, BDP feedback, readahead/waste telemetry, cache, and proven monotone eviction remain open.
- 2026-07-14: Moved exact-range batching behind the neutral `ByteSource` boundary. The default controller coalesces overlap/adjacency without extra egress, fans physical requests out to the source capability ceiling, restores codec-requested order, and reports logical bytes, physical bytes, and request count. Coalesced zero-copy slices retain one complete physical memory lease. Parquet now delegates instead of owning provider concurrency; a loopback HTTP fixture proves three logical extents become two generation-bound requests with exact output order and accounting. Evidence: `.10x/evidence/2026-07-14-p3-g2-transport-neutral-range-batching.md`. Gap-prefetch policy, adaptive BDP feedback, retry/telemetry, cache, and eviction remain open.

## Review

- 2026-07-14 growing-spool slice — adversarial self-review traced generation authority, disk admission, temporary-file lifetime, completion/error propagation, cancellation, notification races, checksum behavior, range fan-out, and the absence of format/destination identity branches. The first pass found two significant internal risks before commit: a `Notify::notify_waiters` race could miss terminal publication, and a byte-count mismatch could mark readers failed while returning successful transfer completion. The repair registers each waiter before inspecting progress and makes the completion invariant fail the joined source future. Focused tests and strict Clippy pass after the repairs. Verdict: **pass for this slice**. Residual risk is explicitly open: fixed tail tuning/telemetry, cancellation chaos, very-large-object eviction/ranges, cache, and the 3.09 GB FineWeb RSS envelope.
- 2026-07-14 large-object fallback slice — adversarial self-review checked the decision at the actual atomic reservation point, concurrent spill ownership, generation enforcement on every fallback read, transformed-byte safety, weak-provider behavior, package equivalence, and orchestration layering. It found and corrected an ordering hazard: spool-size rejection originally preceded identity/capability validation and could have routed an oversized weak source toward an unsafe caller fallback. Capability validation now precedes the optional admission result, and the permanent test fixes that order. Verdict: **pass for this slice**. Residual risk remains open in the controller: uncoalesced ranges may be latency-bound, and no progressive prefix eviction or cache exists yet.
- 2026-07-14 exact-range batch slice — adversarial self-review traced out-of-order inputs, overlaps, shared allocation ownership, capability ceilings, cancellation, physical-length disagreement, duplicate/missing output slots, and codec/provider layering. No significant or critical finding remains. Single extents larger than the preferred request ceiling deliberately retain their contiguous request shape; silently splitting and reassembling them would add an unbudgeted copy and hide a codec working-set problem. Verdict: **pass for this slice**. Residual risk remains explicit: exact adjacency alone does not solve high-BDP sparse ranges, and adaptive feedback/retry/telemetry are still open.

## Retrospective

The performance failure had two independent serial barriers: repeated Parquet footer parsing on every row group and full transfer completion before any seekable decode. Fixing only the first made local decode competitive but would not have made remote execution I/O-bound. A neutral prepared codec session plus a neutral growing `ByteSource` removed both without teaching source orchestration about Parquet. The live roofline immediately exposed the correct success criterion: cumulative CPU stages may exceed wall time when overlap is working; package wall versus contemporaneous transfer is the useful measure. The remaining memory/disk work must preserve that overlap while replacing full retention with byte-aware residency, generation-bound ranges, or a proven monotone eviction frontier.
