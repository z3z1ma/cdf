Status: active
Created: 2026-07-11
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md
Depends-On: .10x/tickets/done/2026-07-11-p3-g2-range-readahead-spool-controller.md, .10x/tickets/done/2026-07-11-p3-b2-parquet-codec.md, .10x/tickets/2026-07-11-p3-b5-json-codecs.md, .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md

# P3 G3: remote codec and pipeline overlap

## Scope

Integrate Parquet/columnar range units and streaming/compressed row codecs with the transport controller and graph so listing/download/transform/decode/package/staging overlap under canonical order/backpressure.

## Acceptance criteria

- Remote Parquet ranges and row streams keep decoders supplied without violating generation/order/memory.
- HTTP compressed NDJSON begins bounded decode before full download where framing permits.
- Slow destination backpressure reaches network prefetch; cancellation stops requests/spool promptly.
- Jobs 1/N and local/remote recorded equivalents produce identical packages.

## Evidence expectations

Timeline/queue/range traces, recorded network fixtures, jobs/golden parity, compression/spool cases, memory/cancellation, and overlap profiles.

## Explicit exclusions

No new codec or destination.

## Blockers

Depends on G2, relevant WS-B codecs, and A5.

## References

- `.10x/specs/remote-local-io-overlap.md`

## Progress and notes

- 2026-07-12: Strong-ETag HTTP and generation-bound object-store sequential formats now feed registered codecs directly through accounted streams; a loopback fixture proves one full GET with no range fan-out. Adaptive Parquet full scans deliberately remain verified spool until selective-plan evidence and spool/decode overlap land. Transforms, backpressure-to-network, jobs parity, timelines, and profiles remain open. Evidence/review: `.10x/evidence/2026-07-12-p3-g1-async-http-byte-source.md`, `.10x/reviews/2026-07-12-p3-g1-async-http-byte-source-review.md`.
- 2026-07-12: Registered transforms now remain in the direct remote stream for sequential codecs. The object-store gzip-NDJSON fixture overlaps transport, transform, and decode with zero disk spill even under a one-byte spool ceiling. Adaptive transformed formats still wait for growing-spool early decode; backpressure, cancellation, jobs parity, timelines, and profiles remain open. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-streaming-transform-product-composition.md`, `.10x/reviews/2026-07-12-p3-b1-streaming-transform-product-composition-review.md`.
- 2026-07-14: Strong-generation adaptive codecs now consume a growing seekable spool while the same sequential transfer appends to it; the codec sees only the neutral `ByteSource` contract. FineWeb's 2,147,509,487-byte HTTPS Parquet resource completed package, DuckDB receipt, and checkpoint in 16.21 seconds versus an immediately subsequent 14.70-second curl roofline (1.10x). Decode's cumulative 14.495 seconds includes network wait while 6.622 seconds of segment encoding overlaps it. Weak-generation, cancellation-chaos, jobs parity, and backpressure-to-network closeout remain open. Evidence: `.10x/evidence/2026-07-14-p3-g2-fineweb-growing-spool-overlap.md`.
- 2026-07-14: Parquet's multi-range demand now crosses one neutral batch contract. The source controller coalesces safe physical requests and returns exact codec-order slices under shared leases, so provider concurrency no longer leaks into the codec. This is the required seam for later BDP/readahead feedback and backpressure propagation. Evidence: `.10x/evidence/2026-07-14-p3-g2-transport-neutral-range-batching.md`.
- 2026-07-14: Registered format units now overlap behind a bounded canonical runtime frontier. FineWeb's 1,059 row groups execute with at most the memory/CPU/I/O/source-admitted unit count and preserve row-group/item order. This supplies the decoder-side demand frontier for G3, but shared nested admission, network timeline/backpressure evidence, and cancellation chaos remain open. Evidence: `.10x/evidence/2026-07-14-p3-c2-b2-canonical-decode-unit-frontier.md`.
