Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md
Depends-On: .10x/tickets/2026-07-11-p3-g2-range-readahead-spool-controller.md, .10x/tickets/2026-07-11-p3-b2-parquet-codec.md, .10x/tickets/2026-07-11-p3-b5-json-codecs.md, .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md

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
