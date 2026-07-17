Status: active
Created: 2026-07-10
Updated: 2026-07-16
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md, .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md, .10x/specs/data-onramp-file-sources-transports.md

# P3 WS-G: remote I/O overlap

## Scope

Build on the P2 transport facade with parallel ranged Parquet GETs, bounded readahead, download/decode overlap, connection pooling, HTTP/2 where supported, and local sequential readahead. Keep logical-file identity, egress, credentials, retries, and manifest semantics unchanged.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-g1-streaming-transport-byte-sources.md`
- `.10x/tickets/done/2026-07-11-p3-g2-range-readahead-spool-controller.md`
- `.10x/tickets/2026-07-11-p3-g3-codec-download-decode-overlap.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`

## Acceptance criteria

- Remote TLC ingest is network-bound with decode hidden where host/network permit.
- Ranged reads and readahead are ledger-owned and cancellation-safe.
- Transport-specific tuning remains behind the transport facade and capability data.
- The full-year TLC envelope target has a labeled profile and replayable recorded-fixture test.

## Blockers

No immediate execution blocker. G1 and G2 are terminal; G3 builds on their provider/controller contracts and the completed lab, execution-host, operator-graph, scheduler-admission, and memory-ledger foundations. Program closure depends on G3 codec/pipeline overlap and the G4 live/recorded envelope.

## References

- `.10x/decisions/generation-bound-overlapped-io.md`
- `.10x/specs/remote-local-io-overlap.md`

## Progress and notes

- 2026-07-14: Remote Parquet full scans now overlap one sequential transfer with local decode through a generation-bound growing spool when the complete finite object fits the atomically reserved spill budget. Strong exact-range sources that do not fit no longer require object-sized local disk: they fall back to the registered codec's generation-bound range access with zero spool consumption. Weak/unversioned sources retain verified sequential staging, and unbounded row streams remain direct bounded decode paths. Evidence: `.10x/evidence/2026-07-14-p3-g2-fineweb-growing-spool-overlap.md`, `.10x/evidence/2026-07-14-p3-g2-large-object-range-fallback.md`.
- 2026-07-16: G2 closed at `.10x/tickets/done/2026-07-11-p3-g2-range-readahead-spool-controller.md`. The terminal controller now covers origin-shared adaptive/fixed range admission, exact retries, bounded coalescing/readahead, full/growing/evicting spools, cancellation, telemetry, and opt-in generation-revalidated cache promotion without adding payload work to the disabled or oversized paths. G3 owns end-to-end codec/network backpressure and jobs parity; G4 owns the live provider/TLC envelope and default tuning.
- 2026-07-16: B5 closed at `.10x/tickets/done/2026-07-11-p3-b5-json-codecs.md`; G3's four direct dependencies are now terminal. Remote row codecs already stream and REST page fetch overlaps tape decode. G3 can execute its remaining transport-to-graph backpressure, cancellation-chaos, jobs-equivalence, and timeline/profile acceptance surface without another codec prerequisite.
