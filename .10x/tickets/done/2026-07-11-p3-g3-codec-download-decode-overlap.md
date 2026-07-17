Status: done
Created: 2026-07-11
Updated: 2026-07-16
Parent: .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md
Depends-On: .10x/tickets/done/2026-07-11-p3-g2-range-readahead-spool-controller.md, .10x/tickets/done/2026-07-11-p3-b2-parquet-codec.md, .10x/tickets/done/2026-07-11-p3-b5-json-codecs.md, .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md

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

None. G2, B2, B5, and A5 are done.

## References

- `.10x/specs/remote-local-io-overlap.md`

## Journal

- 2026-07-12: Strong-ETag HTTP and generation-bound object-store sequential formats now feed registered codecs directly through accounted streams; a loopback fixture proves one full GET with no range fan-out. Adaptive Parquet full scans deliberately remain verified spool until selective-plan evidence and spool/decode overlap land. Transforms, backpressure-to-network, jobs parity, timelines, and profiles remain open. Evidence/review: `.10x/evidence/2026-07-12-p3-g1-async-http-byte-source.md`, `.10x/reviews/2026-07-12-p3-g1-async-http-byte-source-review.md`.
- 2026-07-12: Registered transforms now remain in the direct remote stream for sequential codecs. The object-store gzip-NDJSON fixture overlaps transport, transform, and decode with zero disk spill even under a one-byte spool ceiling. Adaptive transformed formats still wait for growing-spool early decode; backpressure, cancellation, jobs parity, timelines, and profiles remain open. Evidence/review: `.10x/evidence/2026-07-12-p3-b1-streaming-transform-product-composition.md`, `.10x/reviews/2026-07-12-p3-b1-streaming-transform-product-composition-review.md`.
- 2026-07-14: Strong-generation adaptive codecs now consume a growing seekable spool while the same sequential transfer appends to it; the codec sees only the neutral `ByteSource` contract. FineWeb's 2,147,509,487-byte HTTPS Parquet resource completed package, DuckDB receipt, and checkpoint in 16.21 seconds versus an immediately subsequent 14.70-second curl roofline (1.10x). Decode's cumulative 14.495 seconds includes network wait while 6.622 seconds of segment encoding overlaps it. Weak-generation, cancellation-chaos, jobs parity, and backpressure-to-network closeout remain open. Evidence: `.10x/evidence/2026-07-14-p3-g2-fineweb-growing-spool-overlap.md`.
- 2026-07-14: Parquet's multi-range demand now crosses one neutral batch contract. The source controller coalesces safe physical requests and returns exact codec-order slices under shared leases, so provider concurrency no longer leaks into the codec. This is the required seam for later BDP/readahead feedback and backpressure propagation. Evidence: `.10x/evidence/2026-07-14-p3-g2-transport-neutral-range-batching.md`.
- 2026-07-14: Registered format units now overlap behind a bounded canonical runtime frontier. FineWeb's 1,059 row groups execute with at most the memory/CPU/I/O/source-admitted unit count and preserve row-group/item order. This supplies the decoder-side demand frontier for G3, but shared nested admission, network timeline/backpressure evidence, and cancellation chaos remain open. Evidence: `.10x/evidence/2026-07-14-p3-c2-b2-canonical-decode-unit-frontier.md`.
- 2026-07-16: Added a deterministic recorded-HTTP gzip NDJSON law at the actual resource boundary. Sixteen native batches exceed every bounded transport/transform/codec/source frontier. Withholding downstream polls produces a sustained partial-transfer plateau; bounded downstream draining alone resumes transport demand; withholding again produces a second partial plateau. Cancellation races against a one-second external deadline, closes the nested codec stream without EOF or further bytes, and returns managed memory to zero. The test transport now reserves memory before copying source bytes.
- 2026-07-16: Added the remote half of the jobs-invariance law on an explicit four-slot host. Four recorded HTTP gzip NDJSON partitions at jobs 1 and 4 produce identical package hashes, segment entries, profile, lineage, segment positions, and terminal quarantine; the jobs-4 arm proves at least two simultaneously active remote streams rather than merely recording a ceiling. C4 remains the broader jobs 1/2/auto/N destination/source matrix.
- 2026-07-16: Registered the new laws in P2 S3 and friction 16. They are deliberately not listed as S8 evidence because neither invokes preview. The registry self-check also exposed stale references left after pre-production test deletion; references now point only at current authoritative normalization, partitioning, canonical-id, compression, and disposition tests without restoring obsolete behavior.
- 2026-07-16: Verification passed both focused G3 laws, the conformance reference self-check, formatting/diff checks, and strict all-target Clippy for project/conformance. The complete project audit reached 185/197 passing; its twelve unrelated failures are bounded in the evidence record and remain with the active pre-production-vestige owner rather than being hidden as G3 success.

## Evidence

- Remote Parquet generation/order/memory: `.10x/evidence/2026-07-14-p3-g2-fineweb-growing-spool-overlap.md`, `.10x/evidence/2026-07-14-p3-g2-transport-neutral-range-batching.md`, and `.10x/evidence/2026-07-14-p3-c2-b2-canonical-decode-unit-frontier.md` cover the generation-bound growing-spool, exact-range, and canonical row-group paths.
- Early compressed-row decode, causal transport backpressure, bounded cancellation, and memory release: `.10x/evidence/2026-07-16-p3-g3-remote-codec-overlap.md` maps to `http_gzip_ndjson_backpressures_and_cancels_before_download_completion`.
- Slow-destination propagation is a composition law, not a destination-specific file test: A5a's `accounted_edge_backpressures_on_global_bytes_and_releases_on_drop` and `cancellation_wakes_a_sender_blocked_by_a_slow_consumer` prove a slow graph consumer closes upstream edge admission; A5e's staged-ingress bounded channel is the generic destination join; the G3 test proves source-edge demand reaches the remote transport.
- Jobs/local/remote identity: `.10x/evidence/2026-07-16-p3-g3-remote-codec-overlap.md` maps to `recorded_http_multifile_packages_are_jobs_invariant`; `.10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md` owns the broader local jobs 1/2/auto/N and destination matrix.
- Conformance ownership: `p2_registry_named_tests_resolve_to_test_functions` passes with the new G3 laws named in S3 and friction 16.

## Review

Two independent adversarial reviews initially failed closure and found no production hot-path regression. The runtime review found the first five-batch/quiet-interval law could be a false positive, cancellation was not time-bounded, destination propagation was overclaimed, and the fixture copied before reserving. The determinism review found jobs 4 recorded only a ceiling, the tests were misclassified under S8, and exact cardinality was missing. One repair pass replaced timing-only evidence with plateau→bounded-demand→resume→second-plateau causality, bounded the join itself, reserved before copy, used an explicit four-slot host with observed concurrent streams, asserted exact rows and complete package semantics, removed S8 attribution, and cited the existing A5/A5e destination-edge authority. Closure judgment: **pass**; all critical/significant findings are reconciled and G4 retains live-network teardown/throughput measurement.

## Retrospective

The useful lesson was that a quiet counter is not backpressure evidence and a configured jobs value is not parallelism evidence. Permanent laws now require causal resume and observed overlap. Keeping the remote test at the package boundary avoids contaminating a source/codec law with DuckDB's independent ingress cost, while C4 retains full receipt/checkpoint invariance. The reviewers also prevented a coverage-matrix category error: implementation-adjacent tests belong only to the scenarios they actually execute.
