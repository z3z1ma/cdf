Status: recorded
Created: 2026-07-16
Updated: 2026-07-16

# P3 G3 remote codec overlap and backpressure

## Observation

The production file-source boundary streams a recorded HTTP gzip NDJSON payload into the registered transform and codec before the complete compressed object is transferred. The fixture contains sixteen native batches, exceeding every nested item frontier. With downstream polling withheld, transport reaches a sustained partial-transfer plateau. Bounded downstream draining is the only intervening action and resumes transport demand; withholding again produces a second partial plateau with unread bytes. Termination completes inside a one-second external deadline, closes the nested source without EOF or further byte progress, and releases all managed memory.

The same four-file recorded HTTP gzip NDJSON resource runs on an explicit four-slot host at jobs 1 and jobs 4. The jobs-4 arm observes at least two simultaneously active HTTP streams, while both arms produce identical package hashes, segment entries, profiles, lineage, segment positions, and terminal quarantine. Exact output cardinality is 32,768 rows. C4 remains the authority for the broader jobs 1/2/auto/N local-format and destination matrix.

Together with the existing FineWeb profile and exact-range evidence, the remote data plane has three bounded policies rather than one unconditional path: direct streams for sequential codecs, one generation-bound growing spool for high-coverage finite seekable objects, and controlled exact ranges for selective strong-generation scans. Weak-generation seekable objects retain verified sequential spooling.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project http_ --lib --locked -j 12 -- --nocapture` ran eight HTTP-focused tests. Both new G3 laws passed; six other HTTP tests passed and two unrelated current-path expectation tests failed under the separately active pre-production-vestige owner. This is bounded focused evidence, not a claim that every HTTP test passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project http_gzip_ndjson_backpressures_and_cancels_before_download_completion --lib --locked -j 12 -- --nocapture` passed in 1.23 seconds during the repair pass.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project recorded_http_multifile_packages_are_jobs_invariant --lib --locked -j 12 -- --nocapture` passed in 0.52 seconds on the final explicit four-slot package fixture.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance p2_registry_named_tests_resolve_to_test_functions --lib --locked -j 12 -- --nocapture` passed and verified every named P2/G3 regression owner resolves to a current test function.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-project -p cdf-conformance --all-targets --locked -j 12 -- -D warnings` passed.
- The complete `cdf-project` library audit executed 197 tests before the final review repair: 185 passed, including the then-current G3 tests. Twelve failures were outside this diff: one sandbox-denied local PostgreSQL bootstrap poisoned six sibling PostgreSQL fixtures, while five current-path expectation/race failures remain owned by `.10x/tickets/done/2026-07-11-p0-remove-preproduction-compatibility-vestiges.md`. This run is a bounded non-green audit, not global-pass evidence.
- Existing measured evidence remains `.10x/evidence/2026-07-14-p3-g2-fineweb-growing-spool-overlap.md`: 2.147 GB FineWeb HTTPS Parquet completed in 16.21 seconds versus a contemporaneous 14.70-second curl floor, while decode/download and segment encoding overlapped.
- Existing deterministic parallel evidence remains `.10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md`: native file formats and destinations produce identical semantic artifacts at jobs 1/2/auto/4 on an exact four-slot host.

## What this supports or challenges

This supports G3's four acceptance criteria. The existing G2/B2 evidence covers generation-bound Parquet ranges, growing spool, canonical row-group order, and memory. The new source law covers early compressed-row decode and causal source-edge-to-transport backpressure/cancellation. A5a's `accounted_edge_backpressures_on_global_bytes_and_releases_on_drop` and `cancellation_wakes_a_sender_blocked_by_a_slow_consumer`, joined through A5e's bounded staged-ingress channel, provide the generic slow-destination-to-source-edge half. The jobs law covers actual remote overlap plus deterministic package construction; C4 covers the broader local/destination matrix.

No production source, format, engine, or destination hot path changed. The test fixture now mirrors the production reserve-before-materialization invariant. This evidence therefore protects the existing performance floor and architecture; it does not claim a new throughput gain.

The evidence challenges any future eager-prefetch change that can consume the complete object while downstream is stalled. Such a change must retain an explicit byte bound, cancellation barrier, observed-overlap law, and package identity.

## Limits

The new fixtures use deterministic in-process HTTP transports; they prove runtime semantics rather than public-network throughput or socket teardown latency. Live remote Parquet throughput remains represented by the FineWeb evidence and the still-open G4 end-to-end TLC envelope. The full project suite is not green for the unrelated failures listed above and is not represented as such. S8 preview/run parity is not claimed because these tests do not invoke preview.
