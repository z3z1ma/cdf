Status: active
Created: 2026-07-11
Updated: 2026-07-17
Parent: .10x/tickets/2026-07-10-p3-ws-g-remote-io-overlap.md
Depends-On: .10x/tickets/done/2026-07-11-p3-g3-codec-download-decode-overlap.md, .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/done/2026-07-11-p3-c4-jobs-invariance-scaling-matrix.md

# P3 G4: remote/local I/O envelope and TLC closeout

## Scope

Run recorded and live public TLC plus S3/GCS/Azure/local roofline scenarios, publish network/device overlap evidence, tune safe defaults, and make I/O-bound acceptance permanent without making ordinary CI network-dependent.

## Acceptance criteria

- Full-year TLC HTTPS-to-DuckDB meets the 1.5x composite target where environment permits and profile is I/O/destination-bound.
- S3/GCS/Azure/live cells are labeled and recorded; deterministic fixtures gate CI.
- Local sequential/range strategy reaches its measured roofline without unratified unsafe paths.
- Remote controller overhead, retries, waste, cache/spool, memory, and identity are within budgets.

## Evidence expectations

Host/network/provider reports, raw profiles/timelines, live/recorded comparison, jobs/memory/identity conformance, and adversarial weak-validator/throttle review.

## Explicit exclusions

No guarantee about third-party public endpoint uptime/bandwidth.

## Blockers

Depends on G1-G3; DuckDB bulk and deterministic scaling closeout are complete.

## Assumptions

- Record-backed: HTTP transfers may run for hours, so the transport MUST NOT impose a total request deadline. File byte-source progress deadlines may bound response establishment and body-frame idle time because healthy long transfers reset the body deadline every time bytes arrive.
- User-ratified: performance and correctness are the joint first priority; a production change in this ticket must preserve or improve the measured hot path unless it repairs incorrect behavior.

## References

- `.10x/specs/remote-local-io-overlap.md`
- `.10x/decisions/terabyte-scale-performance-envelope.md`

## Progress and notes

- 2026-07-14: The live single-object FineWeb proxy is I/O-bound after prepared decode metadata and growing-spool overlap: 16.21 seconds CDF end to end versus 14.70 seconds immediate curl (1.10x), with a 2.205 GB governed package, DuckDB receipt, and committed checkpoint. This is strong proxy evidence but does not close G4's full-year TLC, provider matrix, memory, retry/waste, or recorded-fixture gates. Evidence: `.10x/evidence/2026-07-14-p3-g2-fineweb-growing-spool-overlap.md`.
- 2026-07-16: Activated after G3 closure. The first action is to turn the catalog's currently unavailable full-year TLC cells into an opt-in, host-labeled runner and run the current release path against the public project fixture. Provider-live cells stay opt-in; deterministic controller/backpressure/identity fixtures remain the ordinary-CI authority. Tuning is permitted only after the same-host composite floor identifies a measured deficit.
- 2026-07-16: The first twelve-month live attempt produced two partitions and then made no durable progress for roughly four minutes while consuming almost no CPU (`real 246.65s`, `user 4.63s`). The same release completed a fresh three-month run over the same origin in `4.71s` for 9.6 million rows, 153 MiB of source data, a 334 MiB governed package, and a DuckDB receipt. This falsifies a general decode/segment/DuckDB collapse and isolates an unbounded stalled-HTTP-response liveness risk. The HTTP client had no connect or read-idle deadline, so a silent socket could never enter the existing typed transient retry path.
- 2026-07-17: Corrected the HTTP liveness boundary after adversarial review. The ratified default is now private to file byte-source transfer phases: 10 seconds to receive the file response, 10 seconds between delivered body frames, and no total transfer deadline. The shared reqwest client no longer carries a global read timeout, so REST and file metadata keep their prior semantics. Timeout failures retain the transport's existing sanitized `Transient` taxonomy; the deterministic slice proves bounded classification/cleanup and preserves slow-progressing transfers, while scheduler retry success remains governed by the compiled source retry path.
- 2026-07-17: Release remeasurement after the liveness correction: fresh three-month TLC completed in `real 5.40s` (`user 5.98s`, `sys 3.61s`, 9.6M rows, 51 segments), preserving the established fast path within public-endpoint noise but not improving it. Fresh full-year TLC was interrupted after `real 114.73s` (`user 3.02s`, `sys 9.42s`, max RSS about 1.0 GB) with only first-partition segment files present and only startup events in the state ledger. This fails the same-host 10.31-second G4 ceiling and keeps G4 open.
- 2026-07-16: Same-host raw controls established the current G4 floor: two-way parallel curl downloaded the 12 public TLC objects (660 MiB) in `4.34s`; DuckDB natively created the 41,169,720-row table in `2.53s`. The ratified 1.5x composite ceiling is therefore `10.31s` on this observation. The native cell omits CDF evidence/package work and is labeled as a favorable roofline rather than semantic equivalence.

## Evidence

- HTTP liveness slice: `.10x/evidence/2026-07-16-p3-g4-http-liveness.md`.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-transport-http --lib --locked -j 12`: 15 passed.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-transport-http --all-targets --locked -j 12 -- -D warnings`: passed.
  - `CARGO_BUILD_JOBS=12 cargo build -p cdf-cli --release --locked -j 12`: passed.
  - Fresh 3-month live TLC, `/private/tmp/cdf-g4-tlc3-after.nBhv2i`: `real 5.40s`, success.
  - Fresh 12-month live TLC, `/private/tmp/cdf-g4-tlc-after.PvMGvm`: interrupted at `real 114.73s`; failed G4 envelope.

## Review

Adversarial review of the first draft found that a global reqwest read timeout leaked file-transfer policy into REST/metadata behavior and consumed the default retry budget. The implementation was revised to private file byte-source phase deadlines and expanded with REST non-inheritance and slow-progress tests. Final review pending after live remeasurement.

## Retrospective

Pending ticket completion.
