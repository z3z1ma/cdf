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
- 2026-07-17: Tested a file-source scheduler hard cap as a source-owned experiment, not as an accepted product default. The experiment preserved generic engine orchestration but violated the P3 performance rule: it could degrade healthy multi-file scans while trying to protect one public-origin workload. The hidden hard cap is rejected; the file source now advertises one high-parallelism capability value and any future pressure control must be adaptive and/or explicitly configured rather than hard-set globally.
- 2026-07-17: Live remeasurement after the four-way source lane cap falsified the cap as a sufficient G4 fix. A disposable 12-file TLC project under `/private/tmp/cdf-g4-full-after-cap` deep-validated 12 partitions, then the release run was interrupted at `real 102.19s` (`user 8.56s`, `sys 11.21s`, peak RSS about 1.6 GB) after missing the 10.31s same-host ceiling. The interrupted package contained 69 data segments across only partitions `p00000000` through `p00000003` (about 478 MiB package data), while no package segment events had reached the run ledger before finalization. This proves the cap prevented all-12 over-admission but introduced wave/head-of-line behavior; the next fix must preserve bounded public-origin pressure without serializing the month set into slow waves.
- 2026-07-17: A retry of the explicit TLC numeric template (`yellow_tripdata_2024-{01..12}.parquet`) initially failed with an HTTP GET 403, but rebuilding the release CLI made the same disposable project deep-validate 12 partitions successfully. The 403 was stale-binary measurement noise, not a source-layer enumeration finding.
- 2026-07-17: Rebuilt-release full-year template run still failed the envelope: `/private/tmp/cdf-g4-template-rebuilt.ZD3czp` deep-validated 12 partitions, then `cdf run tlc.yellow` was interrupted at `real 103.09s` (`user 3.74s`, `sys 3.65s`, peak memory footprint `927466888`). The partial package contained only partitions `p00000000` and `p00000001` (32 segment files, about 220 MiB). This strengthens the conclusion that the four-way source blocking lane is the wrong default: it bounds pressure but lets slow first-wave partitions hold the frontier while later months do no work.
- 2026-07-17: Reframed the current G4 miss as a P0 regression hunt after local/remote isolation. A single local April TLC Parquet file to the Parquet destination completed in `real 0.94s`, while the same local file to DuckDB timed out at `real 45.02s` after producing only 10 segment files and no finalized package events. That falsifies HTTP/range/discovery/source-frontier as the immediate owner and isolates DuckDB staged ingress. The stall aligned with the first hidden mid-stream DuckDB Arrow appender flush threshold (~64 MiB), so the fix removes that arbitrary mid-stream flush and keeps only the final appender flush. Rebuilt release remeasurement of the same local April file to DuckDB then completed in `real 1.93s` for 3.5M rows and 18 segments with zero swaps. This repairs a throughput regression without adding a hard cap or destination-specific orchestration branch.
- 2026-07-17: The rebuilt full-year remote TLC run after the DuckDB flush repair still timed out at the 45-second guard, with only partitions `p00000000` through `p00000003` materialized (69 Arrow segments, about 478 MiB). This keeps G4 open and narrows the remaining owner to remote multi-partition frontier/admission/head-of-line behavior rather than the repaired local DuckDB staged-ingress stall.
- 2026-07-17: Removed the network from the remaining failure. A complete local 12-month TLC control set under `/private/tmp/cdf-g4-tlc-control` loaded to the Parquet destination in `real 5.90s` for 41.2M rows and 215 segments, while the same local files to DuckDB timed out at `real 45.04s` after 106 segments / about 745 MiB and no final target table. This falsifies source decode, package build, local file I/O, schema admission, and destination-independent finalization as the current P0 owner. The remaining regression is DuckDB destination bulk ingestion at full-year scale. A destination-internal temp-staging-table experiment was rejected: it still timed out at 45s, reduced progress to 69 segments, and forced about 1.2 GiB of DuckDB temp spill, so it is not retained.
- 2026-07-17: Replaced knob-poking with a deterministic git bisect using the local 12-file TLC-to-DuckDB predicate (`timeout 30s cdf run tlc.yellow`). `b1ca3879` completed in `real 21.50s`, `703e5293` completed in `real 16.52s`, and current `main` timed out before the fix. `git bisect` identified `3a5e1802 feat(runtime): execute canonical source frontier` as the first bad commit. The static root cause was the new canonical source frontier's partition-retirement path: `finish_current()` called `fill_head()`, collapsing the configured open frontier back to one active partition whenever the current partition retired. The predecessor replenished the open frontier on every partition completion. The fix changes retirement to `fill_active()` so the frontier keeps its declared `maximum_active` warm while preserving the one-prefetched-batch memory gate.
- 2026-07-17: Rebuilt release after the frontier retirement fix and reran the deterministic full-year local TLC-to-DuckDB control. `/private/tmp/cdf-g4-frontier-fix.0DMgcq` completed in `real 16.25s` (`user 24.40`, `sys 3.30`) for 41.2M rows, 215 segments, and a committed DuckDB receipt. This restores and slightly improves the good `703e5293` baseline under the same predicate. The measured remaining bottleneck is destination ingress (`15.52s`) and segment encode/persist (`5.79s`/`2.12s`), not a low-CPU scheduler stall.

## Evidence

- HTTP liveness slice: `.10x/evidence/2026-07-16-p3-g4-http-liveness.md`.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-transport-http --lib --locked -j 12`: 15 passed.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-transport-http --all-targets --locked -j 12 -- -D warnings`: passed.
  - `CARGO_BUILD_JOBS=12 cargo build -p cdf-cli --release --locked -j 12`: passed.
  - Fresh 3-month live TLC, `/private/tmp/cdf-g4-tlc3-after.nBhv2i`: `real 5.40s`, success.
  - Fresh 12-month live TLC, `/private/tmp/cdf-g4-tlc-after.PvMGvm`: interrupted at `real 114.73s`; failed G4 envelope.
- File-source hard-cap rejection:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files execution_capabilities_share_advertised_parallelism_with_blocking_lane --locked -j 12` — passed, 1 passed. Proves the driver capability and blocking lane share the same advertised source parallelism, not a separate hidden hard cap.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files file_source_blocking_lane_matches_advertised_parallelism --locked -j 12` — passed, 1 passed. Replaces the rejected cap-specific test.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-files --all-targets --locked -j 12 -- -D warnings` — passed.
  - G4 remains open for an adaptive or operator-configured pressure-control design with measured non-regression evidence; no hard-cap experiment is retained as closure evidence.
- Post-cap live falsification:
  - Disposable project: `/private/tmp/cdf-g4-full-after-cap`, `glob = "yellow_tripdata_2024-*.parquet"`.
  - `target/release/cdf --project /private/tmp/cdf-g4-full-after-cap validate --deep --color never` — passed with 12 partitions, no writes.
  - `/usr/bin/time -lp target/release/cdf --project /private/tmp/cdf-g4-full-after-cap run tlc.yellow --progress never --color never` — interrupted at `real 102.19`, `user 8.56`, `sys 11.21`, peak memory footprint `1604274024`; failed the 10.31s G4 ceiling.
  - Partial package inspection found 69 segment files across first-wave partitions only: `p00000000` 16 segments / 109,465,200 bytes, `p00000001` 16 / 110,295,472, `p00000002` 19 / 130,376,014, `p00000003` 18 / 128,165,620. The SQLite run ledger contained only `run_started`, `plan_recorded`, and `package_started` because segment events are not durable until later package progress/finalization in this interrupted path.
  - Rebuilt-release template rerun: `/private/tmp/cdf-g4-template-rebuilt.ZD3czp`, `glob = "yellow_tripdata_2024-{01..12}.parquet"`, deep validate passed with 12 partitions; run interrupted at `real 103.09`, `user 3.74`, `sys 3.65`, peak memory footprint `927466888`; partial package held 32 segment files for `p00000000` and `p00000001` only.
- DuckDB staged-ingress regression isolation and repair:
  - Local April source file: `/private/tmp/cdf-g4-local-files/yellow_tripdata_2024-04.parquet`.
  - Parquet destination control: `/private/tmp/cdf-g4-local-april-parquetdest.8AeKAw`, `cdf run tlc.yellow` completed in `real 0.94`, `user 1.04`, `sys 0.35`; proves local source decode/package production is not the P0 stall owner.
  - DuckDB destination before repair: `/private/tmp/cdf-g4-local-april.LYOM0D`, `timeout 45s target/release/cdf --project ... run tlc.yellow --progress never --color never` timed out at `real 45.02`, `user 0.94`, `sys 0.21`, max RSS `534 MiB`; partial package had 10 Arrow segments and manifest status `extracting`.
  - `CARGO_BUILD_JOBS=12 cargo build -p cdf-cli --release --locked -j 12` — passed after target cleanup, rebuilding the edited DuckDB destination into the release CLI.
  - DuckDB destination after repair: `/private/tmp/cdf-g4-local-april-duckdb-rerun.UVZs8R`, `cdf run tlc.yellow` completed in `real 1.93`, `user 2.11`, `sys 0.32`, max RSS `585678848`, 3.5M rows, 18 segments, package `sha256:82444782deb65487edf3354bc6351dbfe55ef42effaa48dd2c2a5dc6e4cce49e`.
  - Full-year remote after repair: `/private/tmp/cdf-g4-full-duckdb-flushfix.mlljXv`, `timeout 45s cdf run tlc.yellow` exited `124` at `real 45.05`, `user 8.62`, `sys 10.20`, max RSS about `1.65 GiB`; partial package contained 69 Arrow segments / 478,302,306 bytes for partitions `p00000000` through `p00000003`. This fails the G4 envelope and remains the next active performance target.
  - Local full-year Parquet destination control: `/private/tmp/cdf-g4-local-full-parquetdest.Dh2IUU`, `cdf run tlc.yellow` completed in `real 5.90`, `user 12.20`, `sys 3.87`, 41.2M rows, 215 segments, package `sha256:ebff72047da350c5ddd04b5095595c62e005fc6a8dadfa9a94fa5964916b8919`.
  - Local full-year DuckDB remaining failure: `/private/tmp/cdf-g4-local-full-duckdb.KABzRa`, `timeout 45s cdf run tlc.yellow` exited `124` at `real 45.04`, `user 11.94`, `sys 2.31`; partial package contained 106 Arrow segments / 744,719,724 bytes across partitions `p00000000` through `p00000005`, and DuckDB had no final `yellow` table after rollback.
  - Rejected temp-staging experiment: `/private/tmp/cdf-g4-local-full-duckdb-stage.gC8GED`, `timeout 45s cdf run tlc.yellow` exited `124` at `real 45.05`, `user 5.46`, `sys 1.57`; progress regressed to 69 segments / 478,302,306 bytes and DuckDB created about 1.2 GiB of temp spill under `.cdf/dev.duckdb.tmp`, so the uncommitted experiment was reverted.
- Canonical source-frontier regression bisect and repair:
  - `git bisect start main b1ca3879` with predicate `CARGO_BUILD_JOBS=12 cargo build -p cdf-cli --release --locked -j 12 && timeout 30s cdf run tlc.yellow` over a fresh project copied from the 12-file local TLC fixture.
  - Known-good anchors: `b1ca3879` completed in `real 21.50s`; `703e5293` completed in `real 16.52s`.
  - Bad/current symptom: low-user-CPU timeout, proving a wait/backpressure regression rather than codec or validation compute.
  - First bad: `3a5e1802 feat(runtime): execute canonical source frontier`.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime partition_finish_replenishes_the_configured_open_frontier --locked -j 12` — passed, 1 passed. The new test fails the old retirement behavior because partition 2 is not opened after partition 0 finishes while partition 1 is already prefetched.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime --lib --locked -j 12` — passed, 84 passed, 1 ignored.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-runtime --all-targets --locked -j 12 -- -D warnings` — passed.
  - `CARGO_BUILD_JOBS=12 cargo build -p cdf-cli --release --locked -j 12` — passed.
  - Repaired local full-year TLC-to-DuckDB: `/private/tmp/cdf-g4-frontier-fix.0DMgcq`, `timeout 30s cdf run tlc.yellow` completed in `real 16.25`, `user 24.40`, `sys 3.30`, max RSS `2.46 GiB`, 41.2M rows, 215 segments, package `sha256:07c3ef5fc12f3876e9bbaca7b17056c62b0c464cf554036443e4caf586d78639`.

## Review

Adversarial review of the first draft found that a global reqwest read timeout leaked file-transfer policy into REST/metadata behavior and consumed the default retry budget. The implementation was revised to private file byte-source phase deadlines and expanded with REST non-inheritance and slow-progress tests. Final review pending after live remeasurement.

## Retrospective

Pending ticket completion.
