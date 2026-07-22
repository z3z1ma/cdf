Status: active
Created: 2026-07-21
Updated: 2026-07-21
Parent: .10x/tickets/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md

# P3 D18A: DuckDB wide roofline and profile

## Scope

Create a reproducible controlled-host workload for the exact finalized 3,513,266-row,
2,052-column package and measure CDF's current stock scanner against the closest semantics-labeled
raw DuckDB materialization. Capture operator timings, CPU, rows, logical/physical bytes, process and
cgroup memory, DuckDB peak buffer memory, peak temp-directory size, and spill.

## Non-goals

No product tuning, path change, source re-extraction, or conclusion from a laptop-only sample.

## Acceptance Criteria

- The retained package and schema/statistics identities are recorded without committing payload.
- The lab has a repeatable raw reference and full-CDF replay cell with explicit semantic bias.
- Median-of-N controlled EC2 evidence attributes scanner conversion, DuckDB sink/storage,
  checkpoint/receipt, peak buffer memory, peak temp storage, and process/cgroup memory.
- The profile names the dominant wide-schema cost and establishes comparison keys for D18B-E.
- Existing full-year TLC control is rerun on the same clean revision/host class.

## References

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`
- `.10x/tickets/done/2026-07-21-p0-duckdb-wide-ingest-memory.md`

## Assumptions

- Record-backed: the finalized local package under `/Users/alexanderbut/code_projects/tmp/.cdf/packages/`
  is reusable benchmark input after manifest verification; no FQ12 source contact is required.
- User-ratified: performance claims require real end-to-end and EC2 evidence rather than intuition.

## Journal

- 2026-07-21: Began execution from the retained exact 3,513,266-row package. The existing lab
  already owns an independent stock-public-C-API canonical-segment scanner and the lean measured
  command runner already owns full `cdf replay` phase/RSS/cgroup evidence. The missing authority is
  DuckDB's native operator profile. The retained design adds opt-in profiling around only the
  materialization query in the destination and the same opt-in to the independent comparator; the
  default path, artifact identity, and performance configuration remain unchanged.
- 2026-07-21: Verified the retained finalized package before transfer: package id
  `pkg-flolake-transactions-92680-1784668000407799000`, package hash
  `sha256:69183c567f1b15bdf2cf6eafcfb3669d83ee1a3f3a29dd39f785a68a331d43c4`, 3,513,266 rows,
  231 canonical segments, 1,291,273,686 segment bytes, and 2,053 persisted fields (2,052 user
  fields plus `_cdf_package_row_ord`). The shape has 1,247 Arrow batches, 15,208 average rows per
  segment, and 2,817 average rows per batch; this is the exact artifact D18A must explain rather
  than a synthetic wide-table approximation.
- 2026-07-21: Added destination-owned, opt-in native DuckDB profiling around only the canonical
  segment `INSERT ... SELECT` materialization statement. `CDF_DUCKDB_PROFILE_DIRECTORY` is absent
  by default, so ordinary commits retain their configuration and path; enabled captures use unique
  filenames and always disable profiling before returning, including failed capture and OOM-retry
  paths. Added the same capture to the independent stock-public-C-API comparator and separated its
  global DuckDB threads from scanner threads so the raw and product cells can use the same admitted
  wide-schema scanner width without artificially suppressing the sink.
- 2026-07-21: Added a versioned DuckDB JSON-profile normalizer to `cdf-p3-lab` and a benchmark-host
  `sync-package` command that verifies the finalized package before and after rsync. The affected
  full tests pass (benchmark lab 19 unit + 7 fixture + 6 policy + 11 runner, one deliberate live
  PostgreSQL ignore; DuckDB 47 tests), including real product/reference profiles and failed-capture
  cleanup. Strict affected-crate Clippy passes. The benchmark catalog test exposed a stale DuckDB
  `max_in_flight_bytes` fixture left by the earlier 256 MiB segment-envelope change; the fixture is
  realigned to the runtime authority rather than weakening the test.
- 2026-07-21: The first live `sync-package` attempt verified all 246 package files, then macOS's
  system rsync rejected GNU-only `--info=progress2` before transferring payload. Replaced the
  display-only flag with portable `--stats`; the failed attempt left only an empty remote target
  directory and produced no measurement or package mutation.
- 2026-07-21: Controlled-host profiled replay on revision `bc8e737d` took 205.224 s for 3,513,266
  rows. DuckDB attributed 194.152 s of query latency and 369.570 aggregate CPU-seconds to the
  materialization: 324.963 aggregate seconds in `INSERT`, 43.980 in the canonical table scan, and
  0.627 in projections. Peak DuckDB buffer memory was 4,961,632,256 bytes and peak temp-directory
  storage was 7,564,656,640 bytes. The 16 GiB cgroup recorded no pressure or OOM event; child peak
  RSS was 4,134,821,888 bytes. This identifies DuckDB wide-table storage/encoding as the dominant
  cost, not CDF verification or canonical IPC decoding.
- 2026-07-21: The first exact raw-reference probe failed at bind time before reading payload because
  the intentionally independent benchmark table-function mapper supported primitives/decimals but
  not the package's `List<Utf8>` fields. The product adapter already supports that type. Added
  generic recursive list binding to the benchmark-only mapper rather than altering product code or
  special-casing field names; the failed observation is retained as evidence and makes no
  performance claim.
- 2026-07-21: The corrected independent raw-reference profile completed all 3,513,266 rows from all
  231 canonical segments on revision `2c61cf73`. Its native DuckDB query took 217.364 s versus
  194.152 s for the product query. DuckDB attributed 324.875 aggregate seconds to
  `CREATE_TABLE_AS`, 90.103 to the independent canonical scanner, and 0.330 to projection. Peak
  buffer memory was 8,544,073,728 bytes and peak temp-directory storage was 6,949,044,224 bytes;
  child peak RSS was 4,581,232,640 bytes and the cgroup recorded no pressure or OOM event. The raw
  reference therefore does not justify replacing the product scanner: under the same two-thread
  scan admission it consumed more scanner CPU and more DuckDB buffer memory while finishing 11.9%
  slower. The warm median-of-three remains the comparison authority; this single profiled sample
  establishes the operator-level hypothesis to test.
- 2026-07-21: The raw warm median-of-three confirmed the profile without material variance:
  217.385 s median wall time, 0.151 s median absolute deviation, approximately 642.05 CPU-seconds,
  and 4,605,509,632 bytes peak child RSS. All three samples completed the exact row count with no
  cgroup pressure or OOM event. The session cgroup was observational rather than capped
  (`memory.max = max`); the workload's explicit DuckDB buffer-manager limit remained 4 GiB and its
  temp-directory budget 32 GiB. The product median uses the same host and explicit DuckDB policy
  under a 16 GiB process cgroup.
- 2026-07-21: Recorded the package's schema authorities in addition to its package identity. The
  compiled effective schema hash is
  `sha256:1585e0c7c1e2a0f1824ef739ea2adb091ce9e736ec00fe63dd06a22475e76943`; the manifest-bound
  runtime Arrow-schema artifact is
  `sha256:f3ed582ec053a7e45f4d16e868412b98fe47727faa399bb187af53b0a4a37b85`. This retained package
  has no `stats/profile.parquet` identity artifact because the source run did not enable the
  optional statistics profile; no statistics-based projection conclusion may be drawn from it.
- 2026-07-21: The full-CDF warm median-of-three under `MemoryMax=16G` was 204.913 s with 0.022 s
  median absolute deviation, approximately 579.39 median CPU-seconds, 4,180,824,064 bytes peak
  child RSS, and zero cgroup pressure/OOM events. CDF was 5.74% faster than the independent raw
  comparator's 217.385 s median. In the profiled product sample, DuckDB's native query occupied
  194.152 s of the 205.224 s full wall. Package verification, database setup, receipt/checkpoint
  publication, and CLI/report teardown are therefore jointly bounded by the remaining 11.072 s
  (5.39%); the replay command does not currently emit individual phase measurements, so D18A does
  not fabricate a finer split. DuckDB's profile attributes the native work directly: 324.963
  aggregate CPU-seconds to `INSERT`, 43.980 to canonical scan, and 0.627 to projection.
- 2026-07-21: The first TLC control attempt correctly failed before measurement because its retained
  `cdf.lock` was produced before the current discovery-binding artifact contract. Customer-zero
  policy does not preserve old artifact compatibility. Prepared a new current-revision template,
  pinned `tlc.yellow` outside the timed region at schema
  `sha256:f9ae139ae8c63e93fa57ff3ba5edf8fe8c9565bd11a557c6ab12b3a855a1d847`, and reran the
  control rather than weakening the authority check.
- 2026-07-21: The current-revision full-year TLC control completed 41,169,720 rows in a 10.247909 s
  warm median with 1.464 ms median absolute deviation, 4,017,377 rows/s, 3,736,166,400 bytes peak
  child RSS, and zero cgroup pressure/OOM/spill. The retained stock-scanner authority is
  10.255643 s; current code is 0.08% faster. D18A therefore preserves the narrow/TLC envelope and
  identifies the 2,052-column DuckDB storage sink as the wide-specific floor.
- 2026-07-21: Adversarial review correctly rejected attributing the approximately 11-second
  non-DuckDB remainder as a single unexplained bound. Added generic replay-owned phase metrics for
  destination settlement, the checkpoint gate, and total package replay; the CLI now serializes
  those metrics and the command benchmark consumes them before its legacy ledger fallback. The
  change is destination-neutral and does not touch DuckDB ingestion. Full affected suites pass:
  10 `cdf-bench-core`, 274 `cdf-cli`, and 214 `cdf-project` tests, plus strict affected-crate
  Clippy and formatting. That gate also exposed and repaired a stale `quasar` test-destination
  capability sheet that omitted the framework's UTF-8 residual column.
- 2026-07-21: Re-ran both wide cells at current revision `2ba50791` under the same
  `host-class-35789da2705a032d`, 16 GiB systemd cgroup, 16 DuckDB threads, two scanner threads,
  exact 3,513,266-row authority, exact 115,421,208,360 logical-byte authority, and exact
  1,291,273,686 physical-IPC-byte authority. The corrected raw median is 217.217974 s
  (0.288080 s MAD); the corrected full-CDF median is 205.170661 s (0.014638 s MAD). CDF is 5.55%
  faster than the semantics-labeled raw comparator. Neither cell recorded cgroup pressure or OOM.
  The macro harness cannot observe DuckDB temp spill and therefore serializes zero; the retained
  native profiles' `system_peak_temp_directory_size` is the explicit spill authority.
- 2026-07-21: Corrected current-revision native profiles preserve the same conclusion. The raw
  comparator spent 217.030 s native latency, 414.306 aggregate CPU-seconds, 323.829 aggregate
  seconds in `CREATE_TABLE_AS`, and 90.142 in its scan; peak buffer/temp were 8,450,850,816 and
  6,960,742,400 bytes. The product spent 194.032 s native latency, 369.357 aggregate CPU-seconds,
  325.358 aggregate seconds in `INSERT`, and 43.374 in its scan; peak buffer/temp were
  5,177,131,008 and 7,475,789,824 bytes. The product scanner is not the wide floor: DuckDB's
  columnar storage sink dominates both plans.
- 2026-07-21: Replay-owned telemetry now attributes the profiled product sample without inference:
  195.648 s destination settlement, 67.547 ms checkpoint gate, and 195.724 s total package replay
  within 205.161 s command wall. The 9.437 s outside package replay remains a bounded CLI/package
  open/runtime/report interval; D18A does not invent subdivisions the event spine did not observe.
  One of three product macro samples reported 580.74 CPU-seconds while the other two reported
  385.27 and 390.68 despite near-identical wall/replay phases. Aggregate `/usr/bin/time` CPU is
  consequently inconclusive for fine attribution; the native DuckDB operator profile is authority.
- 2026-07-21: Re-ran TLC at current revision without a systemd memory cap on the exact historical
  `host-class-649c6f28be3544c8`. The warm median is 10.226223 s (21.554 ms MAD), 4,025,897 rows/s,
  and 3,733,745,664 bytes peak child RSS for all 41,169,720 rows with no cgroup pressure/OOM. It is
  0.29% faster than the 10.255643 s retained floor. The earlier 10.247909 s capped control remains
  a valid observation but is not the comparison authority because its host-class identity differs.

## Blockers

None.

## Evidence

- Package authority: the verified manifest records package hash
  `sha256:69183c567f1b15bdf2cf6eafcfb3669d83ee1a3f3a29dd39f785a68a331d43c4`, effective schema
  `sha256:1585e0c7c1e2a0f1824ef739ea2adb091ce9e736ec00fe63dd06a22475e76943`, runtime Arrow schema
  artifact `sha256:f3ed582ec053a7e45f4d16e868412b98fe47727faa399bb187af53b0a4a37b85`, exact row/field/
  segment/batch shape, and absence of an optional statistics-profile artifact.
- Repeatable cells: commit `5f38d6ee` added opt-in product/reference native profiles and the
  versioned profile reader; `bc8e737d` made package sync portable; `2c61cf73` admitted the exact
  nested list schema in the independent comparator; `2ba50791` added destination-neutral replay
  phase metrics. Profiling is absent by default and does not change ordinary execution.
- Corrected raw cell: the retained `2026-07-21-p3-d18a-wide-raw-corrected-{profiled,median3}`
  request, run-cell, report, and systemd-log files record the command, host class, revision,
  authorities, biases, exact samples, and cgroup state. The adjacent
  `2026-07-21-p3-d18a-wide-raw-corrected.duckdb-profile{,.normalized}.json` pair retains native and
  normalized operator evidence. The raw comparison deliberately excludes CDF evidence work.
- Corrected product cell: the retained
  `2026-07-21-p3-d18a-wide-product-corrected-{profiled,median3}` request, run-cell, report, and
  systemd-log files record the same authorities on the same host class. The adjacent
  `2026-07-21-p3-d18a-wide-product-corrected.duckdb-profile{,.normalized}.json` pair retains native
  and normalized operators. Replay phase metrics directly attribute destination settlement,
  checkpoint, and total package replay.
- TLC control: the retained
  `2026-07-21-p3-d18a-tlc-control-corrected-uncapped-median3-{request,run-cell}.json` and adjacent
  report record current revision `2ba50791` on historical unbounded host class
  `host-class-649c6f28be3544c8`. The older capped control and initial stale-template failure remain
  historical observations and are not used for the non-regression comparison.
- Verification: affected unit/integration suites passed (19 benchmark lab unit tests, 7 fixtures,
  6 policy tests, 11 runner tests, and 47 DuckDB tests); the required product smoke matrix passed
  5 CLI, 2 project runtime, preview/run parity, and 3 Iceberg authority/projection tests. Strict
  affected-crate Clippy, formatting, `git diff --check`, ShellCheck, Gitleaks, and graph refresh all
  passed.

## Review

Prior adversarial verdict: fail. It found stale revision labels, unlike host classes, unmatched byte
authorities, a macro spill field presented as observed, missing product replay attribution, weak
nested-list comparator coverage, unretained requests/specs/profiles, unexplained CPU dispersion,
and an overbroad claim that the hot path was unchanged.

Resolution: every performance claim above now uses current revision `2ba50791`; both wide cells use
the same 16 GiB host class and exact row/logical/physical authorities; requests, run-cell specs,
reports, systemd logs, native profiles, and normalized profiles are retained; native DuckDB temp
storage is named as spill authority; replay phase metrics are serialized by the product; the
recursive list binding has a real bind/execute test; macro CPU dispersion is disclosed as
inconclusive; and the absence-by-default profiling scope is stated narrowly. The uncapped TLC
control uses the historical comparison host class. Final independent verdict pending.

## Retrospective

- Exact production artifacts expose workload shape that synthetic wide tables miss: the 3.5M-row
  package represents roughly 115.4 GB of DuckDB result vectors and 7.2 billion field positions, so
  row/s alone badly understates work.
- A raw comparator must be schema-complete and semantics-labeled. Its stable loss to the product
  path prevents replacing a faster product implementation merely because the comparator is called
  a roofline.
- Native profiling belongs behind an absent-by-default destination-owned switch. It identifies the
  sink/storage floor without changing package identity, runtime orchestration, or the hot path.
- Benchmark templates are versioned inputs. Reusing an artifact across an intentionally broken
  customer-zero contract is not a valid control; prepare a current pin outside the timed region.
- Comparable evidence requires identical semantic counters and host-class identity, not merely the
  same EC2 instance. Retaining executable requests and run-cell specs makes that distinction
  auditable instead of relying on prose.
- Phase attribution should be a generic runtime product surface. Destination-specific profiling
  answers native operator questions; replay-owned phases answer lifecycle questions without leaking
  DuckDB into orchestration.
