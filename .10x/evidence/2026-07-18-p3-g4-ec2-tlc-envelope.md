Status: recorded
Created: 2026-07-18
Updated: 2026-07-18

# P3 G4 EC2 TLC envelope measurement

## Observation

The dedicated EC2 benchmark host (`host-class-95da083e15eebd1c`) confirms that the current full-year TLC Hugging Face mirror gap is real, but the owner has shifted from remote transfer to CDF package/destination hot path on this host.

Important correction: the first EC2 G4 run below was taken before `.10x/tickets/2026-07-18-p3-l6-ec2-benchmark-host.md` corrected the host's root volume from default gp3 3,000 IOPS / 125 MiB/s to gp3 16,000 IOPS / 1,000 MiB/s. Those first numbers remain useful for diagnosing the bad benchmark-host storage floor, but storage-sensitive conclusions must use the tuned-host rerun.

Tuned-host measurements:

- Raw parallel `curl` of the 12 TLC mirror files: 661 MiB in `1.48s` after `sync`.
- `cdf schema pin tlc.yellow` over the 12 remote Parquet files: `8.63s`, 12 matched files, format metadata coverage.
- CDF remote TLC mirror to DuckDB: `36.51s`, 41,169,720 rows, 215 segments, package `pkg-tlc-yellow-53547-1784362780394986180`.
- Tuned HF phase telemetry: `source_read=16.14s` using growing spool, `decode=10.97s`, `segment_encode=9.62s`, `persist_hash=1.62s`, `destination_ingress=32.35s`, `package_execution=33.51s`, and `destination_write_receipt=0.87s`.
- CDF local files to DuckDB did not produce a valid throughput cell: after local schema pin completed in `0.04s`, the local run was terminated at `304.55s` with the CDF process waiting on `futex_wait_queue`, about `2.7%` CPU, about `2.0 GiB` RSS, and only about `345 MiB` written. This is a new high-priority local-fast-source blocking signature, not a benchmark pass/fail number.

Scheduler-default repair measurements, same tuned host:

- Code under test: `5e2f0e0a2b3fb4602a5e9368939982e2bc49b2f4+dirty`, where the dirty diff joins `StagedDurableSegments` destination in-flight pressure into default job admission and leaves explicit `--jobs` as the operator overdrive knob.
- CDF local files to DuckDB now completes in `33.83s` (`user=46.27`, `sys=2.90`, max RSS about `2.16 GiB`) for 41,169,720 rows and 215 segments, instead of the previous `304.55s` low-CPU termination.
- CDF remote HF mirror to DuckDB completes in `36.98s` (`user=46.96`, `sys=4.90`, max RSS about `2.27 GiB`) for 41,169,720 rows and 215 segments, preserving the tuned remote envelope class.
- Both local and remote plans report `effective_jobs.jobs = 2` and limiting factor `staged_destination_in_flight`; source capability remains 16-way. This confirms the default is a generic downstream-pressure admission join, not a destination-identity branch or hidden hard cap. Explicit job configuration remains outside this default join.
- A supervised explicit-overdrive probe with `--jobs 3` rejected raising the default source admission window: the same full-year local TLC-to-DuckDB workload timed out through `measure-cdf` with `CDF command exceeded worker timeout of 119000ms`, and immediate process inspection found no orphaned `cdf` worker. Machine artifact: `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-local-jobs3-measured.json`.
- Remaining owner: package/destination hot path. Local source read drops to about `2.02s` while `destination_ingress` remains about `32.82s`; remote adds `13.96s` aggregate growing-spool source read but wall remains destination/package dominated.

Native DuckDB reference cell, same tuned host and same 12 local TLC files:

- Reference worker: `DuckDbParquetIngest`, materializing a persistent DuckDB database with `CREATE TABLE native_ingest AS SELECT * FROM read_parquet([...])`, three warm samples through `cdf-p3-lab run-cell`.
- Median native ingest wall: `4.174575150s`; median throughput: 9,862,014 rows/s and 166,005,327 physical bytes/s; peak RSS about `3.26 GiB`.
- Work matched the CDF cells: 41,169,720 rows and 693,001,713 physical input bytes for every sample.
- With the tuned raw parallel download floor of `1.48s`, the composite native floor is about `5.65s`; the 1.5x G4 ceiling is therefore about `8.47s`. The current CDF HF default wall of `36.98s` is about `4.37x` that ceiling and about `6.55x` the raw+native floor.

Persistent DuckDB Arrow-appender diagnostic, same tuned host:

- Reference worker: `DuckDbArrowAppend`, materializing a persistent DuckDB database by appending synthetic TLC-shaped Arrow batches with the `_cdf_row_key` provenance column, three warm samples through `cdf-p3-lab run-cell`.
- Median Arrow-appender wall with explicit `CHECKPOINT`: `31.831857687s`; median throughput: 1,293,349 rows/s, about 5.97 GiB logical Arrow bytes per sample, and about 1.14 GiB persistent DuckDB bytes per sample. Peak RSS was about `1.41 GiB`.
- Median Arrow-appender wall without explicit `CHECKPOINT`: `31.986515768s`; median throughput: 1,287,096 rows/s. This is effectively the same duration class, so the diagnostic does not support blaming an explicit checkpoint call for CDF's current wall.
- This diagnostic omits CDF package manifests, validation verdicts, receipts, checkpoints, mirrors, source Parquet reads, and CDF's Arrow C stream bridge. It is therefore not a CDF-equivalence reference. Its value is narrower and decisive: persistent DuckDB Arrow appender throughput at full-year TLC row count is in the same wall-time class as CDF's measured `33.955522533s` local default cell.
- Current interpretation: the remaining G4 gap is not caused by 1,024-row append batches, local source read, remote range reads, or simple CDF scheduler overdrive. It is dominated by DuckDB's persistent Arrow appender/materialization strategy at this scale. The next retained fix must change the DuckDB destination bulk strategy itself or prove a faster adapter-owned path against this host-labeled cell.

DuckDB Parquet-staged ingest diagnostic, same tuned host:

- Reference worker: `DuckDbParquetStagedIngest`, writing synthetic TLC-shaped Arrow batches with `_cdf_row_key` to a Parquet handoff file, then materializing a persistent DuckDB table through native `read_parquet`, three warm samples through `cdf-p3-lab run-cell`.
- Median wall: `20.615132934s`; median throughput: 1,997,063 rows/s, about 5.97 GiB logical Arrow bytes per sample, and about 7.0 GiB combined Parquet-handoff plus DuckDB physical bytes per sample. Peak RSS was about `7.22 GiB`.
- This is materially faster than the persistent Arrow-appender diagnostic (`31.83s`) but still far above the G4 1.5x ceiling of about `8.47s`, and its peak memory is not acceptable as a default under CDF's constant-memory doctrine. It is therefore a promising adapter-owned direction, not a production default.
- Current interpretation: native DuckDB Parquet ingest is the right shape, but a naive destination-owned Parquet handoff is too heavy unless it becomes bounded and substantially faster. If pursued, the production design must account disk/RSS through the memory/spill ledger and must beat the host-labeled CDF baseline before retention.

Tuned machine artifacts:

- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-tuned-tlc-summary.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-tuned-hf-tlc-duckdb-run.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-tuned-hf-tlc-pin.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-scheduler-default-summary.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-scheduler-default-local-run.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-scheduler-default-hf-run.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-native-duckdb-ingest-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-native-duckdb-ingest-run-cell.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-native-duckdb-ingest-reference.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-arrow-append-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-arrow-append-run-cell.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-arrow-append-reference.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-arrow-append-no-checkpoint-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-arrow-append-no-checkpoint-run-cell.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-arrow-append-no-checkpoint-reference.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-parquet-staged-ingest-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-parquet-staged-ingest-run-cell.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-parquet-staged-ingest-reference.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-local-default-measured.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-local-jobs3-measured.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-local-package-read.json`

Same-host measurements:

- Raw parallel `curl` of the 12 TLC mirror files: 661 MiB in `1.22s`.
- `cdf schema pin tlc.yellow` over the 12 remote Parquet files: `8.68s`, `151340` discovery bytes, 12 matched files, format metadata coverage.
- CDF remote TLC mirror to DuckDB: `43.39s`, 41,169,720 rows, 215 segments, package `pkg-tlc-yellow-49275-1784361572388571431`.
- CDF local TLC files to DuckDB using the already-downloaded 12 files: `40.75s`, 41,169,720 rows, 215 segments, package `pkg-tlc-yellow-49861-1784361727249560931`.
- CDF local TLC files to filesystem Parquet destination: timed out at `90.08s` after only 32 Arrow segments / two partitions were present in the partial package and about 408 MiB had been written to the destination root.

The remote CDF run's phase telemetry reports aggregate `source_read=13.77s`, `decode=10.17s`, `segment_encode=9.86s`, `persist_hash=1.64s`, `destination_ingress=33.11s`, and `package_execution=33.91s`. The local CDF run reports nearly identical destination/package costs, with local decode dropping to `0.23s`. Therefore the EC2 G4 miss is not the old range-read pathology or public-provider bandwidth; it is dominated by package/destination execution, especially DuckDB ingress, with Parquet staged ingress showing an additional low-CPU timeout.

Machine artifacts:

- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-tlc-summary.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-hf-tlc-duckdb-run.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-local-tlc-duckdb-run.json`

## Procedure

On the EC2 host created by `.10x/tickets/2026-07-18-p3-l6-ec2-benchmark-host.md`:

1. Synchronized the clean repository and rebuilt release `cdf` / `cdf-p3-lab`.
2. Synchronized the user's scratch CDF workspace in minimal mode.
3. Created a remote-only 12-month TLC workspace by changing `resources/tlc.toml` to `glob = "yellow_tripdata_2024-{01..12}.parquet"` against the Hugging Face TLC mirror.
4. Repinned `tlc.yellow`; the schema authority guard correctly rejected the one-month pin before the repin.
5. Downloaded all 12 mirror files with `curl -L` and `xargs -P12`.
6. Ran CDF remote-to-DuckDB, local-to-DuckDB, and local-to-filesystem-Parquet controls with `/usr/bin/time` and stored JSON run reports where available.
7. Re-synchronized and rebuilt the same host with the staged-destination default-admission patch, then ran default local and HF mirror 12-month TLC-to-DuckDB controls without explicit `--jobs`.
8. Added a persistent native DuckDB Parquet ingest reference workload to `cdf-p3-lab`, rebuilt the lab binary on the same host, and ran a three-sample warm `run-cell` against the same 12 local TLC Parquet files.
9. Added a lab-only `cdf-p3-lab package-read` diagnostic and ran it against the retained full-year TLC package from the local default cell: `pkg-tlc-yellow-56794-1784364958724043936`.
10. Added a persistent DuckDB Arrow-appender reference workload to `cdf-p3-lab`, rebuilt the lab binary on the same host, preflighted the host/build/workspace, and ran three-sample warm `run-cell` controls over 41,169,720 synthetic TLC-shaped rows with `_cdf_row_key` enabled, once with checkpointing enabled and once without explicit checkpointing.
11. Added a DuckDB Parquet-staged ingest reference workload to `cdf-p3-lab`, rebuilt the lab binary on the same host, preflighted the host/build/workspace, and ran a three-sample warm `run-cell` over 41,169,720 synthetic TLC-shaped rows with `_cdf_row_key` enabled, a 65,536-row Parquet row-group target, and no explicit DuckDB checkpoint.

## What it supports or challenges

This supports G4 by moving the TLC envelope evidence from laptop triage to host-labeled EC2 measurement. The tuned-host rerun challenges the prior working assumption that remote transfer overlap remains the sole dominant owner: CDF can complete the HF mirror full-year run on the tuned host, but about 33.5 seconds of the wall is still package/destination execution and about 32.4 seconds is DuckDB ingress. It also challenges the assumption that fast local input is a simple non-regression control: the tuned-host local-files-to-DuckDB control entered a low-CPU futex wait after partial writes, which suggests a backpressure/channel/destination-ingress scheduling bug that slower remote input can mask.

The scheduler-default repair supports retaining a default-admission change: the pathological local fast-source/DuckDB run moved from low-CPU non-completion to a completed 33.83-second run, while the remote HF run stayed in the same class as the previous tuned-host remote measurement. The evidence also narrows the next G4 owner: the admission bug is no longer hiding the local control, and the remaining gap is destination/package execution rather than source-frontier starvation.

The native DuckDB reference cell turns the G4 target from an approximation into a measured host-local comparison. It challenges any interpretation that 33–37 seconds is merely public network or benchmark-host noise: DuckDB can persistently materialize the same 12 files in about 4.17 seconds once the bytes are local, and the raw network floor is about 1.48 seconds. The next retained data-plane work must therefore reduce CDF's destination/package hot path by multiples, not by tuning small remote-read constants.

The L6 `measure-cdf` local default cell makes the retained CDF baseline itself a standard benchmark-lab observation: full-year local TLC to DuckDB completed in `33.955522533s`, with 41,169,720 rows and ten extracted phase metrics. The decisive phases were `destination_ingress=32.916s` and `package_execution=33.136s`; local `source_read` and `decode` together were under 2.3 seconds. This confirms the current G4 owner as destination/package execution under host-labeled evidence.

The supervised `--jobs 3` failure challenges a tempting scheduler shortcut. Increasing source jobs above the staged destination's default pressure join does not recover the old local floor on the EC2 host; it reintroduces a non-completing overdrive shape. Future G4 work should not promote higher default source admission without a different staged-ingress/backpressure architecture and same-host proof.

The package-read diagnostic splits replay from live execution. Reading and decoding every Arrow IPC batch in the retained 1.51 GB package took `16.733112449s` for 633 batches, 215 segments, and 41,169,720 rows. This explains a large part of the `45.97s` package-replay-to-DuckDB result, but it does not by itself explain live staged ingress because live `LiveStagedSegmentReader` hands retained `RecordBatch`es to DuckDB without reopening IPC files. The next production candidate therefore needs finer destination-internal timing or a bulk strategy change; simply increasing source jobs or coalescing append batches remains rejected by evidence.

The persistent DuckDB Arrow-appender diagnostic resolves that fork. The appender alone takes `31.831857687s` with explicit checkpointing and `31.986515768s` without explicit checkpointing at the same row count, which accounts for almost all of the current CDF local wall. This challenges the older small/in-memory DuckDB appender evidence as a representative full-scale proxy: it remains true for its microbench scope, but it is not sufficient for G4. The next code path must either use DuckDB's own Parquet/native bulk ingest facilities in an adapter-owned way while preserving CDF evidence semantics, or find another measured persistent Arrow/materialization route that beats the `33.955522533s` CDF local default and the `31.831857687s` appender diagnostic.

The Parquet-staged ingest diagnostic tests that next shape. It improves the persistent materialization wall to `20.615132934s`, proving DuckDB-native Parquet ingest can help even after paying a handoff-write cost. It also challenges this naive handoff as a default: peak RSS reached about `7.22 GiB` and the wall remains far beyond the G4 ceiling. The production lesson is narrower: a DuckDB-native bulk path is likely required, but it must be ledger-accounted and materially leaner than a single giant Parquet handoff.

The pre-tuning run still has diagnostic value: it proved the benchmark host was originally too slow at durable storage for promotion evidence. After tuning, the HF wall improved from `43.39s` to `36.51s`, but the result is still far above the P3 target envelope.

## Limits

This is a single EC2 host class and one public mirror. It does not close G4 because the full-year TLC target is still failing. The native DuckDB reference cell is now in the lab worker and measured on the host, but it is still a favorable reference: it omits CDF package manifests, receipts, checkpoints, normalizer/provenance columns, and the destination commit gate. The Arrow-appender and Parquet-staged diagnostics are synthetic and omit CDF package/receipt/source work plus some adapter/runtime boundary costs. Those biases are recorded in the machine observations.
