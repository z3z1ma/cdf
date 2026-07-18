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
- Remaining owner: package/destination hot path. Local source read drops to about `2.02s` while `destination_ingress` remains about `32.82s`; remote adds `13.96s` aggregate growing-spool source read but wall remains destination/package dominated.

Native DuckDB reference cell, same tuned host and same 12 local TLC files:

- Reference worker: `DuckDbParquetIngest`, materializing a persistent DuckDB database with `CREATE TABLE native_ingest AS SELECT * FROM read_parquet([...])`, three warm samples through `cdf-p3-lab run-cell`.
- Median native ingest wall: `4.174575150s`; median throughput: 9,862,014 rows/s and 166,005,327 physical bytes/s; peak RSS about `3.26 GiB`.
- Work matched the CDF cells: 41,169,720 rows and 693,001,713 physical input bytes for every sample.
- With the tuned raw parallel download floor of `1.48s`, the composite native floor is about `5.65s`; the 1.5x G4 ceiling is therefore about `8.47s`. The current CDF HF default wall of `36.98s` is about `4.37x` that ceiling and about `6.55x` the raw+native floor.

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

## What it supports or challenges

This supports G4 by moving the TLC envelope evidence from laptop triage to host-labeled EC2 measurement. The tuned-host rerun challenges the prior working assumption that remote transfer overlap remains the sole dominant owner: CDF can complete the HF mirror full-year run on the tuned host, but about 33.5 seconds of the wall is still package/destination execution and about 32.4 seconds is DuckDB ingress. It also challenges the assumption that fast local input is a simple non-regression control: the tuned-host local-files-to-DuckDB control entered a low-CPU futex wait after partial writes, which suggests a backpressure/channel/destination-ingress scheduling bug that slower remote input can mask.

The scheduler-default repair supports retaining a default-admission change: the pathological local fast-source/DuckDB run moved from low-CPU non-completion to a completed 33.83-second run, while the remote HF run stayed in the same class as the previous tuned-host remote measurement. The evidence also narrows the next G4 owner: the admission bug is no longer hiding the local control, and the remaining gap is destination/package execution rather than source-frontier starvation.

The native DuckDB reference cell turns the G4 target from an approximation into a measured host-local comparison. It challenges any interpretation that 33–37 seconds is merely public network or benchmark-host noise: DuckDB can persistently materialize the same 12 files in about 4.17 seconds once the bytes are local, and the raw network floor is about 1.48 seconds. The next retained data-plane work must therefore reduce CDF's destination/package hot path by multiples, not by tuning small remote-read constants.

The pre-tuning run still has diagnostic value: it proved the benchmark host was originally too slow at durable storage for promotion evidence. After tuning, the HF wall improved from `43.39s` to `36.51s`, but the result is still far above the P3 target envelope.

## Limits

This is a single EC2 host class and one public mirror. It does not close G4 because the full-year TLC target is still failing. The native DuckDB reference cell is now in the lab worker and measured on the host, but it is still a favorable reference: it omits CDF package manifests, receipts, checkpoints, normalizer/provenance columns, and the destination commit gate. That bias is recorded in the machine observation.
