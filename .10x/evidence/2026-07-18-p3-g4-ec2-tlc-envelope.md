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

Tuned machine artifacts:

- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-tuned-tlc-summary.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-tuned-hf-tlc-duckdb-run.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-tuned-hf-tlc-pin.json`

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

## What it supports or challenges

This supports G4 by moving the TLC envelope evidence from laptop triage to host-labeled EC2 measurement. The tuned-host rerun challenges the prior working assumption that remote transfer overlap remains the sole dominant owner: CDF can complete the HF mirror full-year run on the tuned host, but about 33.5 seconds of the wall is still package/destination execution and about 32.4 seconds is DuckDB ingress. It also challenges the assumption that fast local input is a simple non-regression control: the tuned-host local-files-to-DuckDB control entered a low-CPU futex wait after partial writes, which suggests a backpressure/channel/destination-ingress scheduling bug that slower remote input can mask.

The pre-tuning run still has diagnostic value: it proved the benchmark host was originally too slow at durable storage for promotion evidence. After tuning, the HF wall improved from `43.39s` to `36.51s`, but the result is still far above the P3 target envelope.

## Limits

This is a single EC2 host class and one public mirror. It does not close G4 because the full-year TLC target is still failing. The raw native DuckDB `CREATE TABLE AS SELECT * FROM read_parquet(...)` floor is not yet recorded on the host because the benchmark host currently has no DuckDB CLI/Python module and the lab reference worker only covers a narrower internal DuckDB read/count path. The next benchmark-lab improvement should add a first-class native DuckDB Parquet ingest reference cell instead of relying on ad hoc host tooling.
