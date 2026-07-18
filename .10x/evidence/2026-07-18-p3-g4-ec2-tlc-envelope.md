Status: recorded
Created: 2026-07-18
Updated: 2026-07-18

# P3 G4 EC2 TLC envelope measurement

## Observation

The dedicated EC2 benchmark host (`host-class-95da083e15eebd1c`) confirms that the current full-year TLC Hugging Face mirror gap is real, but the owner has shifted from remote transfer to CDF package/destination hot path on this host.

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

This supports G4 by moving the TLC envelope evidence from laptop triage to host-labeled EC2 measurement. It challenges the prior working assumption that remote transfer overlap remains the dominant owner: on EC2, remote CDF is only about 2.6 seconds slower than local CDF for the same files, while both spend about 33 seconds in package/destination execution. It also challenges the assumption that filesystem Parquet destination is a fast source/package split on all hosts; current staged Parquet ingress can throttle upstream so severely that the EC2 local Parquet-destination control times out.

## Limits

This is a single EC2 host class and one public mirror. It does not close G4 because the full-year TLC target is still failing. The raw native DuckDB `CREATE TABLE AS SELECT * FROM read_parquet(...)` floor is not yet recorded on the host because the benchmark host currently has no DuckDB CLI/Python module and the lab reference worker only covers a narrower internal DuckDB read/count path. The next benchmark-lab improvement should add a first-class native DuckDB Parquet ingest reference cell instead of relying on ad hoc host tooling.
