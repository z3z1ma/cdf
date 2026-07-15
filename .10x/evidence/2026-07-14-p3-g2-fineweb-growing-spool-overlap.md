Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# FineWeb generation-bound growing-spool overlap

## Observation

CDF loaded the 2,147,509,487-byte Hugging Face FineWeb Parquet object over HTTPS into a 2,205,203,006-byte governed package, committed 1,058,640 rows to DuckDB, verified the receipt, and committed the checkpoint in 16.21 seconds. An immediately subsequent sequential curl of the exact URL took 14.70 seconds. CDF therefore completed the full governed path at 1.10x the contemporaneous transfer roofline rather than the earlier 4.84x gap.

The run's package-execution wall was 15.637 seconds. Cumulative phase metrics were 14.495 seconds decode (including network wait), 0.085 seconds validation/normalization, 6.622 seconds segment encode, 2.004 seconds persist/hash, and 0.022 seconds package finalize. Segment work overlapped the sequential transfer rather than starting after it. DuckDB receipt production took 0.191 seconds and the checkpoint gate took 0.001 seconds.

## Procedure

1. Built the CLI with `CARGO_BUILD_JOBS=12 cargo build --release -p cdf-cli --bin cdf`.
2. Created an isolated project at `/Users/alexanderbut/code_projects/tmp/cdf-overlap-bench`, configured the public FineWeb URL plus its explicit Hugging Face/CAS egress allowlist, and pinned the schema from 3,926,176 bytes of Parquet footer metadata.
3. Ran `/usr/bin/time -lp .../target/release/cdf run fineweb.documents --color never --progress never -v`.
4. Observed run `run-451e96ec91e3d62c043b225d4f6611c7`, package `pkg-fineweb-documents-27531-1784077019128012000`, 115 segments, 1,058,640 receipt rows, and a committed checkpoint. Wall time was 16.21 seconds; maximum resident set size was 3,093,168,128 bytes.
5. Immediately ran `/usr/bin/time -lp curl -L --fail --silent --show-error --output /dev/null <same URL>`. Wall time was 14.70 seconds.
6. Read the eight `phase_measured` rows from the isolated SQLite run ledger to obtain the phase values above.
7. Ran `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --lib`: 46 tests passed after the weak-generation guard was added.
8. Ran `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-files --all-targets --no-deps -- -D warnings` and `git diff --check`: both passed.

## What it supports or challenges

- The strong-generation growing byte source overlaps one sequential body transfer with seekable decode without adding a Parquet branch to source orchestration. The format driver continues to consume only `ByteSource`.
- The full finite object length is reserved from the shared spill budget before transfer. Exact ranges not yet in the readable prefix wait, except for the bounded 32 MiB generation-bound tail window needed to bootstrap seekable metadata.
- The deterministic gated-source test proves an already-written prefix is read locally, one bounded tail request may read the same strong upstream generation, a middle request remains pending until the sequential writer reaches it, and all memory/spill ownership releases at completion.
- A separate guard proves weak/unversioned input cannot enter growing-spool overlap before disk admission. The existing verified full-spool path remains its only seekable option.
- The previous original debug smoke was 92.58 seconds and the earlier curl was 19.116 seconds. Those older values establish the defect's severity but are not used as an isolated release-build speedup claim.

## Limits

- This is one public endpoint and one host observation; CDN/network conditions vary. The recorded ratio uses the curl run immediately after CDF to reduce, not eliminate, that variance.
- Peak RSS remains about 3.09 GB. The run is network-bound, but B2/F still own byte-targeted batch/queue residency and the constant-memory envelope.
- The present tail window is a fixed controller default and transfer/range/waste telemetry is incomplete.
- The finite spool retains the whole object through decode. G2 still owns generation-bound selective ranges and monotone prefix eviction for objects larger than the disk envelope; A8 owns bounded rolling replay retention for unbounded non-pausable streams.
- This slice does not close G2, G3, or G4: adaptive high-BDP control, cache, retry/throttle chaos, jobs parity, multi-provider live cells, and full-year TLC remain outstanding.
