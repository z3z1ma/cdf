Status: recorded
Created: 2026-07-18
Updated: 2026-07-18

# Final current-tree correctness and performance pass

## Observation

The current product path at commit `5496254820f94fa72958b5a0e74ceb048ae230ee` preserves the controlled full-year TLC-to-DuckDB envelope and the enforced-memory control while repairing the previously reported live file-source failures. A three-sample `c7i.4xlarge` EC2 cell processed all `41,169,720` rows with a `10.477064330s` median, `3,929,509` rows/s, `3,958,956,032` bytes peak process RSS, zero spill, and no cgroup pressure/OOM events. The comparable retained stock-scanner median was `10.255642670s`; the current result is 2.16% slower, below the lab's 10% regression threshold. A separate one-sample `MemoryMax=6G` cell completed in `10.426919222s`, peaked at `3,916,914,688` process RSS and `6,175,449,088` cgroup bytes under the exact `6,442,450,944`-byte ceiling, and recorded zero pressure/OOM/spill events.

Manual control-flow tracing found one coherent authority chain rather than source/destination-specific orchestration:

- cold discovery inventories metadata, applies the explicit file/within-file coverage policy, freezes one run-local schema, and compiles the final plan before extraction; pinned runs compile directly from the snapshot and reconcile physical observations during the extraction stream;
- `ScanPlan` and the runtime schedule each carry one closed inline-or-external partition authority, while file inventories bind the discovery identity and executable tasks bind the full compiled execution identity;
- remote high-coverage seekable objects use the transport-neutral sequential/growing-spool policy, selective strong-generation reads retain exact-range access, and ordinary row streams remain directly bounded;
- the engine validates compiled source, partition schedule, schema, operator graph, validation program, expression plan, and segmentation authorities before package mutation;
- canonical segments hash while writing and package verification/settlement consume segment and manifest authority incrementally rather than reconstructing the package in memory;
- generic orchestration branches on destination ingress capabilities, not destination identity; DuckDB owns its canonical scanner, Parquet owns staged publication, and Postgres owns binary COPY inside their adapter crates;
- a verified destination receipt precedes checkpoint commit, and package lifecycle state follows the committed checkpoint.

The pass did expose one workload-specific performance residual. The 2,147,509,487-byte local FineWeb file completed correctly in `13.76s`, but package execution took `5.974s` and the sole DuckDB final binding took `7.377s`, with `4,847,747,072` bytes peak RSS. Historical pre-stock-scanner whole-path observations were in the 5–8 second class because staged DuckDB consumption overlapped package production. Current replay isolation took `9.60s` at default resources; eight native threads did not materially improve it (`9.29s`), while 1 GiB and 2 GiB DuckDB limits reduced RSS at large wall penalties (`16.89s` and `12.92s`). This is not the former HTTP range-read regression and it is not a broad current-tree regression: current remote FineWeb completed in `48.31s` while a same-session raw `curl` took `55.96s`, and the controlled TLC cell remains stable. The residual is owned by `.10x/tickets/2026-07-18-p3-d17-duckdb-wide-string-overlap.md`.

## Procedure

1. Traced the CLI/compiler, discovery, file-source, kernel partition authority, engine execution, package finalization/verification, destination ingress, receipt, checkpoint, and package-lifecycle code paths directly. Searched the dependency graph to confirm kernel/runtime/engine/project do not depend on concrete first-party source or destination crates; concrete adapters compose at the CLI root.
2. Ran a fresh-state copy of the user's `/Users/alexanderbut/code_projects/tmp` project:
   - `github.userdata`: 5 HTTPS Parquet files, 5,000 rows, 5 segments, `3.41s` command wall;
   - `imdb.training_data`: 25,000 rows, 2 segments, `3.87s` after adding the endpoint's actual `us.aws.cdn.hf.co` redirect host to the disposable allowlist;
   - `tlc.yellow`: refreshed the pre-current-format snapshot, then loaded 2,964,624 rows in 16 segments in `3.32s`; an immediate rerun was a `0.44s` manifest no-op;
   - `fineweb.documents`: refreshed the snapshot and loaded 1,058,640 rows/115 segments from the 2.147 GB public object in `48.31s`; an immediately paired raw curl took `55.96s` under variable public-network conditions;
   - `fineweb_local.documents`: loaded the exact cached 2.147 GB object in a clean workspace and isolated package replay under default and explicit native-resource controls.
3. Ran `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 tools/product-smoke-matrix.sh`: all 11 selected product tests passed, covering HTTP add/autopin/run, local discovery/run, package verification and source-free replay, multi-file manifest incrementality/no-op, Parquet destination settlement, preview/run parity, and Iceberg projection/task authority.
4. Reused the tuned CDF EC2 benchmark host (`c7i.4xlarge`, 16 logical CPUs, 30 GiB RAM, 250 GiB gp3 at 16,000 IOPS/1,000 MiB/s), synchronized current committed product source, built release `cdf` and the lean measurement runner with downloaded prebuilt DuckDB, and passed strict measurement preflight.
5. Created a fresh current-format workspace over the retained twelve local 2024 TLC Parquet files, repinned all footer metadata with the current compiler, and ran the standard three-sample measured-command cell. Fetched `.10x/evidence/.storage/2026-07-18-final-pass-current-tlc.json`.
6. Repeated one sample inside a transient systemd user service with `MemoryMax=6G`. Fetched `.10x/evidence/.storage/2026-07-18-final-pass-current-tlc-6g.json`.

The EC2 revision is labeled `54962548...+dirty` because the synchronized checkout contained concurrent edits to destination-envelope evidence, `.gitignore`, one benchmark test-policy file, and generated documentation. No uncommitted product source participated in the release `cdf` binary; its product source is exactly commit `54962548`.

## What it supports or challenges

- Supports current compiler/runtime/package/destination authority composition and the receipt-before-checkpoint gate through both code-path inspection and product executions.
- Supports closure of the previously observed HTTP HEAD, missing remote identity, schema-hash mismatch, discovery-to-pinned inventory reuse, and unchanged-manifest regressions on real public resources.
- Supports the claim that current full-year TLC throughput remains within ordinary controlled-host variance and that the same workload completes under an enforced 6 GiB cgroup ceiling without spill or pressure events.
- Challenges treating numeric/narrow TLC as sufficient coverage for the DuckDB destination hot path. Wide, long-string payloads need a permanent promotion cell because the final canonical scan loses the former package/destination overlap even though the TLC path improved dramatically.

## Limits

The public HTTP observations are single samples and only their same-session CDF-versus-curl relationship is retained; they are not promotion evidence. The local FineWeb samples are diagnostic single runs on macOS and establish a credible workload gap, not a variance-aware target. The EC2 three-sample cell is the performance authority for the current TLC shape only. This pass does not claim the one-terabyte F4 law, long-running streaming retention, every foreign-runtime mode, or every open connector. It does not mutate or claim the concurrent Athena worker's files.
