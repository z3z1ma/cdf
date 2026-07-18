Status: active
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md

# P3 D14: DuckDB nanoarrow 0.8.0 LZ4 revalidation

## Scope

Correct D11's overbroad LZ4 conclusion by building the DuckDB nanoarrow extension against the signed Apache nanoarrow `0.8.0` release with `NANOARROW_IPC_WITH_LZ4=ON`, then re-run the direct current-CDF-segment and full-year TLC Arrow IPC ingestion probes on the controlled EC2 benchmark host. Determine whether direct consumption of CDF's canonical LZ4 Arrow IPC segments removes the redundant destination handoff and materially closes the DuckDB G4 envelope gap.

## Non-goals

- No package artifact-format change.
- No uncompressed duplicate handoff.
- No generic runtime branch for DuckDB, nanoarrow, Arrow IPC paths, or extension installation.
- No product default change before full-CDF EC2 evidence meets the retained performance and memory thresholds.
- No unpinned extension source, nanoarrow source, build flag, or binary artifact.
- No hard-coded performance cap without a configuration authority.

## Acceptance Criteria

- The investigation records the exact DuckDB extension revision, nanoarrow `0.8.0` tag/commit, archive digest, and `NANOARROW_IPC_WITH_LZ4=ON` build configuration.
- The pinned extension loads under CDF's bundled DuckDB `v1.5.4`, reports its version, and successfully reads a generated LZ4 Arrow IPC fixture.
- The pinned extension successfully reads at least one current CDF package segment directly, or records the precise remaining incompatibility with evidence.
- EC2 evidence measures full-year TLC-shaped direct LZ4 Arrow IPC ingestion through `read_arrow(list_of_files)` with 41,169,720 rows, wall time, throughput, peak RSS, and cgroup peak.
- If the direct LZ4 path clears the generated retention threshold, a destination-owned product implementation is retained only after a full-CDF EC2 run proves it faster than the current appender baseline without duplicating package bytes or leaking destination identity into orchestration.
- D11, D12, G4, and WS-D references are corrected so the backlog reflects the new evidence and no stale claim says nanoarrow lacks LZ4 support.

## References

- `.10x/tickets/done/2026-07-18-p3-d11-duckdb-arrow-ipc-handoff-falsification.md`
- `.10x/tickets/cancelled/2026-07-18-p3-d12-duckdb-arrow-ipc-handoff-ingress.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`
- `https://github.com/apache/arrow-nanoarrow/pull/819`
- `https://github.com/apache/arrow-nanoarrow/releases/tag/apache-arrow-nanoarrow-0.8.0`

## Assumptions

- Record-backed: Apache nanoarrow `0.8.0` includes LZ4 IPC decompression from PR 819, gated by the build option `NANOARROW_IPC_WITH_LZ4`.
- Record-backed: the DuckDB community extension artifact tested by D11 was extension revision `42e4199a67c4cd0789087562a025e87e7130fdc3`, which vendors pre-LZ4 nanoarrow revision `4bf5a9322626e95e3717e43de7616c0a256179eb` and does not enable the LZ4 build option.
- Record-backed: D11's observed failure is valid only for that old extension artifact; it does not establish that nanoarrow or `read_arrow(...)` cannot consume CDF LZ4 IPC.
- User-ratified: use nanoarrow `0.8.0`, investigate thoroughly, and prioritize this correction before unrelated architecture work.
- User-ratified: performance and correctness are joint first priority; retain no potentially slower default without controlled EC2 evidence.

## Journal

- 2026-07-18: Opened after upstream verification invalidated D11's general LZ4 conclusion. Apache PR 819 merged LZ4 IPC decompression on 2025-10-27 and nanoarrow `0.8.0` released it behind `NANOARROW_IPC_WITH_LZ4`. The installed DuckDB community extension's `.info` file identifies extension revision `42e4199`; that revision fetches nanoarrow `4bf5a932` from 2025-01-09 and therefore produced the observed `Compression type with value 1 not supported by this build of nanoarrow` error.
- 2026-07-18: Built the DuckDB nanoarrow extension on the controlled EC2 host from extension revision `42e4199a67c4cd0789087562a025e87e7130fdc3`, DuckDB `v1.5.4` commit `08e34c447bae34eaee3723cac61f2878b6bdf787`, and Apache nanoarrow `0.8.0` commit `a579fbf5d192e85b6249935e117de7d02a6dc4e9`. The exact nanoarrow commit archive digest is `sha256:ed186f0b8151c323fd41a1b7cfa830abad0ac84e1657cd597da12d98fa9a4be1`; the extension was compiled with `NANOARROW_IPC_WITH_LZ4=ON` against system `lz4-devel 1.9.4`. The loaded extension reported `nanoarrow_version() = 0.8.0` under DuckDB `v1.5.4`.
- 2026-07-18: The corrected extension counted `1,048,576` rows from a generated LZ4 Arrow IPC file and `196,608` rows from an exact current canonical CDF LZ4 package segment. The previous incompatibility was entirely the pre-0.8.0 extension artifact, not CDF's segment framing or compression.
- 2026-07-18: Extended the performance lab's existing Arrow IPC reference workload with an explicit absolute-path loadable-extension variant and required nanoarrow-version assertion. Unsigned-extension enablement is confined to this lab-only variant; the community-extension probes retain their existing signed install/load path.
- 2026-07-18: Ran three warm full-year samples in an isolated `MemoryMax=24G` user cgroup against the exact 215 canonical CDF LZ4 segment files from the full-year TLC package. All samples materialized `41,169,720` rows. Wall times were `4.672500665s`, `4.558788174s`, and `4.210656148s`; median was `4.558788174s` with `113.712491ms` MAD, `9,030,847` rows/s, `2,289,385,472` bytes peak process RSS, and `4,828,008,448` bytes peak cgroup memory. No memory-high, max, OOM, or spill event occurred.
- 2026-07-18: The result clears the generated retention threshold by a wide margin: direct canonical-segment materialization is approximately `7.5x` faster than the current approximately `34s` full-CDF appender path and approximately `2.0x` faster than D11's generated uncompressed duplicate-handoff median. Its stated limit is equally important: the probe omits CDF receipt/checkpoint work and destination provenance-column construction, so a destination-owned product path must still clear the full-CDF EC2 gate before replacing the default.
- 2026-07-18: Falsified three provenance strategies on the same EC2 package before changing product code. Per-segment `row_number() OVER ()` union materialized exact ranges but took `36.76s`; per-segment positional joins took `36.51s`; both destroy the direct-read advantage and are rejected. A transaction-local sequence preserved a contiguous key range in `11.95s`, but its approximately four million voluntary context switches expose per-row sequence contention and make it a fallback, not the preferred design.
- 2026-07-18: A vectorized post-materialization strategy retained the raw floor: order-preserving `CREATE TABLE AS read_arrow(...)`, one `_cdf_row_key` column addition, and one in-transaction `UPDATE ... SET _cdf_row_key = rowid + first_row_key` completed the full 41,169,720-row materialization plus count/min/max verification in a single uncontrolled `4.50s` sample at approximately 3.23 GiB peak RSS. Setting the stored key `NOT NULL` required a separate `0.37s` uncontrolled probe. DuckDB documents materialized `rowid` as the non-window row enumeration mechanism and documents order preservation for single-table `FROM`/`SELECT`; product conformance must additionally prove that nanoarrow's ordered path list maps each segment to its expected contiguous key range at `threads=1` and `threads=N` before this can become authority.
- 2026-07-18: Added the vectorized row-key strategy to the existing controlled reference workload with explicit `row_key_start`, explicit `preserve_insertion_order=true`, exact min/max verification, and optional `NOT NULL` restoration. This keeps the next EC2 median comparable and prevents an unmeasured product prototype from becoming the benchmark.
- 2026-07-18: Landed the destination-neutral prerequisite locally. `DurableSegmentReader` now optionally exposes the exact length-bound local file represented by its staged identity; live execution supplies the hash-while-write canonical segment, and replay supplies a package-verification-bound segment object. Replay decoding is lazy: a destination consuming the canonical file performs no redundant Arrow IPC decode, while batch-oriented destinations invoke the same accounted decoder on first `next_batch()`. No generic layer names DuckDB, nanoarrow, or Arrow IPC strategy ids.

## Blockers

None.

## Evidence

- Exact controlled-host observation: `.10x/evidence/.storage/2026-07-18-p3-d14-ec2-current-package-lz4-nanoarrow-080-observation.json`.
- Upstream capability: Apache Arrow nanoarrow PR 819 and release `apache-arrow-nanoarrow-0.8.0`, linked under References.
- Focused local lab validation: `cargo fmt --all`; `CARGO_BUILD_JOBS=12 cargo check -p cdf-benchmarks --locked -j 12`; `CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks arrow_ipc_handoff_writer_emits_readable_files --locked -j 12 -- --nocapture`; and `git diff --check` all passed.
- Neutral staged-object validation: `CARGO_BUILD_JOBS=12 cargo check -p cdf-runtime -p cdf-package -p cdf-engine -p cdf-project --locked -j 12`; focused durable-file request validation passed; all 53 non-ignored `cdf-package` library tests passed; and the `cdf-project` library test command completed successfully. The local-file request test also proves length drift is rejected before destination consumption.
- Acceptance mapping: exact pin/build configuration is recorded in the Journal; extension load/version, generated LZ4, and current-segment compatibility are proved by the controlled-host probes; the raw JSON records every full-year sample, row count, throughput, RSS, cgroup peak/events, and measurement biases. Full-CDF product integration remains open.

## Review

Pending.

## Retrospective

Pending.
