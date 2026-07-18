Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/cancelled/2026-07-18-p3-d10-duckdb-stream-scan-staged-ingress.md, .10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md

# P3 D11: DuckDB Arrow IPC handoff falsification

## Scope

Benchmark and falsify DuckDB's community `nanoarrow`/`arrow` extension path for ingesting CDF-compatible Arrow IPC handoff files through `read_arrow(...)`, before productionizing any DuckDB handoff strategy. The primary question is whether CDF can write validated/normalized batches with `_cdf_row_key` into Arrow IPC stream/file handoff objects and have DuckDB materialize them substantially faster than the persistent appender and tuned Parquet handoff paths. If Arrow IPC is unavailable, unreliable, incompatible, or not clearly faster, record the kill decision and continue only with the tuned Parquet handoff fallback under a separate implementation ticket.

## Non-goals

- No production default change.
- No package artifact format change.
- No generic runtime branch for DuckDB, Arrow IPC, Parquet, path ids, or handoff files.
- No dependency from `cdf-dest-duckdb` to `cdf-dest-parquet` during fallback work; reusable writer policy must live in a neutral crate or remain DuckDB-owned without leaking into orchestration.
- No stream-scan/`duckdb_arrow_scan` revival; D10 is terminal.
- No custom DuckDB extension or unpinned opaque extension dependency accepted as production surface during this ticket.
- No merge path.
- No unbounded temporary disk growth in any probe intended to shape production: handoff bytes must have an obvious bounded-spill path.
- No hard-coded performance caps without knobs.

## Acceptance Criteria

- EC2 evidence proves whether the pinned bundled DuckDB crate can `INSTALL nanoarrow FROM community; LOAD nanoarrow; SELECT count(*) FROM read_arrow('small.arrows')`.
- EC2 evidence records a direct-read attempt against at least one current CDF Arrow IPC segment/package data file. If current package segment compression/framing is incompatible, record that precisely and do not mutate the package format in this ticket.
- EC2 evidence records generated full-year TLC-shaped Arrow IPC handoff materialization through DuckDB `read_arrow(list_of_files)`, with `_cdf_row_key` already materialized, 41,169,720 rows, and memory/RSS reported.
- G4 records an explicit keep/kill conclusion comparing Arrow IPC handoff against current appender (`~34s` CDF, `31–32s` isolated) and tuned Parquet handoff (`~10.47s`, ~2.75 GiB RSS).
- Retain the direction only if Arrow IPC handoff is clearly faster than tuned Parquet handoff or materially closer to the `8–12s` roofline while staying under configured memory budgets or presenting an obvious bounded-spill path.
- If retained, open a fresh implementation ticket for the production destination-owned bulk path; if killed, open/activate the tuned Parquet handoff implementation ticket as fallback.

## References

- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`
- `.10x/tickets/cancelled/2026-07-18-p3-d10-duckdb-stream-scan-staged-ingress.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/decisions/compact-lossless-destination-row-provenance.md`
- `.10x/decisions/destination-runtime-composition-boundary.md`

## Assumptions

- Record-backed: current full-year local TLC-to-DuckDB appender baseline is approximately `34s` on the EC2 host, with destination/package materialization dominating.
- Record-backed: raw persistent Arrow appender and raw data-chunk append both remain near `32s`; they do not justify a production rewrite.
- Record-backed: tuned synthetic Parquet handoff with `_cdf_row_key`, no checkpoint, and 128–256 MiB row-group targets materializes 41,169,720 TLC-shaped rows in approximately `10.47s` with approximately `2.75 GiB` peak RSS on the EC2 host; this is the fallback threshold Arrow IPC must beat.
- User-provided hypothesis: DuckDB's community `nanoarrow`/`arrow` extension supports Arrow IPC file scanning through `read_arrow(...)` even though the pinned core/bundled registry does not expose `read_arrow`/`read_ipc`.
- User-ratified: performance and correctness are joint first priority; no potentially regressing default is retained without same-host evidence.

## Journal

- 2026-07-18: Opened after G4's no-code tuning found that the previously rejected naive 64 MiB Parquet handoff was a bad policy point, not a bad direction. Single-sample EC2 variants measured `32MiB/32k` at `14.553145394s` / `5.25 GiB`, `128MiB/131k` at `10.484517844s` / `2.77 GiB`, `256MiB/262k` at `10.502437148s` / `2.74 GiB`, `512MiB/524k` at `10.761288241s` / `3.61 GiB`, and `1GiB/1,048k` at `11.218852136s` / `3.55 GiB`. A median-of-three confirmation for `256MiB/262k` measured `10.472399505s`, MAD `12.644792ms`, and peak RSS `2.757 GiB`.
- 2026-07-18: User supplied the Arrow IPC/nanoarrow packet. D11 was reshaped from immediate Parquet-handoff implementation into Arrow IPC handoff falsification first, because a successful `read_arrow(...)` path could avoid both DuckDB's appender floor and Parquet encode/decode overhead while reusing CDF's Arrow-native segment currency.
- 2026-07-18: Added lab-only DuckDB `read_arrow(...)` reference workloads to `cdf-p3-lab` for generated Arrow IPC handoff files and existing CDF package segment files. Local validation passed with `cargo fmt --all && cargo fmt --check`, `CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks arrow_ipc_handoff_writer_emits_readable_files --locked -j 12`, and `CARGO_BUILD_JOBS=12 cargo check -p cdf-benchmarks --locked -j 12`.
- 2026-07-18: EC2 full-lab preflight passed at dirty revision `48dd7d6d3ef265064b382624e18543046346ac95+dirty`, host class `host-class-649c6f28be3544c8`, tuned gp3 storage, and build marker `2026-07-18T20:40:51Z`.
- 2026-07-18: Nanoarrow extension availability is positive but compression-limited. Generated small uncompressed Arrow IPC handoff loaded through `INSTALL nanoarrow FROM community; LOAD nanoarrow; read_arrow(...)` and counted `2,048` rows in `45.549194ms`. Generated LZ4 Arrow IPC failed with DuckDB/nanoarrow error `Compression type with value 1 not supported by this build of nanoarrow`.
- 2026-07-18: Current CDF package segment direct-read attempt also failed with the same nanoarrow LZ4-compression error. This kills direct consumption of current package segments by DuckDB `read_arrow(...)` under the pinned extension build and explicitly forbids changing package artifact compression/identity in this ticket.
- 2026-07-18: Generated full-year TLC-shaped uncompressed Arrow IPC handoff with `_cdf_row_key`, `65,536` batch rows, and `1,048,576` rows per handoff file materialized through DuckDB `read_arrow(list_of_files)` in median `9.308415940s`, MAD `65.124961ms`, `4,422,849` rows/s, peak RSS `3.707338752 GiB`, and child-cgroup peak `9.272602624 GiB` over three uncontrolled EC2 samples. Generated full-year LZ4 failed with the same nanoarrow compression limitation.
- 2026-07-18: Keep/kill conclusion: keep only the uncompressed Arrow IPC handoff direction, and only as a destination-owned opt-in implementation candidate until full-CDF EC2 promotion evidence exists. It beats the tuned Parquet handoff median (`10.472399505s`) and is materially closer to the `8–12s` roofline, but current-package direct read and LZ4 handoff are killed for this pinned DuckDB/nanoarrow build.

## Blockers

None for the falsification slice. Production retention moved to `.10x/tickets/cancelled/2026-07-18-p3-d12-duckdb-arrow-ipc-handoff-ingress.md`, which cancelled the Arrow IPC handoff product path after full-CDF EC2 evidence failed the retention threshold.

## Evidence

- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-parquet-staged-rg32m-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-parquet-staged-rg128m-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-parquet-staged-rg256m-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-parquet-staged-rg256m-median3-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-parquet-staged-rg512m-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-duckdb-parquet-staged-rg1g-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-d11-arrow-ipc-small-none-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-d11-arrow-ipc-small-lz4-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-d11-arrow-ipc-current-segment-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-d11-arrow-ipc-full-lz4-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-d11-arrow-ipc-full-none-median3-observation.json`
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-arrow-ipc-probe-revision.env`
- `.10x/evidence/.storage/2026-07-18-p3-l7-ec2-after-arrow-ipc-probe-build.env`

## Review

Pass with retained scope narrowed. The evidence answers every falsification question: nanoarrow/read_arrow is installable and works for uncompressed IPC; current CDF segments and LZ4 IPC are incompatible with this extension build; full-year generated uncompressed IPC is faster than tuned Parquet handoff and much faster than the appender floor. The only significant residual risk is productionizing an unpinned community extension and temporary handoff spill without leaking DuckDB-specific behavior into orchestration; D12 owns that before any default change.

## Retrospective

The important correction was separating three Arrow IPC questions that sounded like one: extension availability, existing package compatibility, and destination-owned ephemeral handoff viability. Existing CDF segments stay LZ4-compressed package artifacts and are not a viable DuckDB input today. A separate uncompressed handoff file is viable and fast enough to justify implementation, but it must remain destination-owned temporary spill until a separate artifact-format decision says otherwise.
