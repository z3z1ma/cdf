Status: active
Created: 2026-07-18
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md

# P3 D15: canonical package row ordinal

## Scope

Implement `.10x/specs/canonical-package-row-ordinal.md` as the one destination-neutral row-sequence authority in canonical segments, manifests, verification/replay, and first-party destination bulk paths. Remove superseded DuckDB/Postgres destination-local enumeration code once equivalent conformance and performance evidence is green.

## Non-goals

- No public provenance-address change.
- No destination-visible ordinal column by default.
- No old package reader, migration, or compatibility shim.
- No unmeasured default-path retention.

## Acceptance Criteria

- Canonical segment storage and manifest evidence satisfy every assignment/continuity/tamper scenario in the governing spec.
- The shared memory ledger accounts the generated ordinal buffer and constant-memory tests remain green.
- DuckDB nanoarrow derives `_cdf_row_key` from the persisted ordinal with no `rowid`, window, sequence, or file-order premise.
- Postgres binary COPY derives row keys from the persisted ordinal and removes its generated row-index path.
- Parquet destination strips the internal field from visible data while preserving manifest provenance.
- Jobs-invariance and cross-destination logical-address conformance remain green.
- Controlled EC2 evidence records package overhead and DuckDB/Postgres/Parquet end-to-end impact; no slower default is retained.
- Superseded enumeration code and tests are deleted, and D14 resumes against the current ordinal-bearing package format.

## References

- `.10x/decisions/canonical-package-row-ord.md`
- `.10x/specs/canonical-package-row-ordinal.md`
- `.10x/decisions/compact-lossless-destination-row-provenance.md`
- `.10x/specs/canonical-segmentation-adaptive-batching.md`
- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/tickets/2026-07-18-p3-d14-duckdb-nanoarrow-080-lz4-revalidation.md`

## Assumptions

- User-ratified: shift deterministic row enumeration to the canonical post-verdict Arrow stream and keep destination-global keys transaction-owned.
- Record-backed: one package-global ordinal permits every relational destination to use `allocated_start + persisted_ordinal` while retaining the same public segment-local row address.
- User-ratified: performance and correctness are joint first priority; the extra column must be benchmarked rather than assumed free.

## Journal

- 2026-07-18: Opened after D14 proved direct nanoarrow 0.8.0 LZ4 ingestion at 4.56 seconds but destination-side enumeration alternatives cost 4.50–36.76 seconds and introduced adapter-specific ordering premises. The package-global form is selected because a segment-local ordinal would still require per-file constants, while a dense package ordinal makes the destination key one vectorized addition and keeps segment-local logical provenance derivable from manifest starts.
- 2026-07-18: Renamed the physical field to `_cdf_package_row_ord`: `row_id` would falsely imply identity and `row_num` would suggest one-based SQL numbering. The governing decision/spec now state the unbounded-input contract explicitly: finite package epochs stream durable segments, final binding closes an epoch, and the next package resets the ordinal without buffering an unbounded package in memory.
- 2026-07-18: Removed DuckDB's bundled-C++ build from all four workspace consumers before starting the ordinal implementation. The workspace now selects the exact `duckdb = 1.10504.0` tuple once and lets `libduckdb-sys` download/cache its matching v1.5.4 dynamic library. A DuckDB crate test build completed in 17.95 seconds with 29 passing tests and one ignored; a warm CLI relink completed in 1.25 seconds. The downloaded cache is 143 MiB versus 455 MiB for each stale bundled build directory. Added loader-relative rpaths plus explicit release/archive/installer carriage of the target library; an actual packaged and reinstalled macOS CLI resolved `@rpath/libduckdb.dylib` and printed `cdf 0.1.0`. This is a prerequisite build-graph correction, not a package-provenance behavior change.
- 2026-07-18: Implemented the exact internal `_cdf_package_row_ord` contract in `cdf-package-contract` and made manifest/storage version 2 current-only. Canonical assembly assigns dense package-global ordinals after every row-selecting operator and before asynchronous segment encoding; manifest finalization, package open, verified segment reads, replay, and staged ingress all verify exact schema metadata, density, segment starts, row counts, and continuity. The shared package-memory reservation now includes the generated `UInt64` value buffer.
- 2026-07-18: Removed destination-local enumeration from the first-party paths. DuckDB derives both `_cdf_row_key` and merge order from the persisted ordinal and uses manifest starts for segment mirrors. Postgres reserves one package-sized transactional range, derives binary-COPY row keys from the ordinal, and records manifest-derived segment ranges. Parquet verifies the ordinal, strips it from visible object columns, and records package starts alongside object row offsets. Archive Parquet likewise strips the internal field while canonical Arrow IPC packages retain it.
- 2026-07-18: Fresh review rejected embedding package hash or segment id into every row: package hash would create an identity cycle, while segment id is already a segment-constant manifest fact and prior evidence measured the repeated-string form at 5–6x the compact representation. The canonical ordinal is the only missing per-row fact; `(package hash, segment id, segment row ordinal)` remains exact through manifest starts without payload duplication.
- 2026-07-18: The migration exposed a real shared-ledger lock-order defect in finalized Parquet replay. The destination reserved writer memory before the verified segment could reserve its bounded input window, so a valid constrained-memory test could wait indefinitely. Reordered acquisition to obtain the verified input window first and then take the writer reservation nonblockingly; the focused failure-mode test now completes cleanly instead of hanging.
- 2026-07-18: Regenerated only current-format identity goldens. Manifest v2 changes canonical IPC segment bytes, commit-plan identity, proposed-delta identity, and package hashes by construction; 100-rebuild prepared-package and 100-run DuckDB/Parquet live goldens plus bounded live Postgres repeats all pass with the new bytes.
- 2026-07-18: The first controlled EC2 full-year TLC gate failed closed at the 119-second worker timeout versus the 33.96-second pre-D15 floor. A second instrumented run reproducibly stopped at 88 of 215 durable segments with zero CPU and I/O activity, 37 workers waiting on futexes, and 1.6 GiB resident memory. The canonical builder had correctly reserved its transient input + concat + ordinal construction peak, but incorrectly transferred that peak reservation to durable destination ingress. That stranded source-frontier leases behind a destination channel waiting for more segments and formed a ledger/backpressure cycle. Reconciled the construction lease to the exact canonical output allocation before encode/ingress; transform leases continue to own shared input allocations. Added a direct handoff test asserting the scratch lease equals the output allocation, so a generous test budget cannot conceal recurrence. Controlled EC2 rerun is required before the fix is accepted.
- 2026-07-18: The exact clean `79b4d441` EC2 release rerun completed all 41,169,720 rows in 37.565 seconds with no spill, proving the backpressure-cycle repair. It also falsified retention on the current appender path: the pre-D15 floor was 33.956 seconds, while canonical IPC bytes increased from 1,513,310,278 to 1,678,253,294 (+10.9%), segment encoding increased from 9.868 to 10.556 seconds, destination ingress increased from 32.916 to 36.813 seconds, and peak child RSS increased from 2.154 to 2.345 GiB. No three-sample appender rerun is warranted because the direction already fails the no-slower-default criterion. D15 remains active and D14 must prove that direct nanoarrow consumption converts the ordinal cost into a net end-to-end win before either ticket closes.
- 2026-07-18: The current ordinal-bearing full-year Parquet destination completed all 41,169,720 rows in `8.310422833s`, `4,953,986` rows/s, at `1,703,108,608` bytes peak process RSS on the controlled EC2 host. This proves the internal ordinal is stripped without imposing a retained Parquet regression at the end-to-end scale; DuckDB and Postgres remain separately owned acceptance cells.
- 2026-07-19: D14's final stock-scanner gate exposed a second nondeterministic nested-frontier deadlock before DuckDB final binding: the 4 GiB default stopped at 84/215 durable segments with partitions 0–4 present, every source/CPU/staging worker asleep, and no further I/O. The same revision completed in `9.199782961s` when the diagnostic budget was raised to 8 GiB, proving a bounded-admission cycle rather than scanner throughput. Static tracing found the exact cycle: a decode-unit task retained a shared run-work permit for the lifetime of its output stream, including waits on its bounded publication channel. A later canonical partition could therefore occupy every run slot with ready output that the outer frontier intentionally would not consume, preventing the head partition from decoding the batch needed to advance.
- 2026-07-19: Corrected run-work lifetime at the generic format boundary. Preparation and each physical-batch decode acquire shared run work only while active; the permit is released before bounded publication. No format, source, or destination identity enters the scheduler. A deterministic two-stream/one-slot regression test fills the first stream's bounded channels and proves the second stream still publishes within two seconds; the old lifetime blocks indefinitely. All 82 active file-source tests and strict all-target Clippy pass. The 4 GiB controlled full-year rerun remains the acceptance gate.

## Blockers

None.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo check --workspace --all-targets --locked -j 12` passed in 31.36 seconds on the warm local graph, including every source, destination, package, runtime, project, conformance, benchmark, and CLI target.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-package-contract -p cdf-package -p cdf-runtime -p cdf-dest-duckdb -p cdf-dest-parquet -p cdf-dest-postgres --lib --locked -j 12` passed: package contract 5/5; package 53/53 with three performance tests ignored; runtime 87/87 with one performance test ignored; DuckDB 30/30 with one benchmark ignored; Parquet 35/35 with one benchmark ignored; Postgres 26/26 with two benchmarks ignored. Live local Postgres transaction, merge, replace, correction, quarantine, rollback, and duplicate tests were included.
- Focused project/runtime artifact tests passed for staged and finalized replay into DuckDB, Parquet, and Postgres; recorded-HTTP multi-file jobs invariance persisted starts `[0,2]` and ordinal values `[0,1,2]` identically under inline and parallel encoding.
- `cargo test -p cdf-conformance package_replay::tests:: -- --nocapture` passed all 10 crash, replay, duplicate, recovery, and negative-harness cases.
- `prepared_orders_v1_matches_committed_golden_across_100_rebuilds`, `live_local_file_duckdb_v1_matches_committed_golden_across_100_runs`, `live_local_file_parquet_v1_matches_committed_golden_across_100_runs`, and `live_local_file_postgres_v1_matches_committed_golden_across_bounded_repeats` all passed against manifest v2 goldens.
- `cargo fmt --all -- --check` and `git diff --check` passed.
- `.10x/evidence/.storage/2026-07-18-p3-d15-ec2-canonical-row-ord-local-tlc.json` records the failed controlled EC2 gate at clean revision `bc5a7f66`: no samples completed before the 119-second timeout. Live process inspection localized the stall to deterministic ledger/backpressure at 88/215 segments; this is regression evidence, not performance-retention evidence.
- `cargo test -p cdf-engine canonical_segment_releases_construction_peak_before_durable_ingress --locked` and `cargo test -p cdf-engine parallel_segment_encoding_is_identical_to_inline_canonical_registration --locked` pass after the lease reconciliation. The first test inspects the lease at the durable frontier and proves that only exact output bytes, not transient construction peak bytes, survive into ingress.
- `.10x/evidence/.storage/2026-07-18-p3-d15-ec2-canonical-row-ord-local-tlc-fixed-smoke.json` records the clean controlled-host repair run at `79b4d441`: 37.565 seconds, 41,169,720 rows, 1,095,965 rows/s, 2,517,692,416 bytes peak child RSS, and no spill. Comparison with `.10x/evidence/.storage/2026-07-18-p3-g4-ec2-local-default-measured.json` proves completion but rejects the current appender combination as a retained default.
- Performance retention remains the open acceptance gate; D14's direct-segment product path must pass the controlled full-CDF EC2 comparison before D15 closes.
- `.10x/evidence/.storage/2026-07-18-p3-d15-ec2-parquet-full-year-current.json` records the current full-year Parquet destination cell.
- Controlled-host diagnostic at `c4b0759a`: the default 4 GiB D14 run was terminated after 84/215 segments and several minutes at zero useful CPU/I/O; the same exact release/workspace with `CDF_MEMORY_BUDGET=8GiB` completed 41,169,720 rows in `9.199782961s` with zero memory events. This supports the nested-admission diagnosis but is not retained promotion evidence.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-files --lib --locked -j 12 --no-fail-fast` passed 82 active tests (one slow listing test ignored), including `blocked_decode_publication_releases_shared_run_work`; `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-files --all-targets --locked -j 12 -- -D warnings` passed.

## Review

Pending.

## Retrospective

Pending.
