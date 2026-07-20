Status: recorded
Created: 2026-07-19
Updated: 2026-07-19

# P3 A9 watermark and late-data conformance

## Observation

The finite drain runtime now preserves a monotone, receipt-gated watermark across barriers and process recovery; classifies every transformed row against the prior effective global watermark; and gives each late-data action an exact, identity-bearing outcome. Watermark aggregation is incrementally indexed, row and payload evidence are streamed without row- or batch-cardinality resident vectors, and fixed captured intervals remain package-identical across jobs settings.

## Procedure and results

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime watermark --lib -- -q`: 16 passed, one explicit release benchmark ignored. Coverage includes minimum-all, source-authored minimum-eligible idleness, true idle resumption, new/missing partition claims, monotone clocks, restored per-partition floors, operator/source capability joins, and every-claim validation.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine late_rows_are_quarantined_or_admitted_with_identity_evidence --lib -- --nocapture`: passed. Across all three actions, the test inspects output counts, named quarantine evidence, exact normalized Arrow payloads, payload content identities, verdict totals, verified recapture input, and next-epoch emission.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine drain_rejects_an_earlier_regressing_claim_even_when_the_batch_tail_recovers --lib -- --nocapture`: passed. A valid tail cannot hide an earlier regressing claim.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine fixed_drain_epoch_packages_are_jobs_invariant --lib -- --nocapture`: passed with enabled typed watermarks. Jobs 1 and 8 produced identical package hashes, segment entries, and epoch closure evidence; the jobs-8 execution demonstrated more than one active source partition.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-state-sqlite checkpoint_stores_preserve_committed_watermark_monotonicity --lib -- --nocapture`: passed for both in-memory and SQLite authorities. Propose rejects watermark disappearance; commit revalidates against a concurrently advanced head and rejects regression.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project committed_head_reopens_only_its_verified_late_data_carryover --lib -- -q`: passed. Recovery accepts the carryover object only through a verified package whose hash equals the committed head and rejects conflicting authority.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project late_data_carryover_survives_the_receipt_to_checkpoint_crash_window --lib -- -q`: passed. The real injected receipt-to-checkpoint crash is recovered from the durable receipt, and the next committed checkpoint carries the verified late-data reference.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-package-contract late_data --lib -- -q`: three passed. Payload catalog paths, ordinals, hashes, actions, admitted output ordinals, and admitted-output exclusivity fail closed.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-runtime -p cdf-engine -p cdf-state-sqlite --lib --tests -- -D warnings`: passed at commit `2fc9ccac`.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-engine -p cdf-project --lib --tests -- -D warnings`: passed at commit `dfdf9dc1` and again for the constant-memory evidence accumulator at `37f2f433`.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-package-contract -p cdf-package -p cdf-runtime -p cdf-engine -p cdf-state-sqlite -p cdf-project --lib --tests -- -D warnings`: passed after the complete repair tranche (`2363f411`, `09d25d7c`, and `f99d28a9`).
- `CDF_A9_WATERMARK_PARTITIONS=100000 CDF_A9_WATERMARK_UPDATES=1000000 cargo test --release -p cdf-runtime incremental_watermark_tracker_benchmark --lib -- --ignored --nocapture`: 0.355380 seconds, 355.38 ns/update, 2,813,885 updates/second, 200,000 indexed metadata entries. The index holds one bounded state/deadline entry per planned partition and does not rescan all partitions per batch.
- `CARGO_BUILD_JOBS=12 cargo test --release -p cdf-engine late_data_classification_benchmark --lib -- --ignored --nocapture`: 65,536 rows x 1,024 iterations in 0.074906 seconds, 6.675 GiB/s over the typed event-time column. This exceeds the P3 1 GB/s/core validation floor by 6.675x on this host.
- Repeated fat-LTO probes after grouping late-row evidence by source batch measured 5.955, 5.944, 5.864, 6.107, and 5.924 GiB/s (median 5.944 GiB/s) over 8,192 iterations. A proposed allocation-avoiding mask variant was rejected after it reduced the median to approximately 5.55 GiB/s. The retained representation removes per-row copies of common source authority on the dirty path while preserving a no-late rate 5.944x above the program floor.

## Broad-check observation and resolution

`CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel -p cdf-package-contract -p cdf-runtime -p cdf-engine -p cdf-state-sqlite -p cdf-project --lib` was also attempted after concurrent external-task/discovery changes landed. The engine crate reported 155 passed, 23 failed, and six ignored before Cargo stopped the multi-package command. The failures cluster around newly strengthened, unrelated fixture authorities: noncanonical FileManifest SHA placeholders, discovery-manifest/snapshot binding, source batch memory accounting, and the external partition-schedule migration. One runtime-ownership lint also identifies existing `cdf-format-arrow-ipc`/benchmark helper executor use. None of the focused A9 tests failed. This broad result is not claimed as an A9 pass and must remain owned by the concurrent migration/fast-check stabilization work rather than be hidden or weakened here.

After those concurrent migrations settled, the complete A9-focused command was rerun in one shell invocation: package-contract late-data tests, runtime watermark tests, the late-row identity test, the all-claims regression test, jobs invariance, both checkpoint authorities, both project recovery tests, and strict Clippy over all seven affected crates. Every command exited zero. This supersedes the transient broad-check observation for A9 closure without claiming that unrelated whole-workspace CI is green.

## What this supports

- A committed watermark cannot regress or disappear through controller or store transitions.
- New, idle, and resumed partitions preserve the committed completeness floor.
- Quarantine and recapture retain the complete normalized row in verified Arrow IPC; admit retains it in canonical output. No action silently drops a row.
- Evidence memory is constant with respect to late-row cardinality and bounded by retained source batches rather than rows: common source authority is stored once per batch, JSON catalogs are hash-while-streamed, and row payloads live in durable Arrow artifacts.
- The no-late hot path and incremental partition aggregation are comfortably below the program's control-overhead budget on the measured host.
- Fixed watermark-enabled captures are deterministic across scheduler concurrency.

## Limits

The benchmark is local and synthetic rather than an EC2 host-class envelope cell. It measures the hot classifier and aggregation separately, not a resident multi-day stream. The inline tracker retains O(active planned partitions) control metadata; high-cardinality external task/result storage and its spill law are owned by C5, while a resident multi-day supervisor remains separately owned. A9 proves the finite epoch calculus both must reuse. Whole-workspace CI is outside this evidence record; strict Clippy and the complete affected-path test tranche are green.
