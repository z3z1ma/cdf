Status: recorded
Created: 2026-07-19
Updated: 2026-07-19

# P3 A9 watermark and late-data conformance

## Observation

The finite drain runtime now preserves a monotone, receipt-gated watermark across barriers and process recovery; classifies every transformed row against the prior effective global watermark; and gives each late-data action an exact, identity-bearing outcome. Watermark aggregation is incrementally indexed, row and payload evidence are streamed without row- or batch-cardinality resident vectors, and fixed captured intervals remain package-identical across jobs settings.

## Procedure and results

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime watermark --lib -- --nocapture`: 15 passed, one explicit release benchmark ignored. Coverage includes minimum-all, minimum-eligible idleness, true idle resumption, new/missing partition claims, monotone clocks, restored floors, operator/source capability joins, and every-claim validation.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine late_rows_are_quarantined_or_admitted_with_identity_evidence --lib -- --nocapture`: passed. Across all three actions, the test inspects output counts, named quarantine evidence, exact normalized Arrow payloads, payload content identities, verdict totals, verified recapture input, and next-epoch emission.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine drain_rejects_an_earlier_regressing_claim_even_when_the_batch_tail_recovers --lib -- --nocapture`: passed. A valid tail cannot hide an earlier regressing claim.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine fixed_drain_epoch_packages_are_jobs_invariant --lib -- --nocapture`: passed with enabled typed watermarks. Jobs 1 and 8 produced identical package hashes, segment entries, and epoch closure evidence; the jobs-8 execution demonstrated more than one active source partition.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-state-sqlite checkpoint_stores_preserve_committed_watermark_monotonicity --lib -- --nocapture`: passed for both in-memory and SQLite authorities. Propose rejects watermark disappearance; commit revalidates against a concurrently advanced head and rejects regression.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project committed_head_reopens_only_its_verified_late_data_carryover --lib -- --nocapture`: passed. Recovery accepts the carryover object only through a verified package whose hash equals the committed head and rejects conflicting authority.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-package-contract late_data --lib -- --nocapture`: two passed. Payload catalog paths, ordinals, hashes, actions, and admitted-output exclusivity fail closed.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-runtime -p cdf-engine -p cdf-state-sqlite --lib --tests -- -D warnings`: passed at commit `2fc9ccac`.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-engine -p cdf-project --lib --tests -- -D warnings`: passed at commit `dfdf9dc1` and again for the constant-memory evidence accumulator at `37f2f433`.
- `CDF_A9_WATERMARK_PARTITIONS=100000 CDF_A9_WATERMARK_UPDATES=1000000 cargo test --release -p cdf-runtime incremental_watermark_tracker_benchmark --lib -- --ignored --nocapture`: 0.355380 seconds, 355.38 ns/update, 2,813,885 updates/second, 200,000 indexed metadata entries. The index holds one bounded state/deadline entry per planned partition and does not rescan all partitions per batch.
- `CARGO_BUILD_JOBS=12 cargo test --release -p cdf-engine late_data_classification_benchmark --lib -- --ignored --nocapture`: 65,536 rows x 1,024 iterations in 0.074906 seconds, 6.675 GiB/s over the typed event-time column. This exceeds the P3 1 GB/s/core validation floor by 6.675x on this host.

## Broad-check observation

`CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel -p cdf-package-contract -p cdf-runtime -p cdf-engine -p cdf-state-sqlite -p cdf-project --lib` was also attempted after concurrent external-task/discovery changes landed. The engine crate reported 155 passed, 23 failed, and six ignored before Cargo stopped the multi-package command. The failures cluster around newly strengthened, unrelated fixture authorities: noncanonical FileManifest SHA placeholders, discovery-manifest/snapshot binding, source batch memory accounting, and the external partition-schedule migration. One runtime-ownership lint also identifies existing `cdf-format-arrow-ipc`/benchmark helper executor use. None of the focused A9 tests failed. This broad result is not claimed as an A9 pass and must remain owned by the concurrent migration/fast-check stabilization work rather than be hidden or weakened here.

## What this supports

- A committed watermark cannot regress or disappear through controller or store transitions.
- New, idle, and resumed partitions preserve the committed completeness floor.
- Quarantine and recapture retain the complete normalized row in verified Arrow IPC; admit retains it in canonical output. No action silently drops a row.
- Evidence memory is constant with respect to late-row and late-batch cardinality: JSON catalogs are hash-while-streamed, while row payloads live in durable Arrow artifacts.
- The no-late hot path and incremental partition aggregation are comfortably below the program's control-overhead budget on the measured host.
- Fixed watermark-enabled captures are deterministic across scheduler concurrency.

## Limits

The benchmark is local and synthetic rather than an EC2 host-class envelope cell. It measures the hot classifier and aggregation separately, not a resident multi-day stream. A resident supervisor and external million-task watermark-state spill are explicitly later work; A9 proves the finite epoch calculus they must reuse. The broad multi-crate test run is not green for unrelated concurrently landed authority migrations, as recorded above.
