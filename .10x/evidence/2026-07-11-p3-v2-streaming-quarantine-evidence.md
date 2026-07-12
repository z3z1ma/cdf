Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-v2-validation-graph-integration.md, .10x/tickets/2026-07-11-p3-d4-parquet-streaming-writer.md

# Streaming selected quarantine evidence

## What was observed

Vector validation now separates masks from candidate emission and calls an engine sink once per selected candidate. Production never constructs `Vec<QuarantineCandidate>`. The engine's single `QuarantinePartAccumulator` path handles pre-contract, residual, and ordinary rule evidence, flushes at 8,192 records, and retains at most one bounded record chunk plus the Parquet writer. Package evidence writes directly through `StreamingIdentityArtifact` into the atomic hashing file sink. The former quarantine-to-Parquet bytes and Parquet-bytes-to-tempfile functions were deleted rather than retained as compatibility APIs. Dedup provenance uses the same generic streaming identity writer.

## Procedure

- `cargo test -p cdf-contract vector::tests --lib` — five passed, two performance tests ignored.
- `cargo test -p cdf-package quarantine --lib` — two passed, including a 20,000-record writer split into 4,096-record calls and exact readback.
- `cargo test -p cdf-engine --lib` — 91 passed, four explicit performance/stress tests ignored.
- `cargo clippy -p cdf-contract -p cdf-package -p cdf-engine --all-targets -- -D warnings` — passed.
- Full package tests reached 50 passing before the two already-owned archive-force regression tests failed exactly as recorded in `.10x/tickets/2026-07-11-package-archive-force-replacement-regression.md`; focused quarantine/dedup tests and strict Clippy pass.
- `CDF_A5_FUSION_BENCH_ITERATIONS=200 cargo test --release -p cdf-engine fused_transform_hot_path_benchmark --lib -- --ignored --nocapture` — unfused 1.942 GiB/s, fused 15.496 GiB/s, 7.979x.

## What this supports or challenges

This removes both unbounded-by-rule-count candidate retention and complete encoded-artifact buffering. Artifact visibility, hash, fsync/rename, receipt journal, package verification, quarantine ordering/redaction, residual decisions, and fused/unfused identity remain governed by existing shared authorities. Adding a new validation rule does not change package writing code.

The follow-up ledger slice assigns the buffer to consumer `quarantine-evidence`. Reservation grows before retention, accounts record/source-position/observed strings plus simultaneous Arrow/Parquet construction at a conservative 3x, and shrinks only after the Parquet writer flushes its row group and the record Vec capacity is dropped. A deterministic 1 KiB budget rejects a 4 KiB preserved value with no artifact, zero part count, and zero residual ledger bytes.

## Limits

Macro TLC/package profiling and final golden/RSS closeout remain V2. Archive transcode paths still use a separate byte API and remain D4/package-archive scope; this change does not claim their removal. The 3x quarantine encoding estimate is specific to the flat quarantine schema and must be remeasured if that schema or Parquet writer policy changes.
