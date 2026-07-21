Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/done/2026-07-18-p0-post-iceberg-integration-stabilization.md
Depends-On: .10x/tickets/done/2026-07-18-p0-external-partition-authority.md, .10x/tickets/done/2026-07-18-p0-typed-compiled-source-identities.md, .10x/tickets/done/2026-07-18-p0-source-io-accounting-separation.md

# P0: mandatory product smoke matrix gate

## Scope

Turn the post-tranche product smoke matrix into one reproducible required gate with local/recorded fast coverage and explicit live FQ12/network evidence.

## Acceptance Criteria

- Local Parquet to DuckDB passes.
- HTTPS TLC to DuckDB passes.
- Local multi-file manifest followed by unchanged no-op passes.
- FQ12 Iceberg `gold.dim_date` to DuckDB passes.
- Package verification and replay pass.
- Preview/run parity passes.
- Parquet destination passes.
- The gate is documented in `QUALITY.md` at the correct tier and is required before core-tranche closure.

## Assumptions

- User-ratified: this product matrix is mandatory stabilization evidence, not end-of-program polish.

## Journal

- 2026-07-18: Activated after every structural stabilization child closed and the final workspace
  authority gate passed 1,777/1,777 tests with strict all-feature Clippy. This child now owns one
  reproducible deterministic matrix plus separately labeled live public-network and FQ12 evidence;
  public endpoint flakiness may be recorded but cannot substitute for the local gate.
- 2026-07-18: The first live multi-file GitHub Parquet preview exposed a real attestation defect:
  unopened strongly versioned partitions discarded their pinned physical-schema hash, while opened
  partitions also discarded the physical schema observed during decoding. The file adapter now
  verifies the partition-to-observation binding once, carries the pinned hash through strong
  metadata attestations, and records the first decoded physical-schema hash at terminal completion.
  Weakly versioned sources never claim an unobserved schema. A two-file CLI regression owns the
  bounded-preview case.
- 2026-07-18: Fresh-state release runs passed against the Hugging Face TLC mirror: preview returned
  500 rows in 1.69 seconds, and the full January file committed 2,964,624 rows in 16 segments in
  3.44 seconds. The package verified all 31 identity files. Artifact-only replay to a fresh DuckDB
  committed the same 16 segments in 1.23 seconds; replay to the Parquet destination committed them
  in 0.69 seconds. Replaying into the already-committed checkpoint correctly failed as a lifecycle
  conflict, so replay evidence uses a fresh state authority rather than weakening checkpoint rules.
- 2026-07-18: The first live FQ12 Glue/Iceberg preview exposed a second real adapter defect: the
  planner sorted Iceberg field ids, which changed declared Arrow field order for schemas whose ids
  are not monotonic. Projection authority now preserves schema/request order and rejects duplicate
  ids. The task-set authority moved from v3 to v4 so stale sorted artifacts fail closed without a
  compatibility shim. Fresh-state release execution of `gold.dim_date` then committed 1,097 rows in
  four segments in 2.44 seconds.
- 2026-07-18: Added `tools/product-smoke-matrix.sh` as an 11-cell deterministic product barrier and
  documented it in `QUALITY.md` as mandatory before core-tranche closure, with live network/catalog
  cells explicitly retained in the scheduled/manual tier. The gate passed, strict formatting and
  all-feature Clippy passed, and the final workspace run passed 1,777/1,777 tests with 40 explicit
  skips. An earlier full run found one stale test fixture and one non-reproducing REST example miss;
  after correcting the fixture, the clean full rerun passed including the REST example.

## Blockers

None.

## Evidence

- Local Parquet to DuckDB: `tools/product-smoke-matrix.sh` passed
  `run_local_parquet_discover_autopins_and_commits_pinned_schema`.
- HTTPS TLC to DuckDB: the release binary loaded the January 2024 Hugging Face mirror object into a
  fresh DuckDB destination: 2,964,624 rows, 16 segments, 3.44 seconds. This is temporal live-network
  evidence; the recorded HTTP S1 cell is the deterministic regression owner.
- Multi-file manifest/no-op: the matrix passed
  `file_manifest_append_run_skips_unchanged_files_and_loads_only_changes`; the two-file preview
  attestation regression also passed and a five-file GitHub live run committed 5,000 rows in five
  segments.
- FQ12 Iceberg: using redacted, invocation-local PowerUser credentials, the release binary loaded
  Glue/Iceberg `gold.dim_date`: 1,097 rows, four segments, 2.44 seconds, package
  `sha256:c2daee2cdc12c60a2f4119021c3dc23110df90baf6be702a5df30ac4ebbd963e`.
- Package verification/replay: `cdf package verify` accepted all 31 TLC package files for
  `sha256:573c6f7e11a9c28e6f9f463158f4e68be3c7947b3a5e2108b946c1ac23689992`;
  artifact-only replay into fresh state committed all 16 segments. The deterministic package verify
  and artifact-only replay cells also passed.
- Preview/run parity: the matrix passed
  `p2_preview_run_parity_law_covers_supported_archetypes`; live TLC and GitHub preview/run pairs both
  passed.
- Parquet destination: artifact-only TLC replay to a fresh local Parquet destination committed
  2,964,624 rows in 16 segments in 0.69 seconds; the deterministic ledger-order cell passed.
- Gate placement: `QUALITY.md` requires `tools/product-smoke-matrix.sh` for source planning,
  execution, settlement, destination-ingress, and replay tranches, while live provider cells remain
  outside pull-request fast checks.
- Final gates: `cargo fmt --all -- --check` passed;
  `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` passed;
  `cargo nextest run --workspace --locked --no-fail-fast` passed 1,777/1,777 tests with 40 explicit
  skips; `tools/product-smoke-matrix.sh` passed all 11 selected product cells.

## Review

Fresh-hat adversarial review attempted to falsify generation safety, adapter boundaries,
performance, and artifact compatibility. Strong-generation metadata attestation uses a pinned hash
only after verifying the canonical observation binding; weak generations require the schema
actually observed during the payload stream. The per-batch hot path adds only one predictable
`Option` branch and clones the hash once for the first batch; existing engine validation remains the
authority for later-batch consistency. Iceberg ordering stays wholly in the Iceberg adapter and the
v4 authority rejects stale artifacts rather than translating them. Neither repair introduces
source/destination identity branches into generic orchestration. Verdict: pass. Residual risk is
limited to temporal public-provider and AWS availability; deterministic recorded/local cells own
the behavior independent of those providers.

## Retrospective

Synthetic fixtures had encoded away two production facts: a bounded multi-file preview can attest
files it never opens, and Iceberg field ids need not increase in schema order. The live matrix found
both immediately. The durable correction is not more isolated unit volume; it is a small mandatory
product barrier that crosses discovery, planning, bounded execution, settlement, verification, and
replay, plus fixtures that deliberately use unopened partitions and nonmonotonic field ids.
