Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-b3-parquet-declared-schema-reconciliation.md, .10x/tickets/done/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/types-contracts-normalization.md

# P2 WS-B3 Parquet declared-schema reconciliation evidence

## What Was Observed

B3 integrates the shared B2 schema reconciler into local Parquet declared-schema reads. `read_file_source_with_declared_schema` now treats Parquet separately from undeclared reads, reconciles the physical Parquet Arrow schema against the declared constraint schema, and emits reconciled descriptors, batches, observed schema, and schema hashes when reconciliation succeeds.

The materialized batch path projects declared fields from source physical fields by `cdf:source_name` or field name, drops extra physical fields outside the declared projection, preserves declared field names/metadata, carries `cdf:source_name` and `cdf:physical_type` provenance, and casts supported width widenings using Arrow 59.1 cast kernels. `cdf-declarative` now routes non-empty declared Parquet schemas through the same declared-schema reader gate used by JSON/NDJSON.

Focused tests cover `int32 -> int64`, `float32 -> float64`, source-name projection/rename with extra-field drop, lossy narrowing failure with B2 remediation text, unchanged undeclared Parquet reads, and the declarative routing gate.

## Procedure

- `cargo test -p cdf-formats parquet --locked`
  - Result: passed.
  - Coverage: 10 filtered Parquet tests passed.
- `cargo test -p cdf-declarative parquet --locked`
  - Result: passed.
  - Coverage: 1 filtered Parquet routing test passed.
- `cargo test -p cdf-formats --locked`
  - Result: passed.
  - Coverage: 18 unit tests passed; doc tests passed.
- `cargo test -p cdf-declarative --locked`
  - Result: passed.
  - Coverage: 73 unit tests passed; doc tests passed.
- `cargo clippy -p cdf-formats -p cdf-declarative --all-targets --locked -- -D warnings`
  - Result: passed.
- `cargo fmt --all -- --check`
  - Result: passed.
- `git diff --check -- . ':(exclude)crates/cdf-cli/src/tests.rs' ':(exclude).10x/tickets/2026-07-05-wasm-components-registry-signing.md' ':(exclude).10x/tickets/2026-07-08-wasm-wit-interface-foundation.md'`
  - Result: passed.
- `npx --yes jscpd --reporters console --exit-code 0 --min-lines 6 --min-tokens 60 crates/cdf-formats/src/readers.rs crates/cdf-formats/src/tests.rs crates/cdf-declarative/src/file_runtime.rs`
  - Result: exited 0.
  - Output summary: 2 small clone blocks; 17 duplicated lines; 204 duplicated tokens; 0.80% lines and 1.37% tokens.
  - Inspection: one clone is the local Parquet reader metadata/open pattern introduced beside the existing reader path; the other is existing Arrow IPC test assertion setup. Both are small and clearer left local in this slice.
- `rust-code-analysis-cli -m -O json -p crates/cdf-formats/src/readers.rs`
  - Result: completed.
- `rust-code-analysis-cli -m -O json -p crates/cdf-formats/src/tests.rs`
  - Result: completed.
- `rust-code-analysis-cli -m -O json -p crates/cdf-declarative/src/file_runtime.rs`
  - Result: completed.
  - Observed complexity: new routing helper remains low complexity; high file-level cyclomatic values are pre-existing parser/test concentrations.
- `cargo tree -p cdf-formats -i arrow-cast@59.1.0 --locked`
  - Result: passed and confirmed the new direct `cdf-formats` dependency uses Arrow 59.1.0 cast kernels.
- `cargo deny check`
  - Result: passed. Known duplicate dependency warnings for the ratified Arrow 58/59 residual remained non-fatal.
- `cargo audit`
  - Result: passed with only the already-ratified `paste` advisory allowance.
- `cargo vet --locked`
  - Result: passed.
- `osv-scanner scan --lockfile Cargo.lock`
  - Result: exited 1 only for `paste` RUSTSEC-2024-0436, the already-ratified residual.
- `semgrep scan --config p/rust --error --metrics=off crates/cdf-formats/src/readers.rs crates/cdf-formats/src/tests.rs crates/cdf-declarative/src/file_runtime.rs`
  - Result: passed with 0 findings.
  - Note: an earlier `semgrep --config auto` attempt was invalid because Semgrep's auto config requires metrics; the Rust ruleset rerun is the usable result.
- Scoped Gitleaks:
  - `gitleaks detect --no-git --source crates/cdf-formats --report-format json --report-path target/quality/reports/gitleaks-p2-b3-cdf-formats.json`
  - `gitleaks detect --no-git --source crates/cdf-declarative/src/file_runtime.rs --report-format json --report-path target/quality/reports/gitleaks-p2-b3-file-runtime.json`
  - Result: both passed with no leaks found.
- `cargo machete`
  - Result: passed with no unused dependencies.
- `tools/codeql-rust-quality.sh`
  - Result: exited 0.
  - Database behavior: refreshed `target/quality/codeql-db-rust` because Rust sources/manifests/lockfile changed, preserving the reusable database location.
  - Current SARIF results: 3 `rust/hard-coded-cryptographic-value` findings, all in unrelated dirty `crates/cdf-cli/src/tests.rs` lines 1313, 1403, and 1459. These are owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md` and are not introduced by B3.

## What This Supports Or Challenges

This supports that B3 satisfies its scope: declared Parquet schemas no longer bypass the shared reconciliation model; supported lossless width widenings are materialized into Arrow batches; reconciled fields preserve source provenance; policy-rejected narrowing fails before batches are emitted; undeclared Parquet behavior remains unchanged; and declarative preview/run entry points now send declared Parquet resources through the reconciled read path.

This also adds focused regression coverage for P2 friction 4, "No lossless widening", and friction 5, "Declared schema doesn't drive Parquet reads", at the local Parquet reader boundary. Full WS-I conformance and S1/S2/S8 golden-path coverage remain open.

## Limits

This evidence does not claim discovery auto-pin, remote Parquet ranged reads, REST/SQL/NDJSON reconciliation unification, validation-program serialization of the coercion plan, row-level drift quarantine for incompatible Parquet files, package/golden fixture regeneration, or full source archetype conformance.

The Parquet reader currently applies the default `TypePolicy` at this format boundary because policy plumbing from project resources into `cdf-formats` is outside B3. String parse coercions and lossy mappings remain unavailable here until a later WS-B policy-integration child wires explicit policy through the read path.
