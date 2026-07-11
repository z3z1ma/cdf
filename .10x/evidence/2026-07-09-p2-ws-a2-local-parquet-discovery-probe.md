Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a2-local-parquet-discovery-probe.md, .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md

# P2 WS-A2 local Parquet discovery probe evidence

## What was observed

The A2 slice adds a local Parquet footer/schema discovery API in `cdf-formats` and a small schema snapshot handoff helper in `cdf-project`.

Touched implementation surfaces:

- `crates/cdf-formats/src/parquet_discovery.rs`
- `crates/cdf-formats/src/lib.rs`
- `crates/cdf-formats/src/tests.rs`
- `crates/cdf-project/src/schema_snapshot.rs`
- `crates/cdf-project/src/tests.rs`

The new `discover_local_parquet_schema` API loads Parquet footer/Arrow reader metadata through `ArrowReaderMetadata::load`, returns an Arrow `SchemaRef`, and returns local source identity evidence: file size, modified time when available, row count, row-group count, and a deterministic `footer_sha256` over schema/footer metadata. It does not construct a `ParquetRecordBatchReaderBuilder`, build a record batch reader, or materialize row batches.

The new `schema_snapshot_from_parquet_footer_schema` helper creates a `SchemaSnapshotArtifact`, `SchemaSnapshotReference`, and separate source-identity evidence map. The artifact metadata records `probe = parquet-footer` and `format = parquet`. The source-identity map is intentionally separate from `SchemaSnapshotArtifact::hash_input`; the focused test passes an absolute local path in source identity and verifies that neither that path nor the source footer hash enters the schema snapshot hash input.

## Procedure

Focused tests:

- `cargo test -p cdf-formats local_parquet_schema_discovery --locked`
  - Passed: 3 tests.
- `cargo test -p cdf-project local_parquet_discovery_handoff_builds_deterministic_snapshot --locked`
  - Passed: 1 test.

Required package tests:

- `cargo test -p cdf-formats -p cdf-project --locked`
  - Passed.
  - `cdf-formats`: 13 unit tests passed; 0 doc tests.
  - `cdf-project`: 90 unit tests passed; 0 doc tests.

Required lint and formatting:

- `cargo clippy -p cdf-formats -p cdf-project --all-targets --locked -- -D warnings`
  - Passed after replacing one redundant closure in the new probe.
- `cargo fmt --all -- --check`
  - Passed after applying `cargo fmt --all`.
- `git diff --check`
  - Passed.

Duplication and complexity:

- `jscpd --min-lines 5 --min-tokens 50 --reporters console --no-colors --no-tips crates/cdf-formats/src/parquet_discovery.rs crates/cdf-formats/src/lib.rs crates/cdf-formats/src/tests.rs crates/cdf-project/src/schema_snapshot.rs crates/cdf-project/src/tests.rs`
  - Exited 0.
  - Analyzed 5 Rust files, 2,116 lines, 14,185 tokens.
  - Reported 2 clones, 15 duplicated lines, 0.71% duplicated lines, 181 duplicated tokens, 1.28% duplicated tokens.
  - The reported clones are tiny repeated test/helper structures; no refactor was made for this scoped slice.
- Parent rerun with `--reporters console,json --output target/quality/reports/p2-a2-parent-jscpd` reported the same 2 clones and wrote `target/quality/reports/p2-a2-parent-jscpd/jscpd-report.json`.
- `rust-code-analysis-cli -m -O json -o target/quality/reports/p2-a2/rust-code-analysis -p crates/cdf-formats/src/parquet_discovery.rs -p crates/cdf-project/src/schema_snapshot.rs`
  - Exited 0 and wrote per-file JSON reports.
- `rust-code-analysis-cli -m -O json -p crates/cdf-formats/src/parquet_discovery.rs > target/quality/reports/p2-a2/rust-code-analysis-parquet-discovery.json`
  - Exited 0.
  - New probe file max cyclomatic complexity: 6.
- `rust-code-analysis-cli -m -O json -p crates/cdf-project/src/schema_snapshot.rs > target/quality/reports/p2-a2/rust-code-analysis-schema-snapshot.json`
  - Exited 0.
  - New handoff helper max cyclomatic complexity: 2.
  - The file-level maximum remains 15 in existing A1 schema snapshot conversion helpers.

Security and supply-chain checks:

- `semgrep scan --config p/rust --error --json --output target/quality/reports/p2-a2/semgrep-rust.json <5 touched Rust files>`
  - Exited 0 with 0 findings.
- `gitleaks detect --no-git --redact --source target/quality/gitleaks-src-p2-a2 --report-format json --report-path target/quality/reports/p2-a2/gitleaks-source.json --verbose`
  - Exited 0 with no leaks found.
  - The source was a generated mirror of the 5 touched Rust files only.
- `cargo deny --locked check advisories licenses sources`
  - Passed: advisories ok, licenses ok, sources ok.
- `cargo audit --deny warnings --ignore RUSTSEC-2024-0436`
  - Passed.
- `cargo vet --locked --no-minimize-exemptions`
  - Passed with 455 exempted.
- `osv-scanner scan source --lockfile Cargo.lock --format json > target/quality/reports/osv-p2-a2.json`
  - Exited non-zero only for the already-ratified `paste` advisory `RUSTSEC-2024-0436`; no A2 dependency change introduced a new advisory.
- `tools/codeql-rust-quality.sh 2>&1 | tee target/quality/reports/codeql-rust-p2-a2.log`
  - Used the repository-standard reusable database path `target/quality/codeql-db-rust`; the database refreshed because Rust inputs changed, then analysis completed.
  - CodeQL scanned 252 Rust files in this invocation.
  - SARIF result count: 3, all pre-existing current-tree hardcoded-value findings in `crates/cdf-cli/src/tests.rs` lines 1313, 1403, and 1459. They are owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md` and are outside WS-A2.

Acceptance-specific inspection:

- `rg -n "RecordBatch|ParquetRecordBatchReader|build\\(" crates/cdf-formats/src/parquet_discovery.rs`
  - Exited 1 with no matches, supporting that the probe module does not call the row-batch reader path.
- A scan for forbidden demo wording, stale project naming, placeholder markers, and credential terms across touched Rust files and A2 records found only pre-existing secret-redaction fixture strings in `crates/cdf-project/src/tests.rs`; the A2 implementation did not add credential material.

## What this supports

- `cdf-formats` now has a public local Parquet footer/schema discovery API.
- The probe returns Arrow schema plus source identity evidence sufficient for a later local probe cache without materializing row batches.
- Invalid non-Parquet input fails with a data error containing `Parquet metadata discovery`.
- `cdf-project` can turn a discovered Parquet footer schema into a deterministic `SchemaSnapshotArtifact` and `SchemaSnapshotReference` using the existing `.cdf/schemas/<resource>@<hash>.json` model.
- Snapshot metadata records `probe = parquet-footer` and `format = parquet`.
- Repeating the helper for the same resource id, schema, and metadata produces identical artifact hash and path.
- Source identity evidence is returned separately from the schema hash input, so absolute local paths are not included in the snapshot hash.

## Limits

- This evidence does not cover HTTP ranged discovery, object-store discovery, CLI schema commands, first-use auto-pin, lockfile writes, run/plan integration, conformance S1/S2 closure, or remote file transport wiring; those are explicit exclusions in A2.
- CodeQL's current-tree residuals are outside WS-A2 and remain owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.
- OSV's advisory residual is the already-ratified `paste` advisory `RUSTSEC-2024-0436`.
