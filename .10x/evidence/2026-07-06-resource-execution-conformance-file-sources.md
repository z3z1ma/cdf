Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md, .10x/tickets/2026-07-05-conformance-chaos-golden.md, .10x/specs/conformance-governance-roadmap.md, .10x/specs/resource-authoring-planning-batches.md

# Resource Execution Conformance File Sources Evidence

## What Was Observed

`cdf-conformance` now exposes an async execution-level `ResourceStream` conformance helper. It plans through the public `ResourceStream::plan_partitions` contract, opens each returned partition, drains batch streams, and verifies batch header identity, batch id uniqueness, schema hash, row count, byte count, `RecordBatch` payload presence, partition-union row completeness, optional per-partition row counts, and file-manifest source-position honesty.

`cdf-formats` now exposes `FileResource`, a file-backed `ResourceStream` wrapper over the existing `read_file_source` implementation. The wrapper reuses the existing CSV, JSON, NDJSON, and DuckDB-backed Parquet readers and does not add native `parquet`, `paste`, or advisory-policy changes. The wrapper derives its schema and starting position from existing emitted batch data instead of adding public fields to `FormatRead`, after `cargo semver-checks` caught that public-field additions would be breaking.

CSV, JSON, NDJSON, and Parquet file fixtures now consume both the planning-level resource conformance harness and the new execution harness.

## Procedure And Results

Focused implementation checks:

- `cargo test -p cdf-conformance --locked resource -- --nocapture` passed with 10 resource tests. The output includes expected panic messages from negative self-tests that deliberately prove the harness fails bad resources.
- `cargo test -p cdf-formats --locked --no-fail-fast` passed with 6 tests and 0 doctests.
- `cargo clippy -p cdf-conformance -p cdf-formats --all-targets --locked -- -D warnings` passed.
- `cargo nextest run -p cdf-conformance -p cdf-formats --locked` passed with 38 tests run, 38 passed, 0 skipped.
- `cargo fmt --all -- --check` passed after formatting the compatibility edit.
- `git diff --check -- . ':(exclude).gitignore'` passed.
- `cargo doc -p cdf-conformance -p cdf-formats --no-deps --locked` passed.
- `cargo metadata --format-version=1 --locked` passed and wrote `target/quality/reports/cargo-metadata-resource-execution-file.json`.

Mutation and coverage:

- `cargo mutants --file crates/cdf-conformance/src/resource/execution.rs --file crates/cdf-formats/src/resource.rs --test-package cdf-conformance --test-package cdf-formats --cargo-arg --locked --jobs 4 --timeout 180 --output target/quality/mutants-resource-execution-file-final` passed: 35 mutants tested in 10 minutes, 22 caught, 13 unviable, 0 missed.
- `cargo llvm-cov -p cdf-conformance -p cdf-formats --locked --summary-only` passed. The final summary reported total line coverage 78.75%; `cdf-conformance/src/resource/execution.rs` line coverage 98.14%; `cdf-formats/src/resource.rs` line coverage 82.09%.

Supply-chain, security, and dependency hygiene:

- `cargo deny check > target/quality/reports/deny-resource-execution-file.txt 2>&1` passed; the final line reported `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo audit --json > target/quality/reports/cargo-audit-resource-execution-file.json` passed; JSON summary showed 0 vulnerabilities and 0 warnings.
- `cargo vet --locked --output-format json --output-file target/quality/reports/cargo-vet-resource-execution-file.json` passed with `conclusion = success` and 0 failures.
- `osv-scanner scan source -r . --format json --output target/quality/reports/osv-resource-execution-file.json` passed; JSON result package count was 0.
- `semgrep scan --config p/rust --error --no-git-ignore --json --output target/quality/reports/semgrep-resource-execution-file.json crates/cdf-conformance crates/cdf-formats` passed with 0 findings.
- `tools/codeql-rust-quality.sh` passed. It refreshed `target/quality/codeql-db-rust` once after the final source fingerprint changed, then produced `target/quality/reports/codeql-rust-current.sarif` with 0 SARIF results. CodeQL reported the known local Rust extractor macro-warning profile: 0 extraction errors, 2209 extraction warnings, 146 Rust files extracted, 2397 unresolved macro calls.
- `gitleaks git --redact --report-format json --report-path target/quality/reports/gitleaks-resource-execution-file-git.json .` passed with no leaks.
- A source-only `gitleaks dir` scan over a temporary mirror of `git ls-files --cached --others --exclude-standard -- Cargo.toml Cargo.lock crates/cdf-conformance crates/cdf-formats` passed with no leaks and wrote `target/quality/reports/gitleaks-resource-execution-file-source.json`.
- `rg -n '\bunsafe\b|extern "|\\*const|\\*mut|unsafe impl|impl (Send|Sync)' crates/cdf-conformance crates/cdf-formats > target/quality/reports/unsafe-scan-resource-execution-file.txt` exited 1 with no matches, which is the expected no-owned-unsafe result.
- Isolated `cargo geiger` runs for `cdf-conformance` and `cdf-formats` passed with JSON reports under `target/quality/reports/`. Both owned root packages reported 0 used unsafe functions, expressions, unsafe impls, unsafe traits, and unsafe methods. Geiger stderr contained parse warnings for third-party crate test files (`signal-hook-registry`, `pin-project`), so the direct owned-source unsafe scan remains the primary first-party signal.
- `cargo machete --with-metadata` passed with no unused dependencies.
- `cargo +nightly udeps -p cdf-conformance -p cdf-formats --all-targets --locked` passed; all dependencies appeared used.
- `cargo semver-checks check-release -p cdf-conformance --baseline-rev HEAD` passed.
- `cargo semver-checks check-release -p cdf-formats --baseline-rev HEAD` initially found a breaking public-field addition to `FormatRead`; the implementation was changed to avoid adding public fields, and the final rerun passed.
- `rg -n '^name = "(parquet|paste)"|parquet =|paste =' Cargo.lock crates/cdf-conformance/Cargo.toml crates/cdf-formats/Cargo.toml crates/cdf-conformance/src crates/cdf-formats/src > target/quality/reports/parquet-paste-scan-resource-execution-file.txt` exited 1 with no matches, confirming this slice did not add the native `parquet`/`paste` advisory path.
- `rust-code-analysis-cli` metric runs completed for both touched crates under `target/quality/reports/rust-code-analysis-resource-execution-file/`.
- `jscpd crates/cdf-conformance crates/cdf-formats --reporters json,console --output target/quality/reports/jscpd-resource-execution-file --ignore "**/target/**,**/.git/**,**/reports/**"` completed and reported 5 small clones, 40 duplicated lines, 0.66% duplicated lines. Parent review judged these as existing/import/test-helper shape and not a blocker.

Tooling limits and repaired findings:

- The first `cargo semver-checks check-release -p cdf-formats --baseline-rev HEAD` failed because `FormatRead.schema` and `FormatRead.source_position` had been added as public fields. That would have broken external struct literals. The final implementation removed those fields and derives `FileResource` schema/position from existing batch data; semver-checks then passed.
- Initial `cargo semver-checks ... --locked` and `rust-code-analysis-cli -p <two paths>` invocations failed due local tool CLI syntax, not product failures. They were rerun with supported syntax.

## What This Supports Or Challenges

This supports closing `.10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md`.

The evidence maps to the child acceptance criteria:

- Reusable async execution conformance helper exists and is exported from `cdf-conformance::resource`.
- The helper plans, opens, drains, and validates batch identity, uniqueness, row/byte counts, schema hash, `RecordBatch` payloads, partition completeness, per-partition rows, and file-manifest source positions.
- Negative self-tests cover wrong resource id, wrong partition id, duplicate batch id, bad row count, bad byte count, bad schema hash, missing expected partition, duplicate partition data, missing file position, non-`RecordBatch` payload, invalid expected partition sets, independent file-position trigger conditions, and malformed file manifests.
- `cdf-formats::FileResource` reuses `read_file_source` and the existing DuckDB-backed Parquet path.
- CSV, JSON, NDJSON, and Parquet fixtures consume both planning and execution harnesses.
- No native arrow-rs `parquet`/`paste` path or advisory exception was added.

This also advances `.10x/tickets/2026-07-05-conformance-chaos-golden.md` by covering resource file-source execution/data-completeness for CSV, JSON, NDJSON, and Parquet. Full lifecycle chaos, broader source execution families, boundedness honesty, live-run golden gates, and the MVP acceptance demo harness remain parent scope.

## Limits

The file-resource wrapper currently requires the existing reader to produce at least one `RecordBatch` so it can expose a `ResourceStream::schema` without changing `FormatRead`'s public struct fields. Empty-file semantics are not part of this child ticket's acceptance criteria.

The helper validates emitted file-manifest shape and evidence presence; it does not independently recompute the batch manifest hash for every emitted file because the current `FileManifest` contract carries the hash evidence and the existing reader owns computing it.

The execution helper is an MVP `RecordBatch` oracle. Reference payload execution, DataFusion file scan providers, HTTP/API execution, SQL snapshot resources, suffix replay APIs, chaos killpoints, CLI orchestration, and native Parquet policy remain explicitly outside this ticket.
