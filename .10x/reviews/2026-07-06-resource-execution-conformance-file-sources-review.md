Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md
Verdict: pass

# Resource Execution Conformance File Sources Review

## Target

Review of the resource execution conformance helper, `cdf-formats::FileResource`, file-source consumer tests, dependency changes, and evidence recorded for `.10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md`.

## Findings

No blocking findings.

The implementation stays within the child scope. `cdf-conformance` adds a focused `resource::execution` module and exports the helper from the existing resource namespace. `cdf-formats` adds a focused `resource` module and exports `FileResource`; the crate root remains thin.

The helper validates the acceptance-critical execution facts through the public `ResourceStream` trait: descriptor/schema coherence, request resource id, partition plan shape, expected partition ids, open/drain behavior, batch resource id, batch partition id, unique non-empty batch id, observed schema hash, `RecordBatch` payload presence, row count, byte count, file-manifest source position, total row count, and optional per-partition row counts.

The negative self-tests are meaningful. They intentionally fail wrong ids, duplicate batch ids, header count lies, bad schema hashes, missing/doubled partition data, missing file positions, non-`RecordBatch` payloads, bad expected partition sets, independent position triggers, and malformed file manifests. The final mutation run over both the helper and downstream `FileResource` consumer had 0 missed mutants.

The file-source wrapper reuses `read_file_source` and therefore preserves CSV, JSON, NDJSON, and DuckDB-backed Parquet reader semantics. It does not add native arrow-rs `parquet`, `paste`, or a supply-chain advisory exception. The forbidden dependency scan found no `parquet` or `paste` entries in the touched manifests, lockfile entries, or touched source.

`cargo semver-checks` found one real API risk during review: adding public fields to `FormatRead` would have broken external struct literals. The final implementation removed those fields and derives `FileResource` schema/position from existing emitted batch data. Final semver checks passed for both touched crates.

The duplicate-code report is acceptable for this slice. `jscpd` reported five small clones totaling 0.66% duplicated lines, mostly existing module import/test-helper shapes and one small file-position fixture shape between the new execution self-test and the existing planning harness self-test. Extracting a shared helper now would couple test fixtures more than it would clarify the conformance contract.

## Verdict

Pass. The child acceptance criteria are satisfied with focused tests, downstream file-source fixture coverage, mutation evidence, coverage, semver checks, supply-chain/security scans, CodeQL using the reusable database, and parent review.

## Residual Risk

`FileResource::new` currently requires at least one emitted `RecordBatch` to expose `ResourceStream::schema` without changing the public `FormatRead` struct shape. Empty-file resource semantics are not specified by this ticket and remain outside this closure.

This slice covers file-source execution for CSV, JSON, NDJSON, and Parquet only. HTTP/API source execution, SQL snapshot/incremental source execution, boundedness honesty, suffix replay APIs, reference payload execution, chaos killpoints, live-run golden gates, and MVP killer-demo orchestration remain parent conformance scope.

CodeQL retains the known local Rust extractor macro-warning profile, but the final SARIF result count is 0 and the broader focused gates passed.
