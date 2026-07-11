Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-d2-file-manifest-run-aggregation.md, .10x/tickets/done/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md

# P2 WS-D2 file manifest run aggregation evidence

## What was observed

CDF now aggregates package-producing local multi-file file-resource segment positions into a deterministic resource-level `SourcePosition::FileManifest` checkpoint position.

Implementation changes observed in the parent workspace:

- `crates/cdf-engine/src/execution.rs` normalizes a batch `FileManifest` source position at the partition boundary when the partition scope is `ScopeKey::File`, so downstream contract evaluation and segment evidence use source-root-relative partition paths instead of temp-dir-dependent absolute paths.
- `crates/cdf-project/src/runtime/artifacts.rs` aggregates all-file-manifest non-cursor segment positions into one sorted manifest, preserves `size_bytes`, `etag`, and `sha256`, deduplicates identical entries, and rejects duplicate paths with conflicting evidence.
- `crates/cdf-project/src/runtime_tests.rs` covers deterministic aggregation, duplicate conflicts, mixed file/non-file rejection, file-scope normalization, and a live local two-file project run whose committed checkpoint manifest lists `events-a.ndjson` and `events-b.ndjson` while each state segment keeps one-file evidence.

## Procedure

Focused verification:

```text
cargo test -p cdf-engine --locked execution_returns_segment_source_position_evidence
cargo test -p cdf-project --locked general_project_run_commits_multi_file_resource_manifest_checkpoint
cargo test -p cdf-project --locked file_manifest
cargo test -p cdf-project --locked state_delta_rejects_mixed_file_and_non_file_source_positions
cargo test -p cdf-engine -p cdf-project --locked --no-fail-fast
```

All focused commands passed. The combined affected-crate run passed with 29 `cdf-engine` tests and 98 `cdf-project` tests.

Broad verification:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked --no-fail-fast
git diff --check -- crates/cdf-engine/src/execution.rs crates/cdf-project/src/runtime/artifacts.rs crates/cdf-project/src/runtime_tests.rs .10x/tickets/done/2026-07-09-p2-ws-d2-file-manifest-run-aggregation.md .10x/tickets/done/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md
rg -n "\\bunsafe\\b|extern \\\"|impl (Send|Sync)|unsafe impl" crates/cdf-engine/src/execution.rs crates/cdf-project/src/runtime/artifacts.rs crates/cdf-project/src/runtime_tests.rs
```

All broad commands passed. The direct unsafe/FFI marker scan produced no matches in touched Rust files.

Quality and security scanners:

```text
npx --yes jscpd@5 crates/cdf-engine/src/execution.rs crates/cdf-project/src/runtime/artifacts.rs --reporters json,console --output target/quality/reports/jscpd-d2-impl --min-lines 8 --min-tokens 80
npx --yes jscpd@5 crates/cdf-engine/src/execution.rs crates/cdf-project/src/runtime/artifacts.rs crates/cdf-project/src/runtime_tests.rs --reporters json,console --output target/quality/reports/jscpd-d2 --min-lines 8 --min-tokens 80
rust-code-analysis-cli -m -p crates/cdf-engine/src -O json -o target/quality/reports/rust-code-analysis-d2-engine
rust-code-analysis-cli -m -p crates/cdf-project/src -O json -o target/quality/reports/rust-code-analysis-d2-project
semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-rust-d2.json crates/cdf-engine/src crates/cdf-project/src
gitleaks dir --redact --no-banner --log-level error --report-format json --report-path target/quality/reports/gitleaks-tracked-d2.json <temporary tracked-source copy>
cargo deny --locked check
cargo audit --deny warnings --ignore RUSTSEC-2024-0436
cargo vet --locked --no-minimize-exemptions
cargo machete
osv-scanner scan source --lockfile Cargo.lock --format json > target/quality/reports/osv-d2.json
tools/codeql-rust-quality.sh 2>&1 | tee target/quality/reports/codeql-rust-d2.log
```

Observed scanner results:

- Implementation-only jscpd: 2 Rust files, 1,273 lines, 0 clones, 0 duplicated lines.
- Broader touched-file jscpd: 3 Rust files, 6,792 lines, 17 clones, 245 duplicated lines, 3.61% duplicated lines. Clone ranges are in the existing large `cdf-project` runtime test corpus; the implementation-only scan is clean.
- rust-code-analysis: emitted JSON reports for `cdf-engine` and `cdf-project`. Function-level hotspot among touched logic is `aggregate_file_manifest_output_position` with cyclomatic 12 and cognitive 10; highest pre-existing function remains `execute_to_package_inner` with cyclomatic 50 and cognitive 32.
- Semgrep: 36 tracked Rust files scanned with 11 rules, 0 findings.
- Gitleaks: tracked-source current tree scan passed. An initial raw repo-root scan was interrupted because it traversed generated build output and is not counted as evidence.
- cargo deny: passed. Output includes the already-recorded duplicate Arrow-major warnings from the DuckDB private-driver residual.
- cargo audit: passed with the ratified `RUSTSEC-2024-0436` ignore.
- cargo vet: passed, 455 exempted.
- cargo machete: passed with no unused dependencies.
- OSV: exited nonzero only for the already-ratified `RUSTSEC-2024-0436` advisory on `paste 1.0.15`.
- CodeQL: reused/refreshed the persistent database at `target/quality/codeql-db-rust` because Rust inputs changed. Extraction errors were 0; warnings matched the documented Rust macro-warning profile. SARIF findings are the three pre-existing `rust/hard-coded-cryptographic-value` findings in `crates/cdf-cli/src/tests.rs` lines 1319, 1409, and 1465, owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.

## What this supports

This supports closing D2:

- Valid file-manifest segment positions no longer fail with the old divergent-position error.
- Resource-level checkpoint state now records all files loaded by a local multi-file run.
- File manifest paths in the live project runtime are stable source-root-relative partition paths.
- Conflicting duplicate file evidence and mixed source-position variants fail closed.
- State segments retain per-segment one-file evidence for later traceability.

## Limits

Manifest comparison against prior checkpoint state, changed-file filtering, unchanged no-op reruns, compression, remote transports, and per-file schema variance remain excluded by the D2 ticket and owned by later P2 WS-D/E/I children.
