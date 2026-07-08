Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws1e-tracing-bridge.md, .10x/specs/runtime-event-spine.md, .10x/specs/run-orchestration-ledger.md

# P1 product WS1E tracing bridge evidence

## What was observed

WS1E adds an optional `cdf-project` runtime tracing bridge without adding an OTLP exporter:

- `crates/cdf-project/src/runtime/tracing_bridge.rs` defines `TracingRunEventSink`, a `RunEventSink` implementation.
- The sink emits one `tracing::info!` event per accepted runtime `RunEvent` on target `cdf_project.runtime.run_event`.
- Emitted structured fields include `run_id`, `resource_id`, serialized `scope`, `partition_id`, `package_id`, `package_hash`, `package_path`, `destination_id`, `plan_id`, `checkpoint_id`, `receipt_id`, `event_kind`, `sequence`, `timestamp_ms`, and `details`.
- `details` is emitted as a JSON field after `RunEventDetails::validate()` passes. Invalid or unredacted details are dropped by the sink and cannot fail the run.
- The bridge is optional through the existing `ProjectRunRequest::event_sink` / fanout path. Existing callers that omit a sink keep current behavior.
- No CLI progress renderer, CLI JSON output path, parser grammar, event vocabulary, package identity logic, run success logic, ledger append logic, checkpoint authority, receipt verification, or OTLP exporter was changed by WS1E.

`Cargo.lock` was updated offline only to add the direct `tracing` dependency edge to the `cdf-project` package entry; the `tracing` package was already present in the lockfile.

## Procedure

Inspected before editing:

- `.10x/tickets/done/2026-07-08-p1-product-ws1e-tracing-bridge.md`
- `.10x/specs/runtime-event-spine.md`
- `.10x/specs/run-orchestration-ledger.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth.md`
- `.10x/evidence/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth.md`
- `.10x/reviews/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth-review.md`
- `.10x/tickets/done/2026-07-06-engine-execution-tracing-spans.md`
- `QUALITY.md`
- `crates/cdf-kernel/src/run_event.rs`
- `crates/cdf-project/src/runtime/**`
- `crates/cdf-project/src/runtime_tests.rs`
- existing `cdf-engine` tracing capture tests

Commands run:

- `cargo update -p cdf-project --offline`
- `cargo test -p cdf-project --locked tracing_bridge`
- `cargo test -p cdf-project --locked live_sink`
- `cargo test -p cdf-project --locked project_run_recorder`
- `cargo test -p cdf-project --locked general_project_run_records_ledger_events_in_commit_gate_order`
- `cargo test -p cdf-project --locked`
- `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`
- `cargo check -p cdf-project --all-targets --locked`
- `cargo fmt --all -- --check`
- `rustfmt --edition 2024 --check crates/cdf-project/src/runtime/tracing_bridge.rs crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime_tests.rs`
- `git diff --check -- crates/cdf-project/src/runtime/tracing_bridge.rs crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime_tests.rs crates/cdf-project/Cargo.toml Cargo.lock .10x/tickets/done/2026-07-08-p1-product-ws1e-tracing-bridge.md`
- Direct unsafe/FFI scan: `rg -n "\bunsafe\b|extern\s+\"|\bffi\b|\bFFI\b" crates/cdf-project/src/runtime/tracing_bridge.rs crates/cdf-project/src/runtime.rs crates/cdf-project/src/runtime_tests.rs`
- Semgrep Rust scan: `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-p1-ws1e.json ...`
- Source-only Gitleaks scan over copied WS1E source/manifests: `gitleaks dir target/quality/scans/p1-ws1e-source --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p1-ws1e-source.json`
- `jscpd --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-p1-ws1e-rust --exit-code 0 ...`
- `rust-code-analysis-cli -m -p ... -O json` for touched Rust files.
- `scc --format json ... > target/quality/reports/scc-p1-ws1e.json`
- `tools/codeql-rust-quality.sh`
- SARIF result count: `jq '[.runs[].results[]?] | length' target/quality/reports/codeql-rust-current.sarif`
- Final source/record Gitleaks scan over copied WS1E source, manifests, ticket, parent/dependent reference updates, evidence, and review records.
- Final record `jscpd` over WS1E ticket, parent/dependent reference updates, evidence, and review records.
- Parent integration rerun after concurrent WS3D formatting completed:
  - `cargo test -p cdf-project --locked tracing_bridge`
  - `cargo test -p cdf-project --locked live_sink`
  - `cargo test -p cdf-project --locked project_run_recorder`
  - `cargo test -p cdf-project --locked general_project_run_records_ledger_events_in_commit_gate_order`
  - `cargo test -p cdf-project --locked`
  - `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`
  - `cargo check -p cdf-project --all-targets --locked`
  - `cargo fmt --all -- --check`
  - direct unsafe/FFI scan over WS1E-touched Rust files
  - Semgrep Rust scan over WS1E-touched Rust files
  - Gitleaks over copied WS1E source/manifests/records
  - `jscpd` over WS1E-touched Rust files and records
  - forbidden-phrase scan over `.10x`, docs, crates, root docs, changelog, and tools

## Results

- Focused tracing bridge tests passed: 2 tests.
- Focused live sink/fanout tests passed: 3 tests.
- Focused recorder tests passed: 2 tests.
- Focused ledger order test passed: 1 test.
- Full `cdf-project` tests passed: 85 unit tests, 0 doctests.
- `cargo clippy -p cdf-project --all-targets --locked -- -D warnings` passed.
- `cargo check -p cdf-project --all-targets --locked` passed.
- Touched Rust files passed direct `rustfmt --edition 2024 --check`.
- Scoped `git diff --check` passed.
- Direct unsafe/FFI scan found no matches.
- Semgrep passed with 0 findings; report path `target/quality/reports/semgrep-p1-ws1e.json`.
- Source-only Gitleaks passed with no leaks; report path `target/quality/reports/gitleaks-p1-ws1e-source.json`.
- `jscpd` completed over touched Rust files: 3 files, 17 clones, 245 duplicated lines, 4.45%; report path `target/quality/reports/jscpd-p1-ws1e-rust/jscpd-report.json`. The remaining clones are in existing large runtime test patterns.
- Rust complexity reports were written under `target/quality/reports/rust-code-analysis-p1-ws1e-*.json`.
- Source size metrics were written to `target/quality/reports/scc-p1-ws1e.json`.
- CodeQL refreshed `target/quality/codeql-db-rust`, completed analysis, and wrote `target/quality/reports/codeql-rust-current.sarif` with 0 SARIF results.
- Final CodeQL Rust extractor metrics: 243 files extracted total, 190 with extractor errors, 53 without extractor errors, 3732 extraction warnings, 5681 unresolved macro calls. This matches the known Rust CodeQL coverage limitation pattern recorded in WS1C evidence.
- Final source/record Gitleaks passed with no leaks; report path `target/quality/reports/gitleaks-p1-ws1e-final.json`.
- Final record `jscpd` completed with 0 clones; report path `target/quality/reports/jscpd-p1-ws1e-records/jscpd-report.json`.
- Parent focused tracing bridge tests passed: 2 tests.
- Parent focused live sink tests passed: 3 tests.
- Parent focused recorder tests passed: 2 tests.
- Parent focused ledger-order test passed: 1 test.
- Parent full `cdf-project` tests passed: 85 unit tests and 0 doctests.
- Parent `cargo clippy -p cdf-project --all-targets --locked -- -D warnings` passed.
- Parent `cargo check -p cdf-project --all-targets --locked` passed.
- Parent `cargo fmt --all -- --check` passed.
- Parent direct unsafe/FFI scan found no matches.
- Parent Semgrep Rust scan passed with 0 findings; report path `target/quality/reports/semgrep-p1-ws1e-parent.json`.
- Parent Gitleaks scan over copied WS1E source/manifests/records passed with no leaks; report path `target/quality/reports/gitleaks-p1-ws1e-parent.json`.
- Parent `jscpd` completed with 17 clones, 245 duplicated lines, 4.45%; report path `target/quality/reports/jscpd-p1-ws1e-parent/jscpd-report.json`. The clone ranges remain in existing large runtime test patterns.
- Parent forbidden-phrase scan found no matches.

## Acceptance mapping

- Structured tracing fields: `general_project_run_tracing_bridge_emits_structured_runtime_events` captures all emitted tracing events and asserts the exact field map against the persisted ledger events, including run, resource, scope, partition, package, destination, plan, checkpoint, receipt, kind, sequence, timestamp, and details fields.
- Redaction guardrails: `runtime_tracing_bridge_drops_unredacted_details_before_emit` proves raw secret detail values produce no tracing event, while typed `SecretRef` details remain accepted.
- Optional bridge/no behavior drift: the bridge is only used when passed as the optional event sink; full `cdf-project` tests pass, and the tracing test reopens the run ledger to assert persisted events are unchanged.
- Package/checkpoint/receipt drift: the tracing test calls `assert_run_artifact_identity_unchanged`, checks checkpointed package status, checks committed checkpoint status, and checks receipt/checkpoint/package fields emitted from the persisted event envelopes.
- No OTLP exporter: no exporter dependency, endpoint, subscriber initialization, or CLI/runtime exporter configuration was added.
- No CLI JSON output change: WS1E's final diff contains no `crates/cdf-cli/**` changes. Concurrent unrelated `cdf-cli` dirty files are present in the workspace but are not part of this ticket.

## Limits

CodeQL reports 0 SARIF findings but has partial Rust extraction coverage, with the extractor warnings and unresolved macro counts listed above.
