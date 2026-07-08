Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth.md, .10x/specs/runtime-event-spine.md, .10x/specs/run-orchestration-ledger.md

# P1 product WS1C event lifecycle and payload breadth evidence

## What was observed

WS1C added event lifecycle and payload breadth on top of the WS1B fanout boundary:

- `cdf-kernel::RunEventKind` now includes `package_segment_recorded` and `destination_segment_acknowledged`.
- The SQLite run ledger schema was widened to v3 so durable append accepts the additive event vocabulary; v1 and v2 ledgers migrate forward without changing existing event rows.
- Package execution emits one package segment progress event per finalized package segment before `package_finalized`.
- Destination replay emits one destination segment acknowledgment event per successful `CommitSession::write_segment` before `destination_receipt_recorded`.
- Existing lifecycle events now carry quantitative details when available: row, byte, batch, segment, phase, quarantine record count, receipt counts, migration count, checkpoint segment totals, elapsed milliseconds on terminal events, and retry/backoff fields only when the propagated `CdfError` carries `retry_after_ms`.
- Missing optional receipt counts remain omitted when the receipt has `None`.
- Redaction validation remains centralized in durable ledger append before live sink publication.

The implementation did not add CLI rendering, tracing bridge, OTLP export, or retry behavior. It did not alter package identity, receipt verification, checkpoint authority, or destination commit semantics.

## Procedure

Inspected before editing:

- `.10x/tickets/done/2026-07-08-p1-product-ws1c-event-lifecycle-payload-breadth.md`
- `.10x/specs/runtime-event-spine.md`
- `.10x/specs/run-orchestration-ledger.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md`
- WS1A/WS1B evidence and review records
- `QUALITY.md`
- `crates/cdf-kernel/src/run_event.rs`
- `crates/cdf-project/src/runtime/**`

Commands run:

- `rustfmt --edition 2024` over touched WS1C Rust files.
- `cargo fmt --all -- --check`
- `cargo test -p cdf-project --locked general_project_run_records_ledger_events_in_commit_gate_order`
- `cargo test -p cdf-project --locked general_project_run_records_failure_after_durable_receipt_without_advancing_state`
- `cargo test -p cdf-project --locked live_sink`
- `cargo test -p cdf-project --locked project_run_recorder`
- `cargo test -p cdf-kernel --locked`
- `cargo test -p cdf-state-sqlite --locked`
- `cargo test -p cdf-project --locked`
- `cargo test -p cdf-conformance --locked package_replay`
- `cargo clippy -p cdf-kernel -p cdf-state-sqlite -p cdf-project --all-targets --locked -- -D warnings`
- `cargo check --workspace --all-targets --locked`
- `git diff --check -- <touched WS1C files>`
- Direct unsafe/FFI scan over touched Rust files: `rg -n "\bunsafe\b|extern\s+\"|\bffi\b|\bFFI\b" ...`
- Semgrep Rust scan over touched Rust files: `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-p1-ws1c.json ...`
- Scoped Gitleaks scan over copied touched source files: `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p1-ws1c-source.json ...`
- `jscpd --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-p1-ws1c-rust --exit-code 0 ...`
- `rust-code-analysis-cli -m -p ... -O json` for `runtime/ledger.rs`, `runtime/replay.rs`, `runtime_tests.rs`, and SQLite `run_ledger.rs`.
- `scc --format json ... > target/quality/reports/scc-p1-ws1c.json`
- `tools/codeql-rust-quality.sh`
- Final scoped Gitleaks scan over touched source plus WS1C ticket/evidence/review copies.
- Final record `jscpd` over WS1C ticket/evidence/review records.

## Results

- Formatting passed.
- Focused success lifecycle ordering/payload test passed.
- Focused failure lifecycle ordering/payload test passed.
- Focused live sink/drop/redaction tests passed.
- Recorder redaction and durable-append failure tests passed.
- Kernel tests passed: 10 unit tests plus doc tests.
- SQLite state tests passed: 30 unit tests plus doc tests.
- Full `cdf-project` passed: 83 unit tests plus doc tests.
- Focused conformance package replay tests passed: 11 tests.
- Clippy passed for `cdf-kernel`, `cdf-state-sqlite`, and `cdf-project` with `-D warnings`.
- Workspace check passed across all targets with locked dependencies.
- Scoped diff whitespace check passed.
- Direct unsafe/FFI scan found no matches.
- Semgrep passed with 0 findings; report path `target/quality/reports/semgrep-p1-ws1c.json`.
- Source-only Gitleaks passed with no leaks; report path `target/quality/reports/gitleaks-p1-ws1c-source.json`.
- `jscpd` completed over touched Rust files: 9 files, 20 clones, 329 duplicated lines, 3.47%; report path `target/quality/reports/jscpd-p1-ws1c-rust/jscpd-report.json`.
- Complexity reports were written under `target/quality/reports/rust-code-analysis-p1-ws1c-*.json`.
- Source size metrics were written to `target/quality/reports/scc-p1-ws1c.json`.
- CodeQL refreshed `target/quality/codeql-db-rust`, completed analysis, and wrote `target/quality/reports/codeql-rust-current.sarif` with 0 SARIF results. The Rust extractor reported 3717 extraction warnings and many unresolved macros, matching known coverage limits for this repository's Rust CodeQL setup.
- Final source/record Gitleaks passed with no leaks; report path `target/quality/reports/gitleaks-p1-ws1c-final.json`.
- Final record `jscpd` completed with 0 clones; report path `target/quality/reports/jscpd-p1-ws1c-records/jscpd-report.json`.

## What this supports

This supports closing WS1C. The runtime event spine now covers package segment progress and destination segment acknowledgments where trustworthy values exist, and existing lifecycle events carry quantitative payloads without inventing unavailable totals. Successful and failing lifecycle ordering are tested, and redaction still happens before live subscriber emission.

## Limits

No retry events beyond terminal error retry/backoff metadata were added because the runtime did not have retry behavior to report and the ticket explicitly excluded adding retries solely for events. Replay/resume/backfill entry points outside the current runtime stage-hook path remain broader WS1/WS5 surfaces. CLI rendering remains WS3C-owned.
