Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md, .10x/specs/runtime-event-spine.md, .10x/specs/run-orchestration-ledger.md

# P1 product WS1B event fanout subscriber architecture evidence

## What was observed

WS1B refactored the project runtime recorder into an explicit internal event fanout:

- `ProjectRunRecorder` now publishes through `RunEventFanout`.
- `RunEventFanout` has a mandatory `DurableRunLedgerSubscriber` backed by `SqliteRunLedger`.
- Optional live sinks are normalized into `LiveRunEventSubscribers`.
- Publication appends to the durable ledger first and only then emits the exact persisted event envelope to live subscribers.
- Live subscriber results remain ignored; all-dropped live sinks do not fail the run.
- Durable append failures return an error before any live subscriber can receive the event.

No kernel DTO changes were needed. No CLI renderer, tracing bridge, OTLP export, new lifecycle payloads, or package artifact identity changes were made by this worker.

Focused tests prove:

- live subscribers receive events only after the event is visible in the SQLite run ledger;
- all live subscriber emissions can be dropped while the run still succeeds and the ledger remains complete;
- durable ledger append failure prevents live emission;
- redaction validation still rejects raw secret details before live emission;
- package hash, receipt package hash, checkpoint delta package hash, and package lifecycle status remain aligned with the run report.

## Procedure

Inspection before editing:

- `.10x/tickets/done/2026-07-08-p1-product-ws1b-event-fanout-subscriber-architecture.md`
- `.10x/specs/runtime-event-spine.md`
- `.10x/specs/run-orchestration-ledger.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md`
- `.10x/evidence/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md`
- `.10x/reviews/2026-07-08-p1-product-ws1a-run-event-sink-foundation-review.md`
- `QUALITY.md`
- `crates/cdf-kernel/src/run_event.rs`
- `crates/cdf-project/src/runtime/**`
- focused existing runtime tests around live sink ordering, drops, redaction, artifact identity, and ledger order.

Commands run:

- `rustfmt --edition 2024 crates/cdf-project/src/runtime/ledger.rs crates/cdf-project/src/runtime_tests.rs`
- `cargo fmt --all -- --check`
- `cargo test -p cdf-project --locked live_sink`
- `cargo test -p cdf-project --locked project_run_recorder`
- `cargo test -p cdf-project --locked general_project_run_records_ledger_events_in_commit_gate_order`
- `cargo test -p cdf-kernel --locked`
- `cargo test -p cdf-state-sqlite --locked`
- `cargo test -p cdf-project --locked`
- `cargo clippy -p cdf-project -p cdf-kernel -p cdf-state-sqlite --all-targets --locked -- -D warnings`
- `rg -n "\bunsafe\b|extern\s+\"|\bffi\b|\bFFI\b" crates/cdf-project/src/runtime/ledger.rs crates/cdf-project/src/runtime_tests.rs`
- `git diff --check`
- `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-p1-ws1b.json crates/cdf-project/src/runtime/ledger.rs crates/cdf-project/src/runtime_tests.rs`
- `jscpd --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-p1-ws1b-rust --exit-code 0 crates/cdf-project/src/runtime/ledger.rs crates/cdf-project/src/runtime_tests.rs`
- `rust-code-analysis-cli -m -p crates/cdf-project/src/runtime/ledger.rs -O json > target/quality/reports/rust-code-analysis-p1-ws1b-runtime-ledger.json`
- `rust-code-analysis-cli -m -p crates/cdf-project/src/runtime_tests.rs -O json > target/quality/reports/rust-code-analysis-p1-ws1b-runtime-tests.json`
- `scc --format json crates/cdf-project/src/runtime/ledger.rs crates/cdf-project/src/runtime_tests.rs > target/quality/reports/scc-p1-ws1b.json`
- scoped `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p1-ws1b-source.json` over touched source and ticket copies in a temporary directory.
- final scoped `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/gitleaks-p1-ws1b-final.json` over touched source and WS1B/WS1 reference records in a temporary directory.
- `jscpd --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/jscpd-p1-ws1b-records --exit-code 0 <WS1B/WS1 reference records>`
- `tools/codeql-rust-quality.sh`
- `codeql database analyze target/quality/codeql-db-rust codeql/rust-queries --format=sarif-latest --output=target/quality/reports/codeql-rust-current.sarif --rerun`

## Results

- Formatting: `cargo fmt --all -- --check` passed.
- Focused sink/drop/redaction tests: passed, 3 tests.
- Focused recorder tests: passed, 2 tests.
- Ledger ordering focused test: passed.
- Kernel tests: passed, 10 unit tests plus doc tests.
- SQLite state tests: passed, 30 unit tests plus doc tests.
- Full `cdf-project` tests: passed, 83 unit tests plus doc tests.
- Clippy: passed for `cdf-project`, `cdf-kernel`, and `cdf-state-sqlite` with `-D warnings`.
- Direct unsafe/FFI scan: no matches in touched Rust files.
- Diff whitespace check: passed.
- Semgrep: passed with 0 findings; report path `target/quality/reports/semgrep-p1-ws1b.json`.
- Gitleaks source/ticket scan: passed with no leaks; report path `target/quality/reports/gitleaks-p1-ws1b-source.json`.
- Final Gitleaks source/record scan: passed with no leaks; report path `target/quality/reports/gitleaks-p1-ws1b-final.json`.
- `jscpd`: completed; 2 Rust files analyzed, 17 clones, 239 duplicated lines, 4.39% duplicated lines, `newClones = 0`, `newDuplicatedLines = 0`; report path `target/quality/reports/jscpd-p1-ws1b-rust/jscpd-report.json`.
- Record `jscpd`: completed with 0 clones; report path `target/quality/reports/jscpd-p1-ws1b-records/jscpd-report.json`.
- Rust complexity metrics were recorded at `target/quality/reports/rust-code-analysis-p1-ws1b-runtime-ledger.json` and `target/quality/reports/rust-code-analysis-p1-ws1b-runtime-tests.json`.
- Source size metrics were recorded at `target/quality/reports/scc-p1-ws1b.json`.
- CodeQL database refresh through `tools/codeql-rust-quality.sh` completed Cargo build and TRAP import but the wrapper returned nonzero during process/finalization handling. A direct `codeql database analyze` against the refreshed database exited 0 and wrote `target/quality/reports/codeql-rust-current.sarif` with 0 SARIF results.

Parent verification repeated the closure-relevant checks:

- `cargo fmt --all -- --check`: passed.
- `cargo test -p cdf-project --locked live_sink`: passed, including persisted-before-live, all-dropped sink, and redaction-focused tests.
- `cargo test -p cdf-project --locked project_run_recorder`: passed, including durable append failure preventing live emission.
- `cargo test -p cdf-project --locked general_project_run_records_ledger_events_in_commit_gate_order`: passed.
- `cargo clippy -p cdf-project -p cdf-kernel -p cdf-state-sqlite --all-targets --locked -- -D warnings`: passed.
- Scoped `git diff --check` over WS1B source and records: passed.
- Direct unsafe/FFI token scan over touched Rust files: no matches.
- `cargo test -p cdf-project --locked`: passed, 83 unit tests plus doc tests.

## What this supports

This supports closing WS1B. Runtime event publication now has an explicit fanout boundary. SQLite run ledger append remains mandatory and authoritative. Non-authoritative live sinks receive only persisted event envelopes, may drop all events, and cannot fail the run through return values. The refactor preserved existing runtime event order, redaction guardrails, package hashes, receipt identity, checkpoint identity, and package status behavior.

## Limits

This evidence does not cover CLI progress rendering, tracing bridge, OTLP export, new lifecycle payloads, replay/resume fanout expansion, or renderer work. Those are explicitly outside WS1B.

The worktree contains unrelated dirty CLI/render and WASM ticket files from concurrent work; this worker did not inspect, modify, revert, or validate those unrelated changes beyond observing `git status --short`.
