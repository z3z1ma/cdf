Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md

# P1 product WS1A run event sink foundation evidence

## What was observed

P1 product WS1A implemented the first runtime event-sink slice:

- Shared run event DTOs now live in `cdf-kernel`: `RunEventKind`, `RunEventDetails`, `RunEventValue`, `RunEventAppend`, `RunEvent`, and `SecretReference`.
- `cdf-state-sqlite` consumes those kernel DTOs and preserves the existing public `cdf_state_sqlite::{RunEvent*, SecretReference}` re-exports for callers.
- `cdf-kernel` exports a synchronous `RunEventSink` contract with `try_emit(&RunEvent) -> RunEventSinkResult`.
- `ProjectRunRequest` accepts `event_sink: Option<&dyn RunEventSink>`.
- `ProjectRunRecorder` emits to the optional sink only after `SqliteRunLedger::append_event` returns the persisted event envelope.
- Full/slow sinks can return `Dropped`; the recorder ignores that result and run success remains ledger-authoritative.

Focused runtime tests prove:

- live sink events equal the persisted ledger events for a successful run;
- a bounded/full sink drops events without failing the run or truncating ledger completeness;
- raw secret strings rejected by ledger validation are not emitted to the live sink, while typed `SecretRef` is accepted and emitted.

Package status, checkpoint commit status, receipt verification, row count, and persisted ledger order assertions remain present on the touched successful run path.

## Procedure

Build and test checks:

- `cargo fmt --all --check`
- `cargo test -p cdf-kernel --locked`
- `cargo test -p cdf-state-sqlite --locked`
- `cargo test -p cdf-project --locked live_sink`
- `cargo clippy -p cdf-kernel -p cdf-state-sqlite -p cdf-project --all-targets --locked -- -D warnings`
- `cargo check -p cdf-cli -p cdf-conformance -p cdf-benchmarks --locked`
- `git diff --check`

Quality and scanner checks:

- Direct unsafe/FFI scan over touched Rust files:
  `rg -n "\bunsafe\b|extern\s+\"|\bffi\b|\bFFI\b" <touched Rust files>`
- Source-only Gitleaks scan from a temporary directory containing only touched Rust files and final WS1A `.10x` records:
  `gitleaks dir --no-banner --redact --report-format json --report-path /tmp/firn-ws1a-final-gitleaks.json /tmp/firn-ws1a-final-gitleaks.*`
- Focused `jscpd` scan over touched Rust files and final WS1A `.10x` records:
  `jscpd --min-lines 8 --min-tokens 80 --reporters console,json --output /tmp/firn-ws1a-final-jscpd --exit-code 0 <touched Rust files> .10x/tickets/done/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md .10x/evidence/2026-07-08-p1-product-ws1a-run-event-sink-foundation.md .10x/reviews/2026-07-08-p1-product-ws1a-run-event-sink-foundation-review.md`

## Results

- Formatting: passed.
- Kernel tests: passed, 10 unit tests plus doc tests.
- SQLite state tests: passed, 30 unit tests plus doc tests.
- Focused project live-sink tests: passed, 3 tests, 79 filtered out.
- Clippy: passed with `-D warnings` for the three touched packages.
- CLI/conformance/benchmark compile-fix check: passed.
- Diff whitespace check: passed.
- Direct unsafe/FFI scan: no `unsafe` or `extern` matches; one benign existing `std::ffi::OsString` import appeared in `crates/cdf-conformance/src/mvp_acceptance_demo.rs`.
- Gitleaks source-only scan: passed with no leaks.
- `jscpd`: completed; 16 Rust files analyzed, 22 clones, 330 duplicated lines, 3.44% duplicated lines, `newClones = 0`, `newDuplicatedLines = 0`. Report path: `/tmp/firn-ws1a-final-jscpd/jscpd-report.json`.

## Parent verification

Parent re-verified the slice after subagent completion and after adding public trait-contract docs plus repairing the WS1 parent pointer to the moved done ticket.

Parent commands:

- `cargo fmt --all --check`
- `git diff --check`
- `rg -n "[Kk]iller[ _-]?[Dd]emo" . --hidden`
- `cargo test -p cdf-kernel --locked`
- `cargo test -p cdf-state-sqlite --locked`
- `cargo test -p cdf-project --locked live_sink`
- `cargo clippy -p cdf-kernel -p cdf-state-sqlite -p cdf-project --all-targets --locked -- -D warnings`
- `cargo check -p cdf-cli -p cdf-conformance -p cdf-benchmarks --locked`
- Direct unsafe/FFI pattern scan over touched Rust files.
- Source-only `gitleaks dir` over touched Rust and WS1A records.
- Focused `jscpd` over touched Rust files and WS1A records.
- `rust-code-analysis-cli -m -O json` for `crates/cdf-kernel/src/run_event.rs` and `crates/cdf-project/src/runtime/ledger.rs`.
- `scc --format json` for the touched runtime/event source set.
- `cargo audit --json`
- `cargo deny check advisories`
- `cargo vet --locked`
- `osv-scanner --lockfile Cargo.lock --format json`
- `semgrep scan --config p/rust` over touched Rust files.
- `tools/codeql-rust-quality.sh`
- `cargo geiger --forbid-only` for `cdf-kernel`, `cdf-state-sqlite`, and `cdf-project`.

Parent results:

- Formatting, diff whitespace, kernel tests, SQLite tests, focused live-sink project tests, clippy, and compile checks passed.
- The forbidden demo phrase scan found no matches.
- Direct unsafe/FFI scan over touched Rust files found no matches.
- Gitleaks passed with no leaks; report path: `target/quality/reports/gitleaks-p1-ws1a-parent.json`.
- Rust `jscpd` report: 16 files, 22 clones, 330 duplicated lines, 3.44%, `newClones = 0`, `newDuplicatedLines = 0`; report path: `target/quality/reports/jscpd-p1-ws1a-rust-parent/jscpd-report.json`.
- Record `jscpd` report: 4 files, 0 clones; report path: `target/quality/reports/jscpd-p1-ws1a-records-parent/jscpd-report.json`.
- Rust complexity reports were recorded at `target/quality/reports/rust-code-analysis-p1-ws1a-run-event.json` and `target/quality/reports/rust-code-analysis-p1-ws1a-runtime-ledger.json`.
- Source size metrics were recorded at `target/quality/reports/scc-p1-ws1a-parent.json`.
- `cargo audit`, `cargo deny check advisories`, `cargo vet`, and Semgrep passed; Semgrep produced 0 findings.
- OSV exited nonzero only for the already-ratified `paste` advisory `RUSTSEC-2024-0436`; report path: `target/quality/reports/osv-p1-ws1a-parent.json`.
- CodeQL refreshed `target/quality/codeql-db-rust` because the Rust input fingerprint changed, then passed analysis with 0 SARIF results; report path: `target/quality/reports/codeql-rust-current.sarif`.
- Default Geiger package scans made no progress after several minutes and were stopped; `--forbid-only` scans completed with exit 0 and wrote package reports under `target/quality/reports/cargo-geiger-p1-ws1a-*-forbid-only.json`.

## What this supports

This supports closing P1 product WS1A. The event model is no longer SQLite-owned, the SQLite public API remains compatible, live event sinks see exact persisted event envelopes after durable append, dropped live events do not affect run success or ledger completeness, and secret-detail guardrails run before live emission.

## Limits

This slice does not implement CLI rendering, tracing/OTLP export, NDJSON streaming, replay/resume fanout refactors, or broader event-spine subscribers. `RunRecord` and `RunLedgerSnapshot` remain SQLite-ledger DTOs because this slice only needed the shared event model and live event envelope in `cdf-kernel`.
