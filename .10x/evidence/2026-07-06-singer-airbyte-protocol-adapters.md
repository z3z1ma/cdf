Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-singer-airbyte-protocol-adapters.md

# Singer and Airbyte protocol adapter verification

## What was observed

`cdf-subprocess` now exposes focused `protocol`, `singer`, and `airbyte` modules behind a thin crate root. The adapters parse newline-delimited JSON protocol messages, retain raw parsed envelopes for forward-compatible unknown fields, convert records into per-stream `FormatRead` values with stream identity preserved in scope and batch IDs, and map Singer/Airbyte opaque state into `SourcePosition::ForeignState`.

Parent review found that using `serde_json::to_vec` made opaque state hashes depend on object insertion order. The implementation now writes canonical JSON with sorted object keys before hashing, and tests assert canonical bytes and hashes across reordered state objects.

Mutation testing initially found 24 missed mutants in parser helper edges. Tests were hardened for explicit/implicit Airbyte legacy state, required schema/record/catalog object fields, array-of-string validation, integer timestamp validation, blank-line line numbers, canonical JSON separators, and stream ID sanitization. The final adapter mutation run had 0 missed mutants.

The existing dirty `.gitignore` remained unrelated and untouched. `Cargo.lock` changed only to record the new direct `cdf-subprocess` JSON/hash dependencies required by `crates/cdf-subprocess/Cargo.toml`.

## Procedure

- Re-read `VISION.md` sections for Tier 4 subprocess adapters, Singer/Airbyte mapping, `ForeignState`, bridge discipline, and Airbyte destination exclusion. Result: this ticket remained scoped to parser/batch/state bridging; package archive and Airbyte destinations stayed excluded.
- Ran `cargo test -p cdf-subprocess --no-fail-fast` once after adding direct dependencies so Cargo could refresh lockfile metadata required by later `--locked` verification. Result before parent hardening: passed, 10 unit tests and 0 doc tests.
- Ran focused final adapter checks after parent review and mutation hardening:
  - `cargo fmt --all -- --check`: passed.
  - `cargo test -p cdf-subprocess --locked --no-fail-fast`: passed, 12 unit tests and 0 doc tests.
  - `cargo clippy -p cdf-subprocess --all-targets --locked -- -D warnings`: passed.
  - `cargo +nightly careful test -p cdf-subprocess --locked`: passed, 12 unit tests and 0 doc tests. Installed nightly/rust-src as required by the tool.
  - `cargo mutants --package cdf-subprocess --no-shuffle --jobs 4 --timeout 120 --output target/quality/reports/mutants-singer-airbyte`: final result 109 mutants tested, 82 caught, 27 unviable, 0 missed.
- Ran workspace compile/lint/test gates:
  - `cargo check --workspace --all-targets --locked`: passed.
  - `cargo check --workspace --all-targets --all-features --locked`: passed.
  - `cargo check --workspace --all-targets --no-default-features --locked`: passed.
  - `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
  - `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
  - `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed.
  - `cargo hack check --workspace --all-targets --each-feature --locked`: passed.
  - `cargo hack clippy --workspace --all-targets --each-feature --locked -- -D warnings`: passed.
  - `cargo test --workspace --all-targets --locked --no-fail-fast`: passed, including 12 `cdf-subprocess` tests.
  - `cargo nextest run --workspace --locked`: passed, 140 tests.
  - `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed.
  - `cargo doc --workspace --all-features --no-deps --locked`: passed.
  - `cargo semver-checks --workspace --baseline-rev HEAD`: passed; no semver update required.
- Ran coverage, dependency, maintainability, and size checks:
  - `cargo llvm-cov --workspace --all-features --locked --summary-only`: passed. Total coverage: 74.09% regions, 73.38% functions, 77.43% lines. New adapter files: `airbyte.rs` 85.03% lines, `protocol.rs` 85.87% lines, `singer.rs` 95.65% lines.
  - `cargo machete`: passed, no unused dependencies.
  - `cargo +nightly udeps --workspace --all-targets --all-features --locked`: passed, all dependencies used.
  - `rust-code-analysis-cli -m -p crates -O json -o target/quality/reports/rust-code-analysis-singer-airbyte`: passed and wrote JSON metrics.
  - `jscpd . --reporters json,console --output target/quality/reports/jscpd-singer-airbyte --ignore "**/target/**,**/.git/**,**/reports/**"`: completed; 134 existing clone blocks, total duplicated lines 3.26%, Rust duplicated lines 1.73%. New notable local clone is the shared import prefix between `airbyte.rs` and `singer.rs`.
  - `cargo bloat --release -p cdf-cli --bin cdf-cli -n 20`: passed; top entries remain DuckDB symbols, `.text` 28.9 MiB and file size 45.6 MiB.
- Ran security/supply-chain checks:
  - Direct source search for `unsafe`, FFI, raw pointer, `Send`, and `Sync` terms under `crates`: no local unsafe/FFI/raw-pointer hits; results were safe trait bounds or prose.
  - `CARGO_TARGET_DIR=target/quality/geiger-target cargo geiger --manifest-path /Users/alexanderbut/code_projects/personal/cdf/crates/cdf-subprocess/Cargo.toml --all-targets --all-features --include-tests --locked`: completed inventory but exited 1 with 5 tool/dependency warnings and dependency unsafe counts. The `cdf-subprocess` local package lines showed no local unsafe blocks.
  - `semgrep scan --config p/rust --error crates/cdf-subprocess/src/protocol.rs crates/cdf-subprocess/src/singer.rs crates/cdf-subprocess/src/airbyte.rs crates/cdf-subprocess/src/tests.rs`: passed, 0 findings.
  - `semgrep scan --config p/security-audit --error crates/cdf-subprocess/src/protocol.rs crates/cdf-subprocess/src/singer.rs crates/cdf-subprocess/src/airbyte.rs crates/cdf-subprocess/src/tests.rs`: passed, 0 findings.
  - `tools/codeql-rust-quality.sh`: refreshed reusable database `target/quality/codeql-db-rust` because Rust source changed. SARIF summary: 0 results, 1 run. Extractor metrics: 118/118 Rust files scanned, 0 extraction errors, 1277 extraction warnings, 83 files extracted with errors, 35 without, 1317 macro calls with 40 resolved and 1277 unresolved.
  - `gitleaks git --no-banner --redact .`: passed, no leaks.
  - Source snapshot `gitleaks dir --no-banner --redact "$tmp"` over `git ls-files --cached --others --exclude-standard`: passed, no leaks, including untracked new files.
  - `osv-scanner scan source -r .`: passed, no issues.
  - `cargo audit --json > target/quality/reports/cargo-audit-singer-airbyte-final.json`: passed; vulnerabilities `found=false count=0`.
  - `cargo deny check advisories`: passed.
  - `cargo deny check`: failed only at the existing unratified license allowlist policy; advisories, bans, and sources were ok. Existing owner: `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
  - `cargo vet`: failed because `supply-chain/` is not initialized. Existing owner: `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.
- Ran `git diff --check -- . ':(exclude).gitignore'`. Result: passed.
- Inspected `git status --short`. Result: expected edits were the ticket, evidence/review records, `Cargo.lock`, `crates/cdf-subprocess/Cargo.toml`, and `crates/cdf-subprocess/src/**`; pre-existing `.gitignore` remained dirty.

## What this supports or challenges

This supports the ticket acceptance criteria for Singer schema/record/state parsing, Airbyte catalog/record/legacy-stream-global state parsing, malformed required-field handling as `Data` errors without raw state leakage, deterministic canonical `ForeignState` hashing, per-stream batch conversion, thin crate-root exports, and package write/replay compatibility through existing package APIs.

The `cargo deny check` and `cargo vet` failures challenge repository policy readiness, not this adapter implementation. They remain governed by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.

## Limits

The implementation is scoped to `cdf-subprocess` parser/batch/state bridging. It does not exercise full connector lifecycle execution, Airbyte destinations, Parquet archive/transcode behavior, state migration UI, or package archive CLI behavior, all of which are explicit exclusions from the ticket.
