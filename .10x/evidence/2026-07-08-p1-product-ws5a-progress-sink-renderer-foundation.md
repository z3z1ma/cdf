Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md, .10x/specs/cli-live-progress.md, .10x/specs/runtime-event-spine.md, .10x/decisions/cli-design-language-and-renderer.md

# P1 product WS5A progress sink and renderer foundation evidence

## What was observed

WS5A added the CLI live-progress foundation without wiring command paths end to end.

Observed implementation facts:

- `crates/cdf-cli/src/progress.rs` now exists and is registered from `crates/cdf-cli/src/lib.rs`.
- `crates/cdf-cli/src/render/redaction.rs` exposes the shared sensitive-key predicate used by the progress renderer.
- `CliProgressSink` implements `cdf_kernel::RunEventSink`.
- `CliProgressSink::try_emit` uses `Mutex::try_lock`, so a contended progress state returns `RunEventSinkResult::Dropped` instead of blocking the runtime publisher.
- The sink has bounded milestone buffering. Full buffers drop nonterminal progress events and count drops; terminal events evict the oldest milestone so the final progress state remains visible inside the bounded buffer.
- Duplicate sequences, out-of-order sequences, and post-terminal events are deterministic no-ops that do not move the progress phase.
- Progress phases cover the current `RunEventKind` vocabulary: plan, extract, validate, package, commit, verify, and gate. `run_failed` keeps the current failed phase.
- `ProgressConfig` wraps the existing `RenderConfig`, display verbosity, and capacity. Headless and interactive rendering therefore share renderer configuration.
- `DisplayVerbosity` represents quiet, normal, and verbose progress modes. WS5A did not add parser flags because command grammar wiring is explicitly excluded from this child and WS2C did not establish active `-v`/`-q` parser fields.
- Progress redaction happens before rendering. `SecretRef` values render as `[redacted]`, sensitive-key raw values fail closed to `[redacted]`, and string fields pass through URI userinfo redaction.
- Headless rendering emits timestamped line-oriented milestone logs with no ANSI or carriage-return terminal control. Interactive rendering uses existing `RenderDocument` primitives: section rule, status line, key-value panel, and table.
- No run/replay/resume/backfill command path was wired to live progress. No NDJSON event stream was added. Runtime event semantics were not changed.

The phase mapping for `package_segment_recorded` and `destination_segment_acknowledged` is an explicit foundation mapping for current WS1C event vocabulary: package segment progress maps to extract, and destination segment acknowledgments map to commit. The active live-progress spec did not list those newer event kinds in its initial table, but the runtime event spine requires segment/batch progress where available and current runtime source emits these events in those lifecycle regions.

## Procedure

Inspected before editing:

- The WS5A ticket before closure, now archived at `.10x/tickets/done/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md`.
- `.10x/specs/cli-live-progress.md`.
- `.10x/specs/runtime-event-spine.md`.
- `.10x/decisions/cli-design-language-and-renderer.md`.
- `.10x/evidence/2026-07-08-p1-product-ws3b-renderer-foundation.md`.
- `.10x/reviews/2026-07-08-p1-product-ws3b-renderer-foundation-review.md`.
- `.10x/tickets/done/2026-07-08-p1-product-ws2c-product-grammar-semantics.md`.
- `QUALITY.md`.
- `crates/cdf-cli/src/render/**`.
- `crates/cdf-cli/src/output.rs`.
- `crates/cdf-cli/src/args.rs`.
- `crates/cdf-cli/src/commands.rs`.
- `crates/cdf-kernel/src/run_event.rs`.
- `crates/cdf-project/src/runtime/types.rs`.
- `crates/cdf-project/src/runtime/ledger.rs`.
- `crates/cdf-project/src/runtime/tracing_bridge.rs`.

Implementation changed:

- `crates/cdf-cli/src/progress.rs`.
- `crates/cdf-cli/src/lib.rs`.
- `crates/cdf-cli/src/render/redaction.rs`.

Focused unit tests added under `crates/cdf-cli/src/progress.rs`:

- `phase_mapping_follows_live_progress_spec`.
- `run_failed_stays_on_current_failed_phase_and_closes_terminal_state`.
- `duplicate_and_out_of_order_events_are_deterministic_noops`.
- `backpressure_drops_nonterminal_events_without_blocking`.
- `terminal_event_evicts_oldest_milestone_when_buffer_is_full`.
- `redaction_applies_before_headless_and_interactive_rendering`, including URI userinfo, `SecretRef`, and sensitive-key raw-string fallback redaction.
- `headless_formatting_is_line_oriented_and_ansi_free`.
- `quiet_suppresses_live_progress_while_verbose_includes_event_details`.

Verification commands run:

- `cargo fmt --all -- --check`.
- `cargo test -p cdf-cli progress --locked`.
- `cargo test -p cdf-cli --locked`.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`.
- Direct unsafe-token scan over `crates/cdf-cli/src/progress.rs` and `crates/cdf-cli/src/lib.rs`.
- `jscpd --format rust,markdown --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/ws5a-jscpd --exit-code 0 ...` scoped to `crates/cdf-cli/src/progress.rs`, `crates/cdf-cli/src/lib.rs`, and the WS5A ticket record.
- `scc --by-file crates/cdf-cli/src/progress.rs crates/cdf-cli/src/lib.rs`.
- `rust-code-analysis-cli -m -O json -p crates/cdf-cli/src/progress.rs > target/quality/reports/ws5a-rust-code-analysis/progress.json`.
- `semgrep scan --config p/rust --error crates/cdf-cli/src/progress.rs crates/cdf-cli/src/lib.rs`.
- `gitleaks dir --no-banner --redact --report-format json --report-path target/quality/reports/ws5a-gitleaks-cdf-cli-src.json crates/cdf-cli/src`.
- `git diff --check`.
- Parent review reran `cargo test -p cdf-cli progress --locked`, `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`, `cargo fmt --all -- --check`, scoped `jscpd`, scoped `rust-code-analysis-cli`, scoped Semgrep, scoped Gitleaks, an unsafe-token scan, banned-phrase scan, and `git diff --check` after adding the shared sensitive-key redaction guard.

An initial `rust-code-analysis-cli` invocation used an invalid positional file argument and exited with usage error. It was rerun with the supported `-p` option and then persisted to `target/quality/reports/ws5a-rust-code-analysis/progress.json`.

## Results

- `cargo fmt --all -- --check`: passed.
- Focused progress tests: passed, 8 tests.
- `cargo test -p cdf-cli --locked`: passed, 189 library tests, 1 integration test, and 0 doc tests.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- Direct unsafe-token scan: no matches.
- `jscpd`: 2 Rust files analyzed, 902 lines, 5,396 tokens, 0 clones, 0 duplicated lines, report at `target/quality/reports/ws5a-jscpd/jscpd-report.json`.
- `scc`: 2 Rust files, 902 lines, 811 code lines, total complexity 43. `crates/cdf-cli/src/progress.rs` was 853 lines, 767 code lines, complexity 41.
- `rust-code-analysis-cli`: `crates/cdf-cli/src/progress.rs` metrics persisted at `target/quality/reports/ws5a-rust-code-analysis/progress.json`; total cyclomatic complexity 130, max function cyclomatic 14, total cognitive complexity 46, max function cognitive 15, SLOC 853.
- Semgrep Rust scan: passed with 0 findings.
- Source-only Gitleaks over `crates/cdf-cli/src`: passed with 0 leaks; report at `target/quality/reports/ws5a-gitleaks-cdf-cli-src.json`.
- `git diff --check`: passed.
- Parent rerun after redaction hardening: `cargo test -p cdf-cli progress --locked` passed with 8 tests; `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings` passed; `cargo fmt --all -- --check` passed; `git diff --check` passed.
- Parent scoped `jscpd`: 3 Rust files analyzed, 992 lines, 6,014 tokens, 0 clones, 0 duplicated lines.
- Parent scoped `rust-code-analysis-cli`: `crates/cdf-cli/src/progress.rs` SLOC 875, total cyclomatic complexity 137, max function cyclomatic 14, total cognitive complexity 49, max function cognitive 15.
- Parent scoped Semgrep Rust scan over `progress.rs`, `render/redaction.rs`, and `lib.rs`: passed with 0 findings.
- Parent scoped temp-copy Gitleaks scan over touched source and WS5A records: passed with 0 leaks.
- Parent unsafe-token scan over touched CLI source: no matches. Parent banned-phrase scan: no matches.

## What this supports

This supports closing WS5A. The CLI now has a dormant progress subscriber foundation that consumes persisted run-event envelopes through the non-blocking `RunEventSink` API, maps events into product progress phases, redacts before display, represents quiet/normal/verbose progress modes, renders deterministic headless milestones and interactive renderer primitives, and has focused unit coverage for the required accepted/drop/duplicate/out-of-order/terminal cases.

## Limits

This evidence does not claim live progress is wired into `cdf run`, `cdf replay package`, `cdf resume`, or `cdf backfill`; WS5B and WS5C own command wiring. It does not claim recorded TTY sessions, rate limiting, progress bars, spinners, or multi-resource backfill summaries; later WS5 children own those surfaces. It does not add or ratify NDJSON event streaming. It does not change runtime event semantics.
