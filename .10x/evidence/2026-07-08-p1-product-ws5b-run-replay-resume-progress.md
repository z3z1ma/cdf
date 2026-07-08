Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws5b-run-replay-resume-progress.md, .10x/specs/cli-live-progress.md, .10x/specs/runtime-event-spine.md

# WS5B run, replay, and resume progress evidence

## What was observed

WS5B was implemented without modifying `crates/cdf-project/src/runtime/**` or backfill paths. The changed CLI paths were `crates/cdf-cli/src/run_command.rs`, `crates/cdf-cli/src/replay_command.rs`, `crates/cdf-cli/src/resume_command.rs`, `crates/cdf-cli/src/resume_command/attempt.rs`, `crates/cdf-cli/src/resume_command/report.rs`, `crates/cdf-cli/src/progress.rs`, `crates/cdf-cli/src/output.rs`, and focused tests in `crates/cdf-cli/src/tests.rs`.

Parent review found one closure gap in the worker implementation: human command errors rendered through `CliError` and therefore lost the accumulated progress snapshot on failure paths. The final diff fixes this by carrying an optional boxed `ProgressSnapshot` on `CliError`, rendering it only for human `InvocationResult` errors through the caller's `RenderConfig`, and attaching snapshots before returning run/replay failures. The added regression test is `replay_package_failure_human_stderr_includes_progress_context`.

Acceptance mapping:

- `cdf run` passes a progress sink to `ProjectRunRequest::event_sink` when `--json` is false. Focused human run tests now assert headless and rich progress output.
- `cdf replay package` records durable progress-equivalent `RunEvent` rows for package verification, checkpoint proposal, destination commit start, segment ack, destination receipt, checkpoint commit, package status update, replay completion, and replay failure. Duplicate replay output includes progress details for `duplicate=true` and `no_op=true`.
- `cdf resume` preloads existing ledger events into the human progress sink, emits newly appended recovery events through the same sink, and the post-finalization resume test removes the source file before recovery while still succeeding from package artifacts.
- Human failure output for replay includes progress context before the error line; JSON failure output remains an error envelope without human progress text.
- JSON mode does not construct a human progress sink and does not attach progress output to JSON envelopes. The replay failure JSON test asserts empty stdout and no progress text on stderr.
- Redaction remains centralized in the progress renderer: normal progress only displays safe IDs and whitelisted metric fields; verbose artifact and destination details use URI userinfo redaction; sensitive event-detail keys redact fail-closed unless the value is only secret references.

## Procedure

Baseline before implementation:

- `cargo fmt --all -- --check` passed.
- `cargo test -p cdf-cli progress --locked` passed.
- `cargo test -p cdf-cli run_human_ --locked` passed.
- `cargo test -p cdf-cli replay_package_human_ --locked` passed.
- `cargo test -p cdf-cli resume_human_ --locked` passed.
- `cargo test -p cdf-cli replay_package_duckdb_duplicate_reports_no_op --locked` passed.
- `cargo test -p cdf-cli resume_finalized_package_without_receipt_replays_without_source_contact --locked` passed.

Focused verification after implementation:

- `cargo test -p cdf-cli progress --locked` passed.
- `cargo test -p cdf-cli run_human_ --locked` passed.
- `cargo test -p cdf-cli replay_package_human_ --locked` passed.
- `cargo test -p cdf-cli resume_human_ --locked` passed.
- `cargo test -p cdf-cli replay_package_duckdb_replays_from_artifacts_without_source_contact --locked` passed.
- `cargo test -p cdf-cli replay_package_duckdb_duplicate_reports_no_op --locked` passed.
- `cargo test -p cdf-cli replay_package_failure_records_progress_events_without_json_progress_output --locked` passed.
- `cargo test -p cdf-cli resume_finalized_package_without_receipt_replays_without_source_contact --locked` passed.
- `cargo test -p cdf-cli resume_finalized_package_human_progress_replays_without_source_contact --locked` passed.
- `cargo test -p cdf-cli replay_package_postgres_replays_from_artifacts_without_source_contact --locked` passed.
- `cargo test -p cdf-cli replay_package_parquet_replays_from_artifacts_without_source_contact --locked` passed.
- `cargo test -p cdf-cli replay_package_without_to_uses_environment_destination_without_source_contact --locked` passed.
- `cargo test -p cdf-cli run_local_file_to_duckdb_commits_package_rows_mirrors_and_checkpoint --locked` passed.
- `cargo check -p cdf-cli --all-targets --locked` passed.

Full and quality verification:

- `cargo test -p cdf-cli --locked` passed after the parent correction: 193 unit tests, 1 integration test, and 0 doc tests passed.
- `cargo test -p cdf-cli replay_package_failure --locked` passed after the parent correction: 2 tests passed.
- `cargo test -p cdf-cli progress --locked` passed after the parent correction: 12 tests passed.
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings` passed after boxing progress/remediation cold-path fields. An earlier parent-review run failed because an unboxed progress snapshot made `CliError` hit Clippy's `result_large_err` threshold.
- `cargo fmt --all -- --check` passed.
- `git diff --check` passed after source and record updates.
- `jscpd --format rust,markdown --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/ws5b-parent-jscpd <9 touched Rust files plus ticket>` exited 0. Report summary: 9 Rust sources, 11,656 lines, 27 clones, 372 duplicated lines, 2,521 duplicated tokens, 3.19 percent duplicated lines.
- `rust-code-analysis-cli -m -O json` passed for `progress.rs`, `replay_command.rs`, `run_command.rs`, and `output.rs` after the parent correction. Report highlights: `progress.rs` cyclomatic max 16 and cognitive max 19; `replay_command.rs` cyclomatic max 17 and cognitive max 5; `run_command.rs` cyclomatic max 15 and cognitive max 5; `output.rs` cyclomatic max 5 and cognitive max 11.
- `scc --by-file <9 touched Rust files>` passed after the parent correction: 9 Rust files, 11,656 lines, 4,765 code lines, total complexity 159.
- `semgrep scan --config p/rust --error <8 touched Rust files>` passed with 0 findings.
- `gitleaks detect --no-git --no-banner --redact --source <temporary copy of touched source and WS5B records>` passed with 0 leaks.
- Direct unsafe scan with `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" <9 touched Rust files>` found no matches.
- Banned phrase scan with `rg -n "killer demo" . --glob '!target/**' --glob '!reports/**' --glob '!.git/**' -i` found no matches.
- `cargo audit` exited 0 with one existing allowed warning for unmaintained `paste` `RUSTSEC-2024-0436`.
- `cargo deny check` exited 0 with policy result `advisories ok, bans ok, licenses ok, sources ok`; it printed existing duplicate dependency warnings.

## What this supports or challenges

This supports closing WS5B: the requested command families now surface human-only live progress, replay/resume append or subscribe to progress-equivalent ledger events, JSON envelopes remain isolated from progress text, and deterministic tests cover the requested success, failure, duplicate, and post-finalization recovery cases.

The evidence also supports the implementation boundary: no runtime or backfill source files were changed, and no NDJSON output path was added.

## Limits

The terminal evidence is deterministic test output, not an external terminal recording artifact. `jscpd`, `cargo audit`, and `cargo deny` reported existing advisory or duplication context that is outside WS5B scope and did not fail their configured checks.
