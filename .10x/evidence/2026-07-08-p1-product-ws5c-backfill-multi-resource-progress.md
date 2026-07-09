Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress.md, .10x/specs/cli-live-progress.md, .10x/specs/runtime-event-spine.md

# WS5C backfill and multi-slice progress evidence

## What was observed

Executed `cdf backfill` now creates the same non-authoritative CLI progress sink used by `run`, `replay package`, and `resume`, passes it to every per-slice `run_project` call, and attaches its snapshot to human success and failure output. JSON mode still constructs no human progress sink and does not interleave progress.

The progress sink now keys sequence tracking by `(run_id, sequence)` and resets terminal state when a new run id appears. This preserves milestones for backfill slices whose durable run-ledger sequences each start at 1.

Backfill human success output includes headless progress milestone lines for each slice, followed by the existing backfill panel/table and a summary footer with succeeded slices, total rows, and total segments. Backfill failure output annotates the failed slice window, planned package/checkpoint artifacts, mutation status, and the next recovery command when a run id has been recorded; pre-run failures explicitly state that no recovery command is available before a run id exists.

Parent review found one guidance risk after the worker pass: using the latest progress run id alone could choose a previous successful slice's run id if a later slice failed before emitting its own run-start event. The final diff repairs this by adding `ProgressSnapshot::latest_run_id_for_package`, using it for backfill recovery guidance, and adding `latest_run_id_for_package_uses_matching_slice_package_only`.

## Procedure

Focused tests:

```text
cargo test -p cdf-cli restarted_sequences_from_distinct_runs_remain_visible_for_multi_slice_progress --locked
result: pass

cargo test -p cdf-cli latest_run_id_for_package_uses_matching_slice_package_only --locked
result: pass

cargo test -p cdf-cli backfill --locked
result: pass, 7 passed
```

Full package test:

```text
cargo test -p cdf-cli --locked
result after parent repair: pass, 197 lib tests passed, 1 doctor_env integration test passed, 0 doctests
```

Formatting and lint gates:

```text
cargo fmt --all -- --check
result: pass

cargo clippy -p cdf-cli --all-targets --locked -- -D warnings
result: pass

git diff --check
result: pass
```

Duplication and complexity:

```text
npx --yes jscpd@5 crates/cdf-cli/src/backfill_command.rs crates/cdf-cli/src/progress.rs crates/cdf-cli/src/tests.rs .10x/tickets/done/2026-07-08-p1-product-ws5c-backfill-multi-resource-progress.md --format rust,markdown --min-lines 8 --min-tokens 80 --reporters console,json --output target/quality/reports/ws5c-parent-jscpd --ignore "**/target/**,**/.git/**,**/reports/**"
result after parent repair: pass exit; residual 25 clones, 337 duplicated lines, 3.34% duplicated lines, 2346 duplicated tokens, 3.81% duplicated tokens
note: a small backfill test helper reduced the initial WS5C scoped result from 28 clones / 393 duplicated lines / 3.91% after first implementation.

rust-code-analysis-cli -m -O json -p crates/cdf-cli/src/backfill_command.rs
result: sloc=428, cyclomatic_sum=65, cyclomatic_max=13, cognitive_sum=18, cognitive_max=6

rust-code-analysis-cli -m -O json -p crates/cdf-cli/src/progress.rs
result: sloc=1054, cyclomatic_sum=161, cyclomatic_max=17, cognitive_sum=57, cognitive_max=20

rust-code-analysis-cli -m -O json -p crates/cdf-cli/src/tests.rs
result: sloc=8561, cyclomatic_sum=405, cyclomatic_max=9, cognitive_sum=89, cognitive_max=8

scc crates/cdf-cli/src/backfill_command.rs crates/cdf-cli/src/progress.rs crates/cdf-cli/src/tests.rs
result after parent repair: Rust files=3, lines=10082, code=3251, comments=6600, complexity=92
```

Security and supply-chain:

```text
semgrep scan --config p/rust --error crates/cdf-cli/src/backfill_command.rs crates/cdf-cli/src/progress.rs crates/cdf-cli/src/tests.rs
result: pass, 0 findings

gitleaks detect --no-git --no-banner --redact --source <temp copy of touched source and ticket>
result: pass, no leaks found

rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates/cdf-cli/src/backfill_command.rs crates/cdf-cli/src/progress.rs crates/cdf-cli/src/tests.rs
result: pass, no matches

cargo audit
result: pass with allowed RUSTSEC-2024-0436 paste unmaintained warning

cargo deny check
result: pass; residual duplicate Arrow 58.3/59.1 warnings remain the ratified duckdb/DataFusion tuple residual; advisories, bans, licenses, and sources reported ok

repository banned-phrase scan over non-target, non-report, non-git files
result: pass, no matches
```

## What this supports

- Executed backfill attaches a progress sink to each `run_project` slice in human mode.
- Multi-slice progress remains visible despite per-run sequence restarts.
- Headless human output is line-oriented, ANSI-free, bounded by the progress sink capacity, and includes one slice-distinguishing line set per slice through `scope=window:start..end`.
- JSON backfill output remains progress-free.
- Backfill failure output reports the failed slice, planned package/checkpoint artifacts, mutation status, and recovery availability without changing package identity, checkpoint authority, or ledger semantics.
- Recovery guidance uses only progress events that match the failing slice's planned package id, avoiding stale run ids from earlier slices.
- Dropped progress events remain non-authoritative by reusing the existing `CliProgressSink` `try_lock` and bounded-buffer behavior.

## Limits

The natural failure test covers the pre-run duplicate-package failure branch, where no run id exists and therefore no `cdf resume` command is applicable. The recorded-run recovery-command branch is implemented in `annotate_backfill_slice_error` and relies on the same `CliError::with_progress` rendering path covered by WS5B, but this ticket did not add a new runtime failure hook solely to force that branch because new backfill planning or failure-injection semantics were explicitly excluded.
