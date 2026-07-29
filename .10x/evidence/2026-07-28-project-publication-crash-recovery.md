Status: recorded
Created: 2026-07-28
Updated: 2026-07-28
Relates-To: `.10x/tickets/done/2026-07-27-cdf-add-crash-publication-recovery.md`

# Project publication crash recovery

## Observation

Commit `1b178bfc` introduced a durable generation-stamped pending/committed journal for multi-file
project publication, and commit `03f3f359` repaired the two material defects found by its bounded
delegated review. The final state machine has one post-marker decision: forward recovery. It does
not destructively roll installed targets back after the pending marker is durable.

Ordinary project loads observe the marker without mutation and fail `Contract` while publication
is pending. A real non-dry-run `cdf add` is the explicit recovery entry point. Plan, preview, and
dry-run add therefore retain their no-write contract. Recovery accepts only the journaled prior or
new hash for each target, preserves unrelated target authority, and treats malformed/missing
CDF-private marker or temporary state as `Internal`. Host access/resource failures remain
`Environment`.

## Procedure and results

- A child test process exited with code `86` without unwinding after installing `cdf.toml` and
  before installing `cdf.lock`. The parent observed the pending marker, missing lock, visible new
  project, completed recovery, and proved an identical retry converged.
- Focused project transaction suite:
  `cargo test -p cdf-project project_files::tests --locked --lib` — 18 passed.
- Product recovery/no-write regression:
  `cargo test -p cdf-cli read_only_load_fails_closed_and_real_add_completes_pending_publication --locked --lib`
  — 1 passed. Both plan and preview left a byte-for-byte tree snapshot unchanged while pending;
  the following real add recovered and the new resource planned successfully.
- Full CLI library:
  `cargo test -p cdf-cli --locked --lib` with the local DuckDB library environment — 300 passed
  in 56.31 seconds.
- Strict affected-root lint:
  `cargo clippy -p cdf-project -p cdf-cli --all-features --all-targets --locked -- -D warnings`
  — passed.
- `cargo fmt --all`, `git diff --check`, and focused test reruns passed.

The earlier broad `cdf-project` run passed the complete publication/recovery surface but exposed the
pre-existing nondeterministic
`tests::recorded_http_multifile_packages_are_jobs_invariant` overlap assertion
(`parallel_progress.peak_active_streams >= 2`). Its exact rerun reproduced the same unrelated
scheduler-overlap failure. The remaining long unrelated replay run was stopped rather than spun
indefinitely; this record does not claim a clean full `cdf-project` suite.

## Review

Two independent read-only reviewers inspected frozen commit `1b178bfc` under the selected
open-code-review-delegate rules. Their deduplicated material findings were:

1. automatic load recovery mutated plan/preview project authority;
2. rollback after a durable pending marker could consume recovery material, become unrecoverable
   after another failure, or overwrite a non-cooperating editor;
3. private journal corruption was classified as external data.

Commit `03f3f359` repaired the complete batch. Permanent regressions cover read-only fail-closed
loads, explicit real-add recovery, forward convergence after in-process failure, preservation of a
post-install racer, malformed private markers, missing private temporaries, and the process-exit
window. Per the bounded review policy, no serial re-review was commissioned after the direct
repairs.

## What this supports

This supports every acceptance criterion in
`.10x/tickets/done/2026-07-27-cdf-add-crash-publication-recovery.md`: real process loss is
reproduced, execution-facing read paths fail closed without mutation, explicit retry converges,
and unrelated authority is preserved.

## Limits

The child exit test proves process-loss behavior, not physical power-loss behavior. Durability
ordering is implemented with synced files and parent-directory ancestry on Unix; non-Unix
directory sync remains the existing platform limitation. Non-cooperating filesystem actors are
detected at the before/after hash boundaries but cannot participate in the advisory CDF mutation
guard. `graphify update .` could not run because the `graphify` executable is unavailable.
