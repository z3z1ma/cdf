Status: done
Created: 2026-07-06
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-observability-doctor-status-sql.md
Depends-On: .10x/tickets/done/2026-07-05-cli-surface.md, .10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md, .10x/tickets/done/2026-07-06-local-system-sql.md

# Implement local status freshness evaluation

## Scope

Implement the first concrete `cdf status` evaluator for local projects. The command must evaluate compiled resources with `trust_level = serving` and a `FreshnessSpec` against committed local SQLite checkpoint heads using read-only inspection.

Owns `crates/cdf-cli/**` only. Supporting helper code may be added inside the CLI crate, but this ticket must not change lower-layer checkpoint, package, destination, or project semantics.

## Acceptance criteria

- `cdf status` reports serving resources with freshness SLOs and ignores resources that are not serving or do not declare freshness.
- For a resource to be evaluable, the configured SQLite checkpoint database must already exist, contain `cdf_checkpoints`, and contain exactly one committed head row across all pipelines for the resource's `resource_id` and serialized `state_scope`.
- The implementation must not invent a pipeline default. Missing heads, missing state DB/table, or more than one matching committed head make that resource non-evaluable.
- Evaluable resources compute `age_ms = now_ms - committed_at_ms` using the committed checkpoint timestamp and the current wall clock. Negative ages caused by clock skew are reported as `0`.
- Fresh resources where `age_ms <= max_age_ms` report `fresh`.
- Stale resources where `age_ms > max_age_ms` report `stale` and make `cdf status` exit nonzero.
- If no serving freshness resources exist, `cdf status` exits 0 and reports that there are no freshness SLO resources to evaluate.
- If any serving freshness resource is non-evaluable and none are stale, `cdf status` exits 78 and reports why each resource could not be evaluated.
- JSON output includes enough structured detail to identify resource id, trust level, state scope, max age, observed committed checkpoint when evaluable, age, freshness state, and non-evaluable reason. It must not include secrets.
- Human output remains concise for scheduler/cron use.

## Evidence expectations

Record focused CLI tests for: no serving freshness resources exits 0; fresh committed head exits 0; stale serving resource exits nonzero; missing state/history is non-evaluable exit 78; ambiguous multiple-pipeline heads are non-evaluable exit 78. Record targeted formatting, clippy, and CLI test output, plus required QUALITY evidence before closure.

## Explicit exclusions

No pipeline selector or project-to-pipeline-id convention. No run orchestration, checkpoint mutation, destination probing, `inspect run`, tracing/OTLP work, package archive behavior, or lower-layer API changes unless source inspection proves a CLI-only read-only helper is impossible.

## References

- `VISION.md` Chapter 15 and Chapter 17
- `.10x/specs/project-cli-observability-security.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/tickets/2026-07-05-observability-doctor-status-sql.md`
- `.10x/evidence/2026-07-06-local-system-sql.md`

## Progress and notes

- 2026-07-06: Opened from the observability parent after `cdf sql` and local doctor probes were closed. Read-only inspection found current `status` returns 78 for any freshness resource because it lacks runtime ledger timestamp evaluation. The narrowed implementation is executable because SQLite checkpoint rows already store committed heads and `committed_at_ms`; the ticket explicitly avoids a pipeline default by evaluating only unambiguous local heads.
- 2026-07-06: Implemented CLI-local read-only freshness evaluation in `crates/cdf-cli/src/status_freshness.rs` and wired `cdf status` to report serving resources with `FreshnessSpec` only. Added CLI tests for non-serving freshness/no SLO exit 0, fresh committed head exit 0, stale head exit 1, missing state DB non-evaluable exit 78, and ambiguous multiple-pipeline heads exit 78. Verification passed: `cargo fmt --all -- --check`; `cargo test -p cdf-cli --locked --no-fail-fast` (40 unit tests, 1 integration test, 0 doctests); `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`.
- 2026-07-06: Parent review added missing checkpoint-table, human-output, future-clock-skew clamp, and elapsed-age assertions after focused mutation testing exposed gaps in `human_summary` and `age_ms` test pressure. Final focused verification passed with 43 CLI unit tests plus 1 integration test, package nextest, clippy, fmt, final source scanners, and a focused mutation rerun catching all 14 `human_summary|age_ms` mutants. Evidence: `.10x/evidence/2026-07-06-status-freshness-local-ledger.md`. Review: `.10x/reviews/2026-07-06-status-freshness-local-ledger-review.md`.

## Blockers

None.
