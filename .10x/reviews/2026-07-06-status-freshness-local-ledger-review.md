Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-status-freshness-local-ledger.md
Verdict: pass

# Status freshness local ledger review

## Target

Review of the `cdf status` local freshness evaluator implemented under `crates/cdf-cli/` for `.10x/tickets/done/2026-07-06-status-freshness-local-ledger.md`.

## Findings

No blocking findings.

## Assumptions tested

- The implementation filters to serving resources with declared freshness and ignores governed/non-serving resources without creating a state database.
- The SQLite state store is opened read-only for status evaluation.
- Missing database, missing `cdf_checkpoints`, missing committed head, and multiple matching pipeline heads are reported as non-evaluable rather than guessed.
- No pipeline default or selector convention was introduced.
- Stale resources dominate the exit code; non-evaluable-only resources exit 78.
- JSON output includes resource id, trust level, state scope, max age, checkpoint identity when evaluable, age, state, and non-evaluable reason.
- Human output is concise and covered by non-JSON CLI assertions.
- Age arithmetic uses elapsed wall-clock subtraction and clamps future committed timestamps to zero.

## Residual risk

Miri cannot cover the SQLite-backed status cases on local macOS because `rusqlite` calls unsupported native FFI under Miri. This is mitigated by direct CLI tests through `SqliteCheckpointStore`, `cargo +nightly careful`, first-party unsafe-marker search, and the broader scanner suite.

CodeQL reports Rust extractor warnings caused by macro extraction limits already documented in `.10x/knowledge/quality-gate-execution.md`; it reports zero extraction errors and zero findings.

Geiger reports unsafe usage in dependencies. First-party source search found no owned unsafe blocks, unsafe impls/traits, FFI, raw pointer conversion markers, `transmute`, or `MaybeUninit`.

## Verdict

Pass. The ticket acceptance criteria are covered by focused tests, mutation-hardened assertions, final package checks, and final source-aware security scans. No follow-up is required for this slice.
