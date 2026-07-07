Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-non-file-window-close-checkpoint-semantics.md, .10x/decisions/non-file-window-close-checkpoint-semantics.md, .10x/specs/resource-authoring-planning-batches.md, .10x/specs/run-orchestration-ledger.md

# Non-file window-close checkpoint semantics implementation evidence

## What was observed

Focused project-runtime tests passed for non-file cursor window-close advancement and fail-closed source-position combinations. Parent review preserved raw per-segment source-position evidence in `StateSegment` while using the deterministic window-closed value as the checkpoint delta `output_position`.

The implementation advances ratified non-file cursor checkpoints by schema-backed cursor arithmetic:

- `int64`: max cursor minus lag as signed integer units.
- `uint64`: max cursor minus lag as unsigned integer units, checked for underflow.
- `timestamp`: max cursor micros minus lag milliseconds converted to micros, checked for overflow/underflow.
- `date32`: max cursor epoch-day value minus lag converted to whole days; non-day-aligned lag fails closed.

The implementation keeps page-token-only, mixed cursor/page-token, divergent source-position variants, incompatible cursor fields, unsupported cursor value kinds, unordered cursors, and incompatible lag arithmetic fail-closed before checkpoint proposal/commit.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

```text
cargo fmt --all
cargo test -p cdf-project --locked window_close -- --nocapture
cargo test -p cdf-project --locked state_delta -- --nocapture
cargo clippy -p cdf-project --all-targets --locked -- -D warnings
cargo check --workspace --all-targets --locked
git diff --check -- . ':(exclude).gitignore'
cargo test -p cdf-project --locked general_project_run_rejects_rest_without_cursor_before_writes -- --nocapture
cargo test -p cdf-project --locked general_project_run_executes_deterministic_rest_resource_stream -- --nocapture
cargo test -p cdf-project --locked -- --nocapture
cargo nextest run -p cdf-project --locked
cargo hack check -p cdf-project --all-targets --each-feature --locked
cargo deny check
cargo audit
cargo vet --locked
osv-scanner scan source --lockfile Cargo.lock --format json --output-file target/quality/reports/osv-non-file-window-close.json .
semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-non-file-window-close.json crates/cdf-project/src
rg -n "unsafe|extern \"|raw pointer|Send|Sync" crates/cdf-project/src
cargo tree -p cdf-project --locked -i arrow-schema@59.0.0
gitleaks protect --staged --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-staged-non-file-window-close.json
```

Outcomes:

- `cargo fmt --all`: passed.
- `cargo test -p cdf-project --locked window_close -- --nocapture`: passed; 3 passed, 0 failed.
- `cargo test -p cdf-project --locked state_delta -- --nocapture`: passed; 7 passed, 0 failed.
- `cargo clippy -p cdf-project --all-targets --locked -- -D warnings`: passed after replacing a manual modulo divisibility check with `is_multiple_of`.
- `cargo check --workspace --all-targets --locked`: passed.
- `git diff --check -- . ':(exclude).gitignore'`: passed.
- `cargo test -p cdf-project --locked general_project_run_rejects_rest_without_cursor_before_writes -- --nocapture`: passed; 1 passed, 0 failed.
- `cargo test -p cdf-project --locked general_project_run_executes_deterministic_rest_resource_stream -- --nocapture`: passed; 1 passed, 0 failed.
- Parent rerun after preserving raw segment positions: `cargo test -p cdf-project --locked window_close -- --nocapture` passed; 3 passed, 0 failed.
- Parent rerun after preserving raw segment positions: `cargo test -p cdf-project --locked state_delta -- --nocapture` passed; 7 passed, 0 failed.
- Parent rerun: `cargo test -p cdf-project --locked -- --nocapture` passed; 61 unit tests and 0 doc tests.
- Parent rerun: `cargo nextest run -p cdf-project --locked` passed; 61 tests.
- Parent rerun: `cargo hack check -p cdf-project --all-targets --each-feature --locked` passed.
- Parent rerun: `cargo check --workspace --all-targets --locked` passed.
- Parent rerun: `cargo deny check` passed; advisory, ban, license, and source checks were ok, with non-failing duplicate-version warnings.
- Parent rerun: `cargo audit` passed with one allowed warning, `RUSTSEC-2024-0436` for `paste`, matching the active scoped exception.
- Parent rerun: `cargo vet --locked` passed, `Vetting Succeeded (420 exempted)`.
- Parent rerun: `osv-scanner scan source --lockfile Cargo.lock --format json --output-file target/quality/reports/osv-non-file-window-close.json .` exited 1 with exactly `RUSTSEC-2024-0436` for `paste`, matching the active scoped exception and no unratified advisory.
- Parent rerun: `semgrep scan --config p/rust --error --json --output target/quality/reports/semgrep-non-file-window-close.json crates/cdf-project/src` passed, 0 findings across 9 tracked files.
- Parent rerun: `rg -n "unsafe|extern \"|raw pointer|Send|Sync" crates/cdf-project/src` found no matches.
- Parent rerun: `cargo tree -p cdf-project --locked -i arrow-schema@59.0.0` passed, confirming the new direct manifest dependency resolves to the already-present Arrow 59 tuple without a lockfile change.
- `gitleaks protect --staged --redact --no-banner --report-format json --report-path target/quality/reports/gitleaks-staged-non-file-window-close.json`: passed, no leaks found.

## What this supports

This supports the ticket acceptance criteria that inexact/nonzero-lag numeric, timestamp, and date cursor positions advance with window-close semantics; exact zero-lag REST cursor behavior remains intact; compatible multi-segment cursor positions aggregate deterministically; raw per-segment source-position evidence remains available in state segments; and unsupported page-token, mixed, divergent, incompatible-field, unsupported-value, and bad-lag cases fail closed before checkpoint mutation.

## Limits

This evidence is focused on `cdf-project` runtime behavior and workspace compilation. It includes the local ephemeral Postgres tests in `cdf-project`, but does not run live external HTTP credentials, scheduler/resident streaming, arbitrary SQL execution, or CLI presentation checks, which are outside this ticket's explicit scope.

CodeQL was not rerun for this focused runtime slice. The reusable database at `target/quality/codeql-db-rust` would need a source-fingerprint refresh after these Rust edits; this evidence therefore does not claim current-tree CodeQL coverage.
