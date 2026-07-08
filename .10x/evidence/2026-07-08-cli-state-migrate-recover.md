Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-cli-state-migrate-recover.md, .10x/decisions/state-migrate-recover-package-receipt.md, .10x/tickets/done/2026-07-05-cli-surface.md

# CLI state migrate/recover evidence

## What was observed

`cdf state migrate` is implemented as a local SQLite state migration/reporting command. It initializes missing `checkpoint_store` and `run_ledger` schema components, reports before/after/target versions and action per component, and is idempotent on a second run.

`cdf state recover` is implemented as package-receipt recovery. It reads replay inputs and durable receipts from a package, fails closed on zero or ambiguous receipt selection, resolves destinations through the package replay destination resolver, verifies the supplied receipt through the destination protocol, and commits checkpoint state through the lower package recovery path. It does not write destination rows.

The CLI state command implementation was split so `crates/cdf-cli/src/state_command.rs` remains the dispatcher plus existing show/history/rewind handlers, while new state operational logic lives under `crates/cdf-cli/src/state_command/migrate.rs` and `crates/cdf-cli/src/state_command/recover.rs`.

## Procedure

Implementation and focused behavior checks:

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cargo check -p cdf-cli -p cdf-state-sqlite -p cdf-project --all-targets --locked`: passed.
- `cargo clippy -p cdf-cli -p cdf-state-sqlite -p cdf-project --all-targets --locked -- -D warnings`: passed.
- `cargo test -p cdf-state-sqlite sqlite_state_migration --locked`: 2 passed.
- `cargo test -p cdf-cli state_ --locked`: 9 passed.
- `cargo test -p cdf-cli replay_package_ --locked`: 13 passed.
- `cargo nextest run -p cdf-cli -p cdf-state-sqlite -p cdf-project --locked`: 237 passed, 0 skipped.

Quality and security checks:

- `cargo machete`: passed; no unused dependency candidates reported.
- `cargo deny check`: passed; duplicate Arrow 58/59 warning remains the ratified DuckDB residual.
- `cargo audit`: passed with the already-ratified `RUSTSEC-2024-0436` warning for `paste`.
- `cargo vet`: passed; reported "Vetting Succeeded (452 exempted)" with the existing prune warning.
- `osv-scanner scan source -r .`: exited 1 only for the already-ratified `RUSTSEC-2024-0436` `paste` advisory in `Cargo.lock`.
- `semgrep scan --config p/rust --error --json crates/cdf-cli/src crates/cdf-state-sqlite/src crates/cdf-project/src`: passed; 65 tracked files scanned, 0 findings.
- `tools/codeql-rust-quality.sh`: passed through the reusable database at `target/quality/codeql-db-rust`; the wrapper refreshed the database because Rust source changed, ran 36 Rust queries, and exited 0. CodeQL extracted 232 Rust files with 0 extraction errors and 3553 extraction warnings dominated by known macro-resolution limits documented in `.10x/knowledge/quality-gate-execution.md`.
- `gitleaks detect --no-git --source crates/cdf-cli/src --redact`: passed; no leaks found.
- `gitleaks detect --no-git --source crates/cdf-state-sqlite --redact`: passed; no leaks found.
- `gitleaks detect --no-git --source .10x --redact`: passed after evidence/review/ticket closure records were written; no leaks found.
- `gitleaks git --redact .`: exited 1 with the two known full-history findings owned by `.10x/tickets/done/2026-07-08-historical-gitleaks-findings-triage.md`.
- Direct unsafe/FFI/raw pointer scan over touched implementation files found no matches for `unsafe`, `extern "`, raw pointer forms, or `Send`/`Sync`.

Maintainability checks:

- Broad `jscpd` over `crates/cdf-cli/src` and `crates/cdf-state-sqlite/src`: 41 files, 18536 lines, 123 clone instances, 1221 duplicated lines, 6.59%. The broad clone set is dominated by older CLI test scaffolding and existing state-store symmetry.
- Targeted `jscpd` over the changed CLI state/replay/reporting files and SQLite migration/run-ledger/checkpoint support files: 9 files, 2904 lines, 6 clone instances, 99 duplicated lines, 3.41%. Remaining targeted clones are existing run-ledger/checkpoint-store structural symmetry rather than new command-body duplication.
- `rust-code-analysis-cli -m` targeted at touched Rust files: highest touched-slice new function was `recover` in `crates/cdf-cli/src/state_command/recover.rs` at cyclomatic 13, cognitive 3. Overall touched-file maxima were pre-existing `row_to_checkpoint_result` at cyclomatic 54 and `validate_event_value` at cognitive 10.

## What this supports

- `cdf state migrate` acceptance is covered by the CLI idempotency test and the committed `run-ledger-v1.sql` SQLite migration fixture test.
- `cdf state recover` acceptance is covered by focused CLI tests proving verified package-receipt recovery, no destination row writes, explicit receipt disambiguation, and fail-closed zero/ambiguous receipt behavior.
- The commit gate is preserved because `state recover` routes through `cdf_project::recover_package_from_artifacts`, which verifies replay inputs, verifies the receipt through the destination protocol, and calls `CheckpointStore::commit` or reuses only an exact already-committed head.
- CLI architecture debt did not increase for this slice: the new state operational concerns are not embedded directly in the already-busy command dispatcher.

## Limits

This evidence does not prove broad destination mirror scraping recovery; that behavior is explicitly excluded by `.10x/decisions/state-migrate-recover-package-receipt.md`.

Record-graph cleanup was validated with `rg` for the old open-ticket path after moving the ticket to `done/`; no stale references remained.

`state recover` does not reconstruct arbitrary missing run-ledger history or quarantine lineage. The CLI JSON reports those evidence limits.

Historical Gitleaks findings are triaged under `.10x/tickets/done/2026-07-08-historical-gitleaks-findings-triage.md`; source-only scans over touched implementation paths passed.

CodeQL Rust extraction has known macro-resolution warning noise under the local toolchain. This run had 0 extraction errors and no blocking security findings, but CodeQL is not treated as complete semantic coverage for macro-heavy Rust.
