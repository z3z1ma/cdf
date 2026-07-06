Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md
Verdict: pass

# Doctor Secrets and DuckDB ICU Health Review

## Target

Review of the scoped doctor hardening change in `crates/firn-cli/src/commands.rs`, `crates/firn-cli/src/tests.rs`, `crates/firn-cli/tests/doctor_env.rs`, and `.10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md`.

## Assumptions tested

- Project/environment doctor details are structured and cover both absent and present lockfiles.
- Secret success output contains only references and counts, not resolved environment, file, declarative auth-token, or declarative SQL connection values.
- Secret failure output fails the command and does not leak already resolved secrets, missing-provider values, file contents, or unrelated process environment values.
- DuckDB ICU reporting is read-only for absent databases and runs the real probe for existing databases without assuming a particular local ICU installation state.
- The change does not widen into status freshness, inspect-run, new providers, destination writes, or supply-chain policy.
- Tests avoid global environment mutation and new unsafe code.

## Findings

No blocking findings.

Resolved during parent review:

- The initial worker test shape mutated process environment. It was replaced with project-local file-backed secret fixtures and missing env references only, and Semgrep now reports 0 findings.
- The initial coverage did not prove `lockfile_present == true` or later-failure redaction after successful secret resolution. Focused tests now cover both cases.
- Parent added `crates/firn-cli/tests/doctor_env.rs` to exercise resolved env secrets through a child process with `Command::env`, avoiding global process environment mutation.
- Semgrep flagged the first integration-test fixture root as an insecure `std::env::temp_dir()` use. The fixture now uses `target/quality/test-projects`, and explicit Semgrep for the new test reports 0 findings.

## Verdict

Pass. The diff matches the ticket and active spec, acceptance criteria map to recorded evidence, scanner counts are clean for this slice, and the remaining supply-chain blockers are pre-existing policy owners rather than doctor implementation defects.

## Residual risk

Full `cargo deny check` still fails at unratified license policy and `cargo vet` still lacks an initialized `supply-chain/` store; both remain owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`. DuckDB ICU availability remains local-environment dependent by design.
