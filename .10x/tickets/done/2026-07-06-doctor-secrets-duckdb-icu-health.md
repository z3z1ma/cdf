Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-observability-doctor-status-sql.md
Depends-On: .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/done/2026-07-05-duckdb-destination.md

# Harden doctor secrets, environment, and DuckDB ICU checks

## Scope

Make the existing `firn doctor` environment, secret-resolvability, and DuckDB ICU checks closure-grade. Owns only the relevant `crates/firn-cli/**` doctor/context/tests surface and this ticket's evidence/review records.

## Acceptance criteria

- `firn doctor --json` reports structured details for the project/environment health check: project root, selected environment name, compiled resource count, and whether a lockfile was present.
- The `secrets` check passes with structured details when all referenced secrets resolve, including count and secret references only. It MUST NOT include resolved secret values.
- The `secrets` check fails and makes `doctor` exit nonzero when an environment, file, declarative auth token, or declarative SQL connection secret is missing or uses an unavailable provider.
- Secret failure output MUST NOT leak resolved secret values, file secret contents, or unrelated process environment values in stdout, stderr, JSON details, or error messages.
- DuckDB destinations report a `duckdb_icu` check with structured details. If the database file is absent, the check MUST skip without creating the DuckDB file. If the database file exists, the check MUST run the DuckDB ICU probe and report passed or failed with safe diagnostic details without assuming a particular local ICU-extension outcome.
- Existing Python doctor and ledger/destination drift behavior remains unchanged.

## Evidence expectations

Record focused `firn-cli` tests for resolved env/file/declarative secrets, missing/unavailable secret failures, no secret-value leakage, DuckDB ICU skip without file creation, and existing-database ICU probe details. Before closure, run focused fmt/test/clippy for `firn-cli`, relevant workspace checks, security/secret scans from `QUALITY.md`, and update the observability parent with the child outcome.

## Explicit exclusions

Do not implement new secret providers, OS keychain integration, destination write behavior, status freshness, inspect run, OTLP export, or package archive. Do not change repository supply-chain policy; that remains owned by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.

## Progress and notes

- 2026-07-06: Opened from `.10x/specs/project-cli-observability-security.md` and the observability parent after closing the Python doctor child. Current source already has basic secret and DuckDB ICU checks; this child hardens their structured JSON contract, redaction tests, and read-only ICU behavior without widening into status or inspect-run work.
- 2026-07-06: Worker implementation updated `crates/firn-cli/src/commands.rs` to add structured doctor details for project/environment health, resolved secret references, destination identity, and DuckDB ICU probes. Added focused `crates/firn-cli/src/tests.rs` coverage for resolved file/declarative secrets without value leakage, missing/unavailable secret failures, missing-DuckDB ICU skip without file creation, and existing-DuckDB ICU probe details without assuming local ICU availability. Verified with `cargo fmt --all -- --check`, `cargo test -p firn-cli --locked --no-fail-fast`, and `cargo clippy -p firn-cli --all-targets --locked -- -D warnings`.
- 2026-07-06: Added parent-review gap coverage in `crates/firn-cli/src/tests.rs`: a later missing SQL secret now fails doctor without leaking already resolved destination, file, or auth token secret values, and `project_file.details.lockfile_present == true` is covered with a local minimal lockfile fixture. Re-ran `cargo fmt --all -- --check`, `cargo test -p firn-cli --locked --no-fail-fast`, and `cargo clippy -p firn-cli --all-targets --locked -- -D warnings`.
- 2026-07-06: Added `crates/firn-cli/tests/doctor_env.rs` integration coverage for resolved environment secrets by running the compiled CLI as a child process with `Command::env`, proving env/file/declarative success details without global process environment mutation.
- 2026-07-06: Parent QUALITY closure passed workspace fmt, tests, nextest, clippy, docs, feature-powerset, semver, coverage, mutation, security scanners, CodeQL with reusable DB, and direct unsafe search. Evidence recorded in `.10x/evidence/2026-07-06-doctor-secrets-duckdb-icu-health.md`; closure review recorded in `.10x/reviews/2026-07-06-doctor-secrets-duckdb-icu-health-review.md`.

## Blockers

None.
