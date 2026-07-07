Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-cli-replay-package-spine.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/project-cli-observability-security.md, .10x/decisions/destination-introspection-package-and-cli-policy.md

# CLI Postgres Package Replay

## What was observed

`cdf replay package <pkg> --to postgres://... --target schema.table --merge-dedup fail` now routes verified package artifacts through `cdf_project::replay_postgres_package_from_artifacts`.

The CLI requires explicit Postgres replay target and merge-dedup policy, rejects unsupported merge-dedup values, verifies the explicit target against the package destination-commit target before state or destination mutation, resolves `postgres://secret://provider/key` through the project secret provider, and redacts resolved DSNs from error output.

A live CLI replay test created a package, deleted the source file and checkpoint store, replayed only from package artifacts into a disposable local Postgres schema, committed checkpoint state, appended a package receipt, recorded one terminal `replay_recorded` run-ledger event, and verified two rows in the Postgres target table.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo fmt --all`: passed after the CLI changes.
- `cargo check -p cdf-cli --offline`: passed and refreshed `Cargo.lock` after adding the `cdf-http` dependency edge and `postgres` dev-dependency to `cdf-cli`.
- `cargo test -p cdf-cli replay_package_postgres --locked`: passed, 6 focused Postgres replay tests including live local Postgres replay.
- `cargo test -p cdf-cli --locked --no-fail-fast`: passed, 84 library tests, 1 integration test, and 0 doc tests.
- `cargo test -p cdf-project postgres_artifact_replay --locked --no-fail-fast`: passed, 2 lower artifact replay tests.
- `cargo clippy -p cdf-cli -p cdf-project --all-targets --locked -- -D warnings`: passed after boxing the large Postgres destination field in the CLI replay enum.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo nextest run --workspace --locked --no-fail-fast`: passed, 423/423 tests.
- `cargo test --workspace --all-targets --locked --no-fail-fast`: passed.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast`: passed, all workspace doctest targets had 0 doctests.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings`: passed.
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked`: passed.
- `cargo hack check --workspace --all-targets --each-feature --locked`: passed across 17 workspace crates.
- `cargo deny check`: passed; duplicate Arrow 58/59 warnings remain tracked separately and advisories/bans/licenses/sources were ok.
- `cargo audit`: passed with one allowed warning, `RUSTSEC-2024-0436` for `paste`.
- `cargo vet --locked`: passed, `Vetting Succeeded (393 exempted)`.
- `osv-scanner scan source -r .`: exited 1 only for the already-ratified `RUSTSEC-2024-0436` / `paste 1.0.15` advisory.
- `cargo machete --with-metadata`: passed, no unused dependency candidates.
- `cargo semver-checks --workspace --baseline-rev HEAD`: passed for all workspace crates; no semver update required.
- `semgrep scan --config p/rust --error --quiet .`: passed.
- `tools/codeql-rust-quality.sh`: passed using reusable database path `target/quality/codeql-db-rust`. The database was refreshed because Rust source, manifest, or lockfile content changed. SARIF result count was 0; extraction errors were 0; extraction warnings were 2781, matching the known local Rust extractor macro-warning profile.
- `jq '{total: ([.runs[].results[]?] | length), by_level: ([.runs[].results[]? | (.level // "none")] | group_by(.) | map({level: .[0], count: length}))}' target/quality/reports/codeql-rust-current.sarif`: reported `total: 0`.
- `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|\*const|\*mut|Send for|Sync for" crates --glob '*.rs'`: found only literal/test strings containing the word `unsafe`; no Rust unsafe blocks, FFI, raw pointer, or unsafe impl surfaces.
- `gitleaks dir --redact --no-banner crates`: passed, no leaks found.
- `gitleaks dir --redact --no-banner .10x`: passed, no leaks found.
- `git diff --check`: passed.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-cli-replay-package-spine.md`: all currently supported replay destinations are wired from the CLI to package-artifact replay APIs, Postgres replay semantics are explicit and fail-closed, duplicate/no-op receipt reporting remains covered through DuckDB, and replay JSON includes the required package, destination, receipt, checkpoint, ledger, and package-status fields.

## Limits

`gitleaks dir --redact --no-banner .` was intentionally interrupted because it began walking generated `target/` output. The source-scoped scans passed, and the final staged `gitleaks protect` remains the commit gate.

Kani, Miri, cargo-careful, cargo-geiger, cargo-mutants, and coverage were not run for this CLI replay slice. The implementation did not add unsafe code, FFI, arithmetic/state-machine proof harnesses, or parser/validator algorithms beyond CLI argument routing already covered by focused negative tests, full workspace tests, direct unsafe search, Clippy, Semgrep, and CodeQL.
