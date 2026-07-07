Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-cli-run-general-runtime.md, .10x/tickets/done/2026-07-07-cli-replay-package-spine.md, .10x/decisions/destination-introspection-package-and-cli-policy.md, .10x/specs/project-cli-observability-security.md, .10x/specs/run-orchestration-ledger.md

# CLI filesystem Parquet run and replay

## What was observed

`cdf run` now accepts environment destinations spelled `parquet://<root>` and routes supported local file resources through `cdf_project::run_project` with `ProjectRunDestination::ParquetFilesystem`.

`cdf replay package <pkg> --to parquet://<root>` now routes package artifacts through `cdf_project::replay_parquet_package_from_artifacts`, records a `replay_recorded` run-ledger event, commits checkpoint state, appends a package receipt, and reports Parquet destination root/target/receipt/checkpoint/package status in JSON.

Malformed Parquet URI roots such as `parquet://` and nested non-filesystem roots such as `parquet://s3://bucket` fail closed before package, destination, or checkpoint mutation.

The canonical receipt destination id remains the destination crate's `parquet_object_store`; the CLI destination kind remains `parquet` because that is the user-facing URI scheme.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

- `cargo test -p cdf-cli run_parquet --locked`: passed, 2 tests.
- `cargo test -p cdf-cli replay_package_parquet --locked`: passed, 2 tests.
- `cargo test -p cdf-cli --locked`: passed, 79 library tests, 1 integration test, and 0 doc tests.
- `cargo clippy -p cdf-cli -p cdf-engine --all-targets --locked -- -D warnings`: passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- `cargo check --workspace --all-targets --all-features --locked`: passed.
- `cargo check --workspace --all-targets --no-default-features --locked`: passed.
- `cargo nextest run --workspace --locked --no-fail-fast`: passed, 418 tests.
- `cargo hack check --workspace --all-targets --feature-powerset --locked`: passed.
- `cargo deny check`: passed; duplicate Arrow 58/59 warnings remain tracked by `.10x/tickets/2026-07-07-duckdb-arrow58-transitive-residual.md`.
- `cargo audit`: passed with the already-ratified `paste` / `RUSTSEC-2024-0436` allowed warning.
- `cargo vet --locked`: passed, `Vetting Succeeded (393 exempted)`.
- `osv-scanner --lockfile Cargo.lock`: exited nonzero only for the already-ratified `paste` / `RUSTSEC-2024-0436` finding.
- `cargo machete`: passed, no unused dependencies.
- `cargo semver-checks --workspace --baseline-rev HEAD`: passed for all workspace crates.
- `semgrep scan --config auto --error --quiet`: passed.
- `tools/codeql-rust-quality.sh`: passed using `target/quality/codeql-db-rust`; SARIF result count was `0`.
- `git diff --check -- . ':(exclude).gitignore'`: passed.

## What this supports

This supports the filesystem Parquet portions of:

- `.10x/tickets/done/2026-07-07-cli-run-general-runtime.md`
- `.10x/tickets/done/2026-07-07-cli-replay-package-spine.md`

It proves the ratified `parquet://<root>` CLI spelling is now wired for run and replay, with no source contact during package replay and no mutation for malformed Parquet destination URIs.

## Limits

At recording time, the CLI run ticket remained open because REST still lacked a production `HttpTransport` adapter and Postgres still needed project destination-policy parsing. The CLI replay ticket also remained open at recording time because Postgres replay still required `--target` and `--merge-dedup` parser/wiring.

`gitleaks detect --no-banner --redact` over full repository history reported two pre-existing historical leaks. This does not prove a new staged leak; staged-only `gitleaks protect --staged --no-banner` remains the commit gate.
