Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-07-cli-init-scaffold.md, .10x/decisions/cdf-init-local-scaffold-defaults.md

# cdf init scaffold evidence

## What was observed

`cdf init [DIR] [--name NAME] [--force]` now creates the ratified local scaffold through `cdf-project` and exposes it through a thin `cdf-cli` command wrapper.

The implemented scaffold writes only:

- `cdf.toml`
- `resources/`
- `resources/files.toml`
- `data/`

It does not create `.cdf/`, `cdf.lock`, package directories, checkpoint state, DuckDB destination files, resolved secrets, or data files. The generated project validates without manual edits. Existing scaffold paths fail closed unless `--force` is supplied; `--force` replaces scaffold-owned files while preserving unrelated files, existing data files under `data/`, existing `.cdf` state, and lockfiles.

## Procedure

Source and record changes inspected:

- `crates/cdf-project/src/scaffold.rs`
- `crates/cdf-project/src/lib.rs`
- `crates/cdf-project/src/tests.rs`
- `crates/cdf-cli/src/project_command.rs`
- `crates/cdf-cli/src/tests.rs`
- `.10x/decisions/cdf-init-local-scaffold-defaults.md`

Behavior checks:

- `cargo test -p cdf-cli init_ --locked` passed: 4 init tests.
- `cargo test -p cdf-project local_project_scaffold --locked` passed: 1 project scaffold test.
- `cargo test -p cdf-cli -p cdf-project --locked --no-fail-fast` passed: 110 `cdf-cli` tests, 1 `doctor_env` test, 74 `cdf-project` tests, and package doctests.
- `cargo test --workspace --all-targets --locked --no-fail-fast` passed.
- `cargo nextest run -p cdf-cli -p cdf-project --locked` passed: 185/185 tests.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` passed.

Compile, lint, API, and docs:

- `cargo fmt --all -- --check` passed.
- `cargo check --workspace --all-targets --locked` passed.
- `cargo check --workspace --all-targets --all-features --locked` passed.
- `cargo check --workspace --all-targets --no-default-features --locked` passed.
- `cargo hack check --workspace --all-targets --each-feature --locked` passed.
- `cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` passed.
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked` passed.
- `cargo semver-checks -p cdf-project --baseline-rev HEAD` passed: 196 checks passed, 57 skipped; no semver update required.

Quality, security, and supply chain:

- `git diff --check` passed.
- `cargo metadata --format-version=1 --locked`, `cargo tree --workspace --locked`, and duplicate tree output were recorded under `reports/ai-quality/`.
- `cargo deny check` passed; report stored at `reports/ai-quality/cargo-deny-cli-init-scaffold.txt`.
- `cargo audit --json` passed; report stored at `reports/ai-quality/cargo-audit-cli-init-scaffold.json`.
- `cargo vet` passed with the existing warning that some exemptions may be pruned; report stored at `reports/ai-quality/cargo-vet-cli-init-scaffold.txt`.
- `cargo machete --with-metadata` passed with no unused dependency candidates.
- `rust-code-analysis-cli` completed for `crates/cdf-cli/src/project_command.rs` and `crates/cdf-project/src/scaffold.rs`; JSON reports stored under `reports/ai-quality/rust-code-analysis-cli-init-scaffold-*`.
- `npx --yes jscpd --reporters json --output reports/ai-quality/jscpd-cli-init-scaffold-source --threshold 0 --min-lines 6 --min-tokens 50 crates/cdf-cli/src/project_command.rs crates/cdf-project/src/scaffold.rs` passed with 0 clones, 0 duplicated lines, and 0.0% duplication.
- `semgrep scan --config p/rust --error --json --output reports/ai-quality/semgrep-rust-cli-init-scaffold.json crates/cdf-cli/src/project_command.rs crates/cdf-project/src/scaffold.rs` passed with 0 findings.
- `gitleaks dir --no-banner --redact` passed for the touched 10x decision, init ticket, `crates/cdf-cli/src`, and `crates/cdf-project/src`; reports stored under `reports/ai-quality/gitleaks-cli-init-scaffold/`.
- `osv-scanner scan source -r . --format json --output reports/ai-quality/osv-cli-init-scaffold.json` exited non-zero only for the already-ratified `RUSTSEC-2024-0436` `paste` advisory.
- `tools/codeql-rust-quality.sh` completed successfully and wrote `target/quality/reports/codeql-rust-current.sarif`. The reusable database was refreshed because Rust source content changed. The run reported the current extractor profile: 229 Rust files scanned, 0 extraction errors, 3510 extraction warnings, and 23% files extracted without errors.
- `rg -n "\bunsafe\b|unsafe\s+impl|unsafe\s+trait|extern\s+\"C\"|from_raw|into_raw|transmute|MaybeUninit|Unpin|Send|Sync" crates/cdf-cli/src/project_command.rs crates/cdf-project/src/scaffold.rs` found no matches.
- `cargo geiger` was attempted for `cdf-cli` and `cdf-project`, including `--forbid-only`; it did not produce usable evidence because `cargo-geiger 0.13.0` failed while parsing `signal-hook-registry-1.4.8` and panicked while walking transient generated `target` output. The direct touched-source unsafe search above is the relevant unsafe evidence for this slice.

Coverage:

- `cargo llvm-cov -p cdf-cli -p cdf-project --locked --summary-only` passed.
- Package-slice total: 74.39% line coverage, 71.74% region coverage.
- `crates/cdf-project/src/scaffold.rs`: 76.51% line coverage, including success, no-overwrite, force, validation, and no-runtime-artifact behavior through CLI and project tests.

## What this supports

This supports closing `.10x/tickets/done/2026-07-07-cli-init-scaffold.md`:

- The CLI command is implemented and returns stable human/JSON output.
- The scaffolded project validates through the existing typed project validation path.
- Overwrite and `--force` semantics are covered by tests and match `.10x/decisions/cdf-init-local-scaffold-defaults.md`.
- Scaffold semantics are owned by `cdf-project`; `cdf-cli` does not embed a project-format template or bypass lower-layer validation.
- No resolved secrets, package state, checkpoint state, destination data, or hidden runtime artifacts are created by `init`.

## Limits

This evidence does not claim completion of the full CLI parent. Preview breadth, contract registry/freeze/test, state migrate/recover, backfill, package GC retention, and status freshness remain separate open owners under `.10x/tickets/2026-07-05-cli-surface.md`.

This slice does not add remote templates, credential discovery, package/state initialization, or non-local scaffold variants.

Miri, cargo-careful, fuzzing, Kani, mutation testing, benchmark regression checks, and binary-size profiling were not run because this change is ordinary safe Rust file-scaffold and CLI plumbing with no unsafe code, parser/protocol logic, concurrency primitive, critical arithmetic, or hot-path performance claim.
