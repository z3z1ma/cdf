Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-package-archive-persistence-cli.md, .10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md

# Package archive persistence and CLI evidence

## What was observed

`firn-package` now persists the package Parquet archive contract from `.10x/specs/package-lifecycle-determinism.md`:

- `persist_package_parquet_archive` writes sidecars under `archive/parquet/data/<segment_id>.parquet`, writes canonical `archive/parquet/fidelity.json`, and records top-level `archives.parquet` metadata in `manifest.json`.
- `verify_package` validates canonical package identity first, then validates present archive metadata, sidecars, source metadata, orphan sidecars, and the fidelity report. `verify_package_identity` remains available for canonical-only repair flows.
- Archive writes are status-gated, leave canonical IPC replay/read paths unchanged, use operation temp paths under `archive/.tmp/`, and require `--force` to replace corrupted or mismatched final archive state.
- `firn package archive <DIR> [--format parquet] [--force] [--json]` parses through the CLI, appears in help, rejects unsupported formats, prints human output, and emits structured JSON.
- The slice did not add a direct `parquet` or `paste` dependency path.

## Procedure

Focused implementation and behavior checks:

- `cargo fmt --all -- --check` passed.
- `git diff --check -- . ':(exclude).gitignore'` passed.
- `cargo check -p firn-package -p firn-cli --all-targets --locked` passed.
- `cargo test -p firn-package --locked --no-fail-fast` passed with 22 package tests.
- `cargo test -p firn-cli package_archive --locked --no-fail-fast` passed.
- `cargo test -p firn-cli help_lists_required_command_surface --locked --no-fail-fast` passed.
- `cargo test -p firn-cli package_verify_uses_lower_package_reader --locked --no-fail-fast` passed.
- `cargo clippy -p firn-package -p firn-cli --all-targets --locked -- -D warnings` passed.
- `cargo nextest run -p firn-package -p firn-cli --locked` passed with 69 tests run and 69 passed.

Quality and supply-chain checks:

- `cargo deny check` passed. The command still reports duplicate-version warnings before the final advisory/bans/licenses/sources OK result.
- `cargo audit` passed after scanning 429 dependencies.
- `osv-scanner scan source -r --format json --output-file target/quality/osv-package-archive-persistence.json .` passed with no vulnerabilities reported.
- `cargo vet --locked` passed with the current exemption backlog.
- `gitleaks git --redact --report-format json --report-path target/quality/gitleaks-package-archive-git.json .` passed with no leaks.
- A source-only `gitleaks dir` run over a temporary mirror of `git ls-files` passed with no leaks and wrote `target/quality/gitleaks-package-archive-source.json`.
- `semgrep scan --config p/rust --error --json --output target/quality/semgrep-package-archive-rust.json crates/firn-package crates/firn-cli` passed with 0 findings.
- `rg -n '^name = "(parquet|paste)"' Cargo.lock crates/firn-package/Cargo.toml crates/firn-cli/Cargo.toml crates/firn-dest-parquet/Cargo.toml` found no direct lockfile/package entries for `parquet` or `paste`.
- `rg -n '\bunsafe\b|extern "|raw pointer|\*const|\*mut|unsafe impl (Send|Sync)' crates/firn-package crates/firn-cli` found no unsafe Rust in the changed crates.
- `cargo +nightly udeps -p firn-package -p firn-cli --all-targets --locked` passed.
- `cargo machete --with-metadata` passed.
- `tools/codeql-rust-quality.sh` reused `target/quality/codeql-db-rust` when the content fingerprint was current and refreshed it only after Rust source changes changed the fingerprint. `target/quality/reports/codeql-rust-current.sarif` contained 0 CodeQL findings.

Mutation testing:

- Initial `cargo mutants --package firn-package --file crates/firn-package/src/archive.rs --jobs 4 --timeout 300 --output target/quality/mutants-package-archive-persistence --test-tool cargo --cargo-arg --locked` found 19 missed mutants.
- After adding focused verification tests, the final run found 87 mutants total: 67 caught, 7 missed, and 13 unviable.
- Remaining missed mutants were limited to low-level platform/error-injection guards: temporary sidecar self-check boolean tightening, `AlreadyExists` handling while creating an operation temp directory, stale temp `NotFound` cleanup, and OS-string composition in synthetic missing-file errors.

## What this supports or challenges

This evidence supports the acceptance criteria for persisted archive layout, manifest archive metadata, non-mutation of canonical package identity/status/replay, archive verification, status gates, rerun and force behavior, CLI parsing/help/output, and preservation of the current supply-chain constraint.

The mutation survivors challenge total confidence in rare platform/error-injection branches, but not the ratified product contract covered by focused tests and end-to-end package/CLI behavior.

## Limits

At this evidence point, the archive implementation kept the DuckDB-backed Parquet workaround required by the active no-advisory-ignore policy. The architectural question of replacing it with native Arrow/DataFusion Parquet, and potentially accepting `RUSTSEC-2024-0436`, was owned by `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md` and later ratified by `.10x/decisions/native-arrow-datafusion-parquet-policy.md`.
