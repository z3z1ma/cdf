Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md, .10x/knowledge/quality-gate-execution.md

# CodeQL Rust extractor coverage

## What was observed

The reusable CodeQL Rust database remains `target/quality/codeql-db-rust`. Before this slice, it was stale for the current workspace: `target/quality/codeql-db-rust/codeql-database.yml` was from `2026-07-06T04:03:48Z`, while `Cargo.lock` and multiple Rust source files were newer. A direct stale check found 9 source, manifest, or lockfile inputs newer than the database metadata, including the local system SQL files.

Added `tools/codeql-rust-quality.sh`, a small local wrapper that:

- Keeps the database at `target/quality/codeql-db-rust`.
- Regenerates `target/quality/codeql-rust-config.yml` with `paths-ignore` for `target/**` and `reports/**`.
- Recreates the database only when metadata is missing, the CodeQL CLI version differs from the database metadata, the stored input fingerprint is missing, or Rust source/manifests/lockfile content differs from the fingerprint.
- Uses `CARGO_TARGET_DIR=target/codeql-cargo-target` for the CodeQL build command.
- Runs `codeql database analyze` with `--rerun` and prints the extraction summary when `jq` is available.

The first wrapper run refreshed the stale database in place. The wrapper initially misreported the reason as an unknown CodeQL version because of a version-parsing bug, but the stale source/lockfile state had already been established before recreation. The parser was fixed, and a second wrapper run proved the refreshed database was reused without another `codeql database create`.

Parent review later found that mtime-only staleness would still recreate the database after content-preserving touches. The wrapper was tightened to store and compare a content fingerprint at `target/quality/codeql-db-rust/firn-codeql-inputs.sha256` after database creation. Because another worker had active uncommitted Rust edits by then, the parent did not bootstrap a fingerprint for the already-created database; the next wrapper refresh over the final source tree will write it.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

```text
codeql --version
codeql resolve languages --format=json
cargo --version --verbose
rustc --version --verbose
find Cargo.toml Cargo.lock crates -type f \( -name '*.rs' -o -name 'Cargo.toml' -o -name 'Cargo.lock' \) -newer target/quality/codeql-db-rust/codeql-database.yml | wc -l
jq extraction-metric queries against target/quality/reports/codeql-rust-current-batch.sarif
bash -n tools/codeql-rust-quality.sh
tools/codeql-rust-quality.sh
bash -n tools/codeql-rust-quality.sh && tools/codeql-rust-quality.sh
jq extraction-metric queries against target/quality/reports/codeql-rust-current.sarif
bash -n tools/codeql-rust-quality.sh
git diff --check -- . ':(exclude).gitignore'
```

Tool versions:

- CodeQL CLI: 2.25.6.
- CodeQL Rust queries: `codeql/rust-queries` 0.1.36 from `/opt/homebrew/share/codeql-cli-v2.25.6/codeql/qlpacks/codeql/rust-queries/0.1.36/`.
- Cargo: 1.96.1 (`356927216`, 2026-06-26).
- Rustc: 1.96.1 (`31fca3adb`, 2026-06-26).

Current database metadata:

- `target/quality/codeql-db-rust/codeql-database.yml` records `primaryLanguage: rust`.
- `creationMetadata.cliVersion: 2.25.6`.
- `creationMetadata.creationTime: 2026-07-06T06:24:10.091564Z`.
- `baselineLinesOfCode: 20833`.
- After refresh and before concurrent product edits, 0 Rust source, manifest, or lockfile inputs were newer than the database metadata.

## Results

Before refresh, the previous current-batch SARIF had:

- SARIF results: 0.
- Extraction errors: 0.
- Extraction warnings: 1106.
- Files extracted: 113 total, 80 with warnings, 33 without warnings.
- Clean-file percentage: 29%.
- Macro calls: 37 resolved, 1143 total, 1106 unresolved.

After refreshing the stale database and reusing it on a second wrapper run:

- SARIF results: 0.
- Extraction errors: 0.
- Extraction warnings: 1149.
- Files extracted: 114 total, 81 with warnings, 33 without warnings.
- Clean-file percentage: 28%.
- Macro calls: 37 resolved, 1186 total, 1149 unresolved.

Top macro-expansion warning families in `target/quality/reports/codeql-rust-current.sarif`:

- `format`: 403.
- `assert_eq`: 317.
- `assert`: 184.
- `vec`: 143.
- `params`: 35.
- `concat`: 18.
- `json`: 16.
- `matches`: 11.
- `write`: 8.

Top files by extraction warning count:

- `crates/firn-state-sqlite/src/tests.rs`: 77.
- `crates/firn-python/src/tests.rs`: 73.
- `crates/firn-cli/src/tests.rs`: 65.
- `crates/firn-dest-postgres/src/tests.rs`: 56.
- `crates/firn-engine/src/tests.rs`: 54.
- `crates/firn-cli/src/commands.rs`: 53.
- `crates/firn-dest-duckdb/src/tests.rs`: 46.
- `crates/firn-project/src/tests.rs`: 45.
- `crates/firn-contract/src/tests.rs`: 43.
- `crates/firn-package/src/tests.rs`: 40.

The fresh database creation log also shows two extractor-side `cargo metadata` warnings before macro warnings:

```text
WARN `cargo metadata` failed and returning succeeded result with `--no-deps` error=`cargo metadata` exited with an error: error: unexpected argument '--lockfile-path' found
```

The stack trace is inside `ra_ap_project_model::cargo_workspace::FetchMetadata::exec`, invoked by the CodeQL Rust extractor. The local `cargo metadata --help` output does not list `--lockfile-path`, only `--locked`, `--offline`, and `--frozen`.

## What this supports or challenges

This supports keeping CodeQL as a useful local security query gate for Firn because the refreshed database analyzes current source from the CodeQL slice and still produces 0 SARIF findings and 0 extraction errors. It also supports the workflow change: future local runs can use `tools/codeql-rust-quality.sh` to avoid recreating `target/quality/codeql-db-rust` unless source/dependency input content or CodeQL version makes it stale.

This challenges treating the local Rust extractor metrics as a repo-code quality failure. The remaining diagnostics are not generated-artifact noise and are not the earlier `include!` issue. They are dominated by ordinary Rust and third-party macros (`format!`, `assert!`, `assert_eq!`, `vec!`, `matches!`, `serde_json::json!`, `rusqlite::params!`) plus a CodeQL extractor/Cargo metadata compatibility warning involving `--lockfile-path`. The current evidence does not identify a minimal Firn source change that would materially improve extraction without rewriting normal Rust idioms or weakening product code.

## Limits

The metrics did not materially improve after refreshing the stale database; they changed because one current Rust file was added to the database. The durable local limit is that CodeQL CLI 2.25.6 with local Cargo/Rust 1.96.1 reports unresolved macro diagnostics for normal Firn source while still completing analysis with 0 extraction errors and 0 SARIF findings.

The parent did not rerun full CodeQL analysis after the fingerprint hardening because the DuckDB drift worker had active Rust edits in progress. Running the wrapper at that point would either analyze a database that intentionally predates those edits or refresh the expensive database for an unreviewed intermediate tree. The final verification for the next product-code slice should run the wrapper once the source tree is stable.

Tool success does not prove absence of vulnerabilities. CodeQL should remain paired with Cargo checks, tests, Clippy, Semgrep, dependency scanners, secret scanning, and direct source review.
