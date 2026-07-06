Status: active
Created: 2026-07-06
Updated: 2026-07-06

# Quality gate execution

Quality checks SHOULD be parallelized whenever tools do not contend on the same exclusive output. Run independent Cargo checks, scanners, and report readers in parallel batches instead of serializing the entire `QUALITY.md` loop.

CodeQL database creation is expensive. Keep a reusable Rust database under ignored build output, such as `target/quality/codeql-db-rust`, and run `codeql database analyze` against the existing database when it is still valid for the current source, CodeQL version, and extractor inputs. Recreate the database only when source or dependency changes make the existing database stale, when CodeQL/extractor version changes, or when analysis indicates the database is invalid.

For Firn, prefer `tools/codeql-rust-quality.sh` for local Rust CodeQL runs. It keeps the reusable database at `target/quality/codeql-db-rust`, regenerates the generated CodeQL config with `target/**` and `reports/**` ignored, stores a content fingerprint beside the database, skips database creation when the existing database is fresh for the current CodeQL version and Rust source/manifests/lockfile content, analyzes with `--rerun`, and prints the extraction summary.

Before creating a CodeQL database from the repository root, remove or avoid generated analysis directories such as `target/semver-checks` and `target/llvm-cov-target`; otherwise CodeQL may index generated Rust files and report extraction-warning noise even when the source checks pass. If the command must run from the root so Cargo can see the workspace, keep the database under `target/quality/codeql-db-rust` and record extraction-warning limits in evidence.

With CodeQL CLI 2.25.6 and local Cargo/Rust 1.96.1, Firn's Rust extractor diagnostics are dominated by ordinary macro expansion failures for `format!`, `assert!`, `assert_eq!`, `vec!`, `matches!`, `serde_json::json!`, and `rusqlite::params!`, plus an extractor-side `cargo metadata` warning for unsupported `--lockfile-path`. Treat this as a local CodeQL Rust extractor limit, not a reason to rewrite normal product code, as long as analysis has 0 extraction errors and 0 SARIF findings and the broader quality gates pass.

`cargo geiger` can clean normal Cargo build output and may fail on dependency scan warnings even when firn-owned code has no `unsafe`. Prefer running it in an isolated `CARGO_TARGET_DIR` when possible, and always pair it with a direct source search over `crates/` for `unsafe`, FFI, raw-pointer, and `Send`/`Sync` surfaces.

Do not place generated quality reports or CodeQL databases in tracked source. Prefer ignored build output or `/tmp` for transient reports, and record summarized results in `.10x/evidence/`.
