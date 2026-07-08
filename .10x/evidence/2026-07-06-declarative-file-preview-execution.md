Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-06-declarative-file-preview-execution.md, .10x/tickets/done/2026-07-05-cli-surface.md, .10x/tickets/2026-07-05-implement-cdf-system.md

# Declarative file preview execution evidence

## What was observed

`cdf preview` now opens the first supported declarative runtime slice: `kind = "files"` resources that resolve to exactly one local file under the declared file source root. The runtime path compiles project-relative file source roots under the project root, validates the requested partition against the compiled file plan before opening, resolves the glob, rejects zero and multiple matches, and delegates CSV, JSON, NDJSON, and Parquet reads to `cdf-formats::FileResource`.

Preview output preserves the no-write contract: tests assert no package root, checkpoint state database, or destination DuckDB file is created. REST and SQL declarative resources still return explicit unsupported runtime errors. `arrow_ipc` declarative file preview remains unsupported because `cdf-formats::FileResource` does not expose an Arrow IPC source variant. Multi-file scan semantics remain intentionally out of scope.

The parent review also hardened path traversal behavior. The runtime rejects absolute globs, rejects `..`, treats missing roots and missing literal intermediate directories as zero matches, reports unreadable directory/path inspection errors as data errors, ignores symlink directories during glob traversal, and canonicalizes matched files to ensure they stay under the file source root.

## Procedure

The implementation was first delegated to worker subagent `019f3997-4246-71f1-b8ba-e809d5c1a81e` under the ticket now closed at `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`. The parent then inspected and patched the implementation, added additional edge-case coverage, ran quality gates, and recorded this evidence from parent-observed command results.

Focused behavior checks:

- `cargo test -p cdf-declarative -p cdf-project -p cdf-cli --locked --no-fail-fast`: passed.
- `cargo nextest run -p cdf-declarative -p cdf-project -p cdf-cli --locked`: passed, 92 tests.
- `cargo clippy -p cdf-declarative -p cdf-project -p cdf-cli --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo metadata --locked --format-version 1 > /tmp/cdf-metadata-preview.json`: passed.
- `git diff --check -- . ':(exclude).gitignore'`: passed.

Security, supply-chain, and quality gates required because `Cargo.toml` and `Cargo.lock` changed:

- `cargo deny check`: passed. Duplicate-version warnings remain, but advisories, bans, licenses, and sources were ok.
- `cargo audit`: passed, scanning 429 dependencies against 1158 advisories.
- `cargo vet --locked`: passed with the existing exemption backlog.
- `osv-scanner --lockfile Cargo.lock`: passed with no issues found.
- `cargo machete`: passed with no unused dependencies.
- `cargo +nightly udeps -p cdf-declarative -p cdf-project -p cdf-cli --all-targets --locked`: passed.
- `semgrep scan --config p/rust --error --no-git-ignore crates/cdf-declarative crates/cdf-project crates/cdf-cli`: passed with 0 findings.
- `gitleaks detect` passed for the relevant slices: `crates`, `.10x`, `Cargo.lock`, `tools`, and `supply-chain`.
- Direct first-party unsafe/FFI scan with `rg -n "\bunsafe\b|extern \"|raw pointer|\*const|\*mut|MaybeUninit|transmute|impl Send|impl Sync" crates/cdf-declarative crates/cdf-project crates/cdf-cli -S`: returned no matches.
- `tools/codeql-rust-quality.sh`: passed using the reusable database at `target/quality/codeql-db-rust`. Because source and lockfile content changed, the wrapper refreshed the existing database in place. CodeQL scanned 147 of 147 Rust files, reported extraction errors as 0, and completed all 36 Rust security/diagnostic/summary queries. The Rust extractor still emitted 2234 macro-expansion warnings and the known inconsistent "files extracted with errors" metric; this is the previously recorded local extractor limitation, not a failing gate.

Gradient and mutation checks:

- `rust-code-analysis-cli -m -O json -p crates/cdf-declarative/src/file_runtime.rs > target/quality/rust-code-analysis-declarative-file-runtime/metrics.json`: completed. Largest functions after refactor were `glob_component_matches` cognitive 11/cyclomatic 10, `pattern_components` cognitive 5/cyclomatic 10, `collect_wildcard_matches` cognitive 5/cyclomatic 8, `validate_partition` cognitive 5/cyclomatic 6, and `collect_matches` cognitive 3/cyclomatic 3.
- `jscpd --min-lines 8 --min-tokens 80 --reporters console crates/cdf-declarative/src/file_runtime.rs`: passed with 0 clones.
- `CARGO_TARGET_DIR=target/quality/semver-target cargo semver-checks --workspace --baseline-rev HEAD`: passed after the manifest/lockfile change. Later edits were test-only and did not alter public API.
- `cargo mutants --package cdf-declarative --file crates/cdf-declarative/src/file_runtime.rs --cargo-arg=--locked --jobs 2 --test-tool cargo --timeout 900 --output target/quality/mutants-declarative-file-preview -- -p cdf-cli -p cdf-declarative -- --nocapture`: final run passed, with 66 mutants tested in 11 minutes, 61 caught, and 5 unviable.

Mutation testing materially improved the change. Earlier runs exposed missing coverage for `validate_partition` returning `Ok(())`, wildcard and `?` glob behavior, zero-match missing-path behavior, and symlink-directory traversal. Those misses were closed with direct declarative runtime and CLI preview tests before the final mutation pass.

## What this supports or challenges

This supports the child ticket acceptance criteria for the single-match declarative local file preview slice: a file resource opens through the lower-layer runtime used by `cdf preview`; NDJSON, CSV, JSON, and Parquet previews succeed; preview drains one batch and writes no package, destination, or checkpoint state; project-root-relative file roots are explicit; zero and multiple matches fail closed; and non-file resources remain unsupported below the CLI.

It also supports the parent CLI ticket's narrower preview blocker being resolved for local single-file declarative resources. It does not close the broader CLI ticket, because run/resume/replay orchestration, contract writer/runner surfaces, backfill, package GC, and several state operations still lack lower-layer APIs.

## Limits

This evidence does not prove `cdf run`, package creation, checkpoint advancement, destination commits, REST execution, SQL execution, multi-file file scans, or native Arrow/DataFusion Parquet policy. The Parquet preview path still uses the existing DuckDB-backed `cdf-formats` implementation and does not add the direct arrow-rs `parquet`/`paste` advisory path. The unrelated `.gitignore` worktree change was not inspected as part of this ticket and is intentionally left unstaged.
