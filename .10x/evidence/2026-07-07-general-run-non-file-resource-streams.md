Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md, .10x/specs/run-orchestration-ledger.md, .10x/specs/resource-authoring-planning-batches.md

# General run non-file resource streams evidence

## What was observed

The general project runtime now accepts dependency-bearing `RestResource` and `SqlResource` wrappers in addition to local-file compiled resources. REST and table-backed Postgres SQL resource streams execute through `ResourceStream`, write deterministic packages, commit DuckDB checkpoints, and record cursor source positions. Raw REST/SQL `CompiledResource` inputs still fail closed through the local-file path.

Non-file project runs are intentionally limited to exact zero-lag cursor sources in this slice. Missing cursor declarations, inexact cursor ordering, nonzero lag, missing secret providers, missing secret values, and empty SQL connection secrets all fail before transport, package, destination, or checkpoint mutation. Divergent per-segment source positions fail closed instead of inventing aggregation semantics.

## Procedure

- `cargo fmt --all -- --check`: passed after final edits.
- `git diff --check -- . ':(exclude).gitignore'`: passed.
- `cargo check --workspace --all-targets --locked`: passed after final edits.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: passed after final edits.
- `cargo test -p cdf-project -p cdf-declarative --locked --no-fail-fast`: passed after final edits; `cdf-declarative` 50 tests passed and `cdf-project` 50 tests passed, including the deterministic REST run and local Postgres table-backed SQL run.
- Earlier full workspace verification on the same production code passed: `cargo test --workspace --locked --no-fail-fast`, `cargo nextest run --workspace --locked` with 388 tests passed, `cargo hack check --workspace --all-targets --each-feature --locked`, all-features and no-default-features check/clippy, doctests, and rustdoc with warnings denied.
- Supply-chain and security checks passed or matched ratified limits: `cargo deny check` passed; `cargo vet` passed; `cargo audit` and OSV reported only ratified `RUSTSEC-2024-0436` for `paste`; Semgrep Rust and security-audit final scans over `crates/cdf-project` and `crates/cdf-declarative` reported 0 findings; source-only gitleaks final scan reported no leaks.
- Reused CodeQL database `target/quality/codeql-db-rust` was analyzed without recreating the DB and produced 0 SARIF results. Limit: the database was not recreated per instruction, so current-tree security evidence is primarily Semgrep, gitleaks source scan, compiler/lint, and tests.
- Direct first-party unsafe/FFI/raw-pointer scan over touched crates found no unsafe blocks, unsafe impls, unsafe traits, FFI, raw pointer conversions, transmute, or `MaybeUninit`; remaining matches were trait bounds such as `Send`/`Sync` and a test variable name.
- `cargo machete --with-metadata` found no unused dependencies. `cargo semver-checks --workspace --baseline-rev HEAD --all-features` reported no semver update required.
- `cargo mutants` was attempted against the production diff and then a narrowed `cdf-declarative` diff, but both scratch-build modes pulled `libduckdb-sys` native builds into each mutant worker. Both mutation runs were interrupted and are recorded as an infrastructure limit, not closure evidence.

## Supports

This supports closing the bounded non-file project-run stream ticket: exact zero-lag REST and table-backed Postgres SQL resources can now run through the general orchestrator with explicit runtime dependencies, deterministic state cursor commits, and fail-closed preflight behavior for unsupported or under-specified cases.

## Limits

This does not prove broader non-file cursor aggregation, page-token aggregation, or window-close checkpoint advancement. Those semantics were later owned by `.10x/tickets/done/2026-07-07-non-file-window-close-checkpoint-semantics.md`.
