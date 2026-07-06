Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md

# Improve local CodeQL Rust extractor coverage

## Scope

Investigate and improve the local CodeQL Rust extractor setup so `codeql database analyze` reports high-confidence extraction metrics for the Firn Rust workspace. Owns local quality-tooling configuration and evidence only.

## Acceptance criteria

- The reusable CodeQL database path remains `target/quality/codeql-db-rust`.
- CodeQL database creation excludes generated build/report artifacts without requiring repeated full rebuilds when source/dependencies are unchanged.
- The root cause of generic `macro expansion failed` diagnostics is identified.
- Either CodeQL extraction metrics improve materially, or a durable documented limit explains why current CodeQL Rust extractor behavior is acceptable for local quality gates.
- Any required repository configuration is explicitly justified and does not weaken other quality checks.

## Evidence expectations

Record CodeQL command lines, extractor/tool versions, diagnostic counts before and after, and any configuration changes.

## Explicit exclusions

No product behavior changes. No suppressing CodeQL findings. No generated baselines. No changes to dependency policy, CI, or release gates unless separately ratified.

## Progress and notes

- 2026-07-06: Opened after final quality evidence for the project/Python/DuckDB/Postgres batch showed 0 CodeQL SARIF findings but poor extractor metrics: 41 Rust files extracted with errors and 9 without errors, mostly generic macro expansion diagnostics for standard and third-party macros. The earlier `include!`-specific failures were fixed by `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`.

## Blockers

None.
