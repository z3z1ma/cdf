Status: done
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
- 2026-07-06: CLI/dlt/crate-split quality refreshed the reusable database at `target/quality/codeql-db-rust` only after source files were newer than the existing DB. Analysis still produced 0 non-diagnostic findings, but extractor metrics were 113 Rust files scanned, 80 extracted with errors, and 33 without errors. This reinforces the need to improve or explicitly accept the local Rust extractor coverage limit.
- 2026-07-06: Worker slice added `tools/codeql-rust-quality.sh`, a stale-aware local wrapper that keeps `target/quality/codeql-db-rust`, writes the generated CodeQL config with `target/**` and `reports/**` ignored, recreates only when database metadata is missing, source/manifests/lockfile content changes, input fingerprint is missing, or CodeQL version changes, then analyzes with `--rerun`. Evidence recorded in `.10x/evidence/2026-07-06-codeql-rust-extractor-coverage.md`; review recorded in `.10x/reviews/2026-07-06-codeql-rust-extractor-coverage-review.md`.
- 2026-07-06: Current refreshed metrics are 114 Rust files scanned, 81 with warnings, 33 without warnings, 1149 extraction warnings, 0 extraction errors, and 0 SARIF findings. The remaining warnings are ordinary macro expansion failures plus a CodeQL extractor-side `cargo metadata --lockfile-path` compatibility warning under local Cargo 1.96.1. No minimal safe Firn source change was identified that would materially improve extraction without rewriting normal Rust idioms.
- 2026-07-06: Parent integration hardened the wrapper from mtime staleness to content-fingerprint staleness, validated `bash -n tools/codeql-rust-quality.sh`, and validated `git diff --check -- . ':(exclude).gitignore'`. Closed with evidence and review.

## Blockers

None.
