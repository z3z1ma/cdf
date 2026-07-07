Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md
Verdict: pass

# Project format, lockfile, and secrets closure review

## Target

Implementation in `crates/cdf-project/**` for `.10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md`, plus the required verification state recorded in `.10x/evidence/2026-07-06-project-format-lockfile-secrets.md`.

## Findings

No closure-blocking findings.

The earlier workspace-load blocker was resolved by the DuckDB destination worker restoring `crates/cdf-dest-duckdb/src/lib.rs`; the earlier PyO3 advisory blocker was resolved by the Python worker moving to PyO3 0.29 through `pyo3-arrow`. Parent integration revalidation then passed formatting, integrated package tests, clippy, `cargo audit`, `cargo deny check advisories`, OSV, pyright/compileall, and `git diff --check`.

OS keychain support is explicitly unavailable in the `DefaultSecretProvider` rather than implemented. This is acceptable under the ticket's "where feasible" wording because env/file providers are implemented and tested, and the unavailable keychain path fails closed without leaking a secret value.

## Verdict

Pass. The project format, environment overlay, declarative source resolution, secret-reference validation/redaction, semantic lockfile generation, TOML round-trip, and deterministic lock diff behavior satisfy the ticket's acceptance criteria.

## Residual risk

The lockfile now includes dependency consequences from the same committed batch: project, Python, DuckDB, and Postgres destination work. Full workspace quality still needs to be rerun before committing the batch, but no project-specific closure risk remains.
