Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-non-duckdb-package-replay-recovery.md
Verdict: pass

# Non-DuckDB package replay recovery review

## Target

Review of the implementation and evidence for `.10x/tickets/done/2026-07-07-non-duckdb-package-replay-recovery.md`.

## Findings

- No finding: Parquet replay mirrors the existing artifact replay structure and uses package-owned replay inputs instead of reopening a resource.
- No finding: Postgres replay does not invent destination policy. It requires explicit target/dedup/existing-table inputs and rejects a target mismatch before checkpoint or destination mutation.
- No finding: Postgres column derivation remains destination-owned through `cdf-dest-postgres::postgres_columns_for_schema`; project runtime does not duplicate Arrow-to-Postgres type mapping.
- Minor accepted limit: The tests simulate the finalized-package/no-receipt window by deleting package receipts and resetting the destination after a controlled package-finalized failure, rather than by a dedicated Parquet/Postgres lifecycle failpoint. This is sufficient for the ticket because the new public replay functions are the missing source-free recovery path, while exact named crash-window failpoints remain DuckDB/conformance coverage.

## Verdict

Pass. The child closes the parent audit gap without adding semantic defaults or destination introspection.

## Residual risk

CLI resume/replay still needs to route to these public replay functions. That is owned by `.10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md`.
