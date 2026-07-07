Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/2026-07-07-cli-run-general-runtime.md
Verdict: concerns

# CLI run general runtime review

## Target

Partial implementation for `.10x/tickets/2026-07-07-cli-run-general-runtime.md`, covering `cdf run` routing through `cdf_project::run_project` for local file resources into DuckDB, SQL resource dependency construction, expanded run JSON, and fail-closed unsupported CLI combinations.

## Findings

- Significant: The ticket acceptance criteria are not fully satisfied. REST CLI run success still lacks a production `HttpTransport`, Postgres destination success lacks ratified explicit existing-table and merge-dedup policy configuration, and filesystem Parquet destination success lacks ratified CLI URI spelling. The ticket correctly remains `blocked`.
- No finding: The implemented DuckDB/local-file path now crosses the general runtime and preserves the receipt-gated commit behavior. Focused tests assert package, destination, checkpoint, receipt, and run-ledger JSON, and the full workspace nextest run passed.
- No finding: The unsupported REST, Postgres destination, and Parquet destination paths fail before package, destination, or checkpoint writes. The Postgres destination path also does not resolve a destination secret before the explicit-policy blocker.

## Verdict

Concerns raised. The partial implementation is acceptable to commit as progress, but the ticket must not close until the three recorded blockers are ratified or implemented.

## Residual risk

No live CLI-level Postgres destination success path exists yet; lower-layer `cdf-project` and destination live tests cover the capability beneath the CLI. SQL source wiring through the CLI is covered for secret handling and preflight failure, while live SQL source success remains covered at the `cdf-project` layer.
