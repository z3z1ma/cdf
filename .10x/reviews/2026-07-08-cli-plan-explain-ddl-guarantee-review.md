Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-07-cli-plan-explain-ddl-guarantee.md
Verdict: pass

# CLI plan/explain DDL and guarantee review

## Target

Review of `.10x/tickets/done/2026-07-07-cli-plan-explain-ddl-guarantee.md` and evidence `.10x/evidence/2026-07-08-cli-plan-explain-ddl-guarantee.md`.

## Findings

None blocking.

## Assumptions tested

No-write behavior is explicit, not implied. CLI tests assert `cdf plan` and `cdf explain` do not create `.cdf/packages`, `.cdf/state.db`, `.cdf/dev.duckdb`, or a Parquet root on unsupported Parquet planning. The lower `cdf-project` tests assert the DuckDB planning facade does not create the database path and Parquet planning does not create the destination root.

Destination planning is lower-layer owned. The CLI resolves the selected environment destination, calls the project planning facade, and formats the returned destination sheet, migrations, and commit plan. It does not duplicate destination-specific DDL generation.

Guarantee output is mechanically derived. The CLI compares the destination commit plan guarantee to a guarantee derived from sheet idempotency, disposition, transaction support, and merge key facts. A mismatch becomes an internal error instead of a product claim.

Unsupported combinations fail closed. The Parquet merge test exercises a sheet-unsupported disposition and asserts the command fails without destination artifacts or an `effectively_once` claim.

The `cdf-cli` architecture concern is improved rather than worsened. Destination resolution shared by scan/plan/explain and resume now lives in `destination_uri::resolve_environment_destination`, reducing the duplicated command-level concern identified by `jscpd`.

## Verdict

Pass. The ticket acceptance criteria are supported by focused no-write tests, lower-layer planner tests, workspace tests, clippy/check/fmt, duplication and complexity checks, supply-chain/security gates, CodeQL with the reusable database path, and source-only Gitleaks scans.

## Residual risk

Full-history Gitleaks still reports two historical findings outside this change and outside the current tree. That is now owned by `.10x/tickets/2026-07-08-historical-gitleaks-findings-triage.md`; it is not a blocker for this source-level CLI planning slice.

The current planning facade uses synthetic state/package inputs by design. It proves destination DDL/guarantee planning, not package materialization or destination commit behavior; those remain covered by the run/replay/session conformance slices.
