Status: open
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md
Depends-On: .10x/tickets/done/2026-07-18-p0-external-partition-authority.md, .10x/tickets/done/2026-07-18-p0-typed-compiled-source-identities.md, .10x/tickets/done/2026-07-18-p0-source-io-accounting-separation.md

# P0: mandatory product smoke matrix gate

## Scope

Turn the post-tranche product smoke matrix into one reproducible required gate with local/recorded fast coverage and explicit live FQ12/network evidence.

## Acceptance Criteria

- Local Parquet to DuckDB passes.
- HTTPS TLC to DuckDB passes.
- Local multi-file manifest followed by unchanged no-op passes.
- FQ12 Iceberg `gold.dim_date` to DuckDB passes.
- Package verification and replay pass.
- Preview/run parity passes.
- Parquet destination passes.
- The gate is documented in `QUALITY.md` at the correct tier and is required before core-tranche closure.

## Assumptions

- User-ratified: this product matrix is mandatory stabilization evidence, not end-of-program polish.

## Journal

Pending activation.

## Blockers

Depends on the structural stabilization children.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
