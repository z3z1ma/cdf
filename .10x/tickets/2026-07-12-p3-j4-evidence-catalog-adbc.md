Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Depends-On: .10x/tickets/2026-07-12-p3-j1-evidence-statistics-pruning.md, .10x/tickets/2026-07-05-observability-doctor-status-sql.md

# P3 J4: evidence catalog and ADBC exposure

## Scope

Implement read-only DataFusion catalog/schema/table providers over loads, checkpoints, receipts, packages, quarantine, lineage, and resource-at-checkpoint views; add bounded table functions such as `cdf_package`; expose the catalog through `cdf sql` and a standard ADBC-compatible query surface.

## Acceptance criteria

- Providers stream budgeted batches and prune package archives through J1 statistics.
- Evidence generation/content identity, schema evolution, time travel, corruption, and missing artifacts are explicit.
- Redaction and authorization match CLI artifact access; query clients receive no mutation authority.
- DataFusion Python/notebook and ADBC clients can mount the same catalog without copying whole histories.
- Large-history queries satisfy the configured memory ceiling.

## Evidence expectations

Catalog/table-function integration tests, large-history RSS, corruption/redaction/adversarial authorization cases, Python/ADBC smoke tests, query/pruning benchmarks, and review.

## Explicit exclusions

No evidence mutation API, server daemon, destination commit, or checkpoint write through SQL/ADBC.

## Blockers

J1 and the existing observability store shape.

