Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Depends-On: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md

# P3 J6: DataFusion selective-adoption audit

## Scope

Measure and differentially audit three bounded opportunities: CDF `SchemaCoercionPlan` versus DataFusion physical-expression schema adaptation; an FX1-hosted DataFusion `FileFormat` adapter for exotic non-primary formats; and selected DataFusion aggregate/function kernels such as approximate distinct or doctor reconciliation joins. Open implementation children only for measured wins that preserve the identity boundary.

## Acceptance criteria

- Audit classifies semantic overlap, gaps, unsound differences, copy/memory cost, and throughput.
- Primary Parquet/Arrow/CSV/JSON codecs, native reconciliation, validation, dedup, and statistics remain authoritative.
- Any optional `FileFormat` adapter satisfies ordinary FX1 registry/accounting/attestation/cancellation laws without generic branches.
- Any adopted kernel has native-oracle differential tests and before/after evidence.
- Rejected candidates receive durable no-action rationale; accepted candidates receive separate bounded tickets.

## Evidence expectations

Semantic matrix, generated-array differential tests, format mock, dependency/build-graph impact, benchmark report, and adversarial review.

## Explicit exclusions

This audit cannot directly replace a primary codec or identity-bearing kernel.

## Blockers

FX1 remains open; V2's final validation graph seam is complete.
