Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md
Depends-On: .10x/tickets/done/2026-07-11-p3-d2-duckdb-arrow-bulk.md, .10x/tickets/done/2026-07-11-p3-d3-postgres-binary-copy.md, .10x/tickets/done/2026-07-11-p3-d4-parquet-streaming-writer.md

# P3 D5: destination bulk-path conformance and envelope matrix

## Scope

Run every first-party path and forced fallback through schema/type/disposition/staging/crash/receipt/jobs/memory laws, publish measured sheet evidence, and prove a mock fourth destination inherits the full generic matrix.

## Acceptance criteria

- Every declared path is truthful and measured; unavailable/ineligible cells are explicit.
- All P3 destination envelope rows are green on named hosts.
- Runtime/conformance contains no destination-name/path-id branch.
- Docs/inspect/doctor render descriptors and degradation from registry/sheet data.

## Evidence expectations

Generated matrix, host reports/profiles, full type/disposition/crash/replay suite, architecture/build graph gates, and adversarial destination-extension review.

## Explicit exclusions

No new destination implementation.

## Blockers

Depends on D2-D4.

## References

- `.10x/decisions/schema-planned-destination-bulk-paths.md`
- `.10x/specs/destination-bulk-path-runtime.md`

## Progress and notes

- 2026-07-11: Every first-party runtime descriptor now publishes a stable measured evidence version (`p3-d2/d3/d4-2026-07-11-v1`) at both selected-path and path-descriptor levels. A single catalog-driven conformance law inspects every enrolled driver and rejects missing paths, missing measurement authority, or correction/provenance capability drift without destination-name branching. Current DuckDB, PostgreSQL, and Parquet suites plus strict all-target Clippy pass. D5 remains open for the fourth-driver full law, generated unavailable/ineligible cells, registry-rendered docs/doctor assertions, and consolidated host envelope.
