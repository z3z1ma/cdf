Status: open
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/specs/types-contracts-normalization.md

# P2 WS-C source identity and automatic normalization

## Scope

Wire canonical resource ids, `namecase-v1`, destination identifier rules, and automatic `cdf:source_name` metadata into the live plan/run path.

Split executable child tickets before code for resource-id migration/validation, normalizer plan integration, destination-sheet joins, collision diagnostics, and schema/package normalizer evidence.

## Acceptance criteria

- New compiled resource ids are `<source>.<resource>` and mapping patterns that match zero compiled ids fail validation with useful diagnostics.
- Destination identifiers are derived automatically from source names at plan time using `namecase-v1` plus destination sheet rules.
- `cdf:source_name` metadata is populated automatically.
- `source_name` declarations are override-only, not required for every renamed field.
- Post-normalization collisions fail plan time with rename hints.
- Schema snapshots and packages record the normalizer version.

## Evidence expectations

Plan/run tests across DuckDB, Parquet, and Postgres identifier rules, collision tests, package metadata evidence, `cdf inspect resources` output snapshots, and conformance coverage for case-folding destinations.

## Explicit exclusions

This ticket does not change the normalizer algorithm itself unless a bug is found and separately scoped.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-08: Split first executable child `.10x/tickets/2026-07-08-p2-ws-c1-declarative-schema-normalization.md` for source-name defaults and `namecase-v1` normalization in declarative compiled schemas. Destination-specific sheet rules and package normalizer evidence remain later children.
- 2026-07-09: Closed first executable child `.10x/tickets/done/2026-07-08-p2-ws-c1-declarative-schema-normalization.md`. Direct declarative compiled-schema source-name defaults, `VendorID` normalization, explicit `source_name` preservation, and collision diagnostics are covered. Destination-specific sheet rules, schema/package normalizer evidence, and broader live plan/run coverage remain open WS-C work.

## Blockers

None.
