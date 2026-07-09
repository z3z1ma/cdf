Status: open
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/tickets/2026-07-08-p2-ws-a-discovery-compiler-stage.md, .10x/tickets/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md, .10x/tickets/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md

# P2 WS-H scaffolding, id model, and two-minute path

## Scope

Implement the user-facing happy path: `cdf add <id> <url|path|dsn>`, canonical resource-id diagnostics, evidence-preserving ad-hoc mode, and TLC quickstart updates once the underlying source shapes stabilize.

Split executable child tickets before code for `cdf add`, resource-id validation/inspection, ad-hoc resource persistence, quickstart/example updates, and the S1/S2 recorded session.

## Acceptance criteria

- `cdf add tlc.yellow <public parquet URL>` probes, infers, pins, writes resource configuration, and prints the file it wrote.
- `cdf add --dry-run` prints the config without mutating the project.
- Resource ids and mapping patterns are legible through `cdf validate` and `cdf inspect resources`.
- `cdf run <url-or-path> --to <dest>` synthesizes a real `.cdf/adhoc/` resource and preserves plan/package/receipt/checkpoint evidence.
- Docs quickstart uses the TLC dataset and remains executable by conformance or a docs freshness job.

## Evidence expectations

CLI snapshots, project-file fixture diffs, ad-hoc package evidence, docs/example execution, S1+S2 recorded terminal session, and redaction checks.

## Explicit exclusions

This ticket does not implement the lower-level discovery, file, or schema reconciliation primitives it depends on.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-09: Split `.10x/tickets/2026-07-09-p2-ws-h1-resource-id-validation-inspection.md` for canonical compiled-id validation and inspection before `cdf add` and ad-hoc mode.
- 2026-07-09: Closed `.10x/tickets/done/2026-07-09-p2-ws-h1-resource-id-validation-inspection.md`; resource ids and mapping patterns are now legible through validation errors and `cdf inspect resources`.
- 2026-07-09: Split executable child `.10x/tickets/2026-07-09-p2-ws-h2-cdf-add-single-file-parquet.md` for the first `cdf add` surface.
- 2026-07-09: E2 closed in `.10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md`, unblocking H2 for local and deterministic HTTPS single-file Parquet.

## Blockers

None for H2's scoped local/deterministic HTTPS single-file Parquet `cdf add` work.
