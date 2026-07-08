Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/2026-07-08-p1-product-ws4-error-experience-catalog.md, .10x/specs/cli-error-experience-catalog.md

# P1 WS4 error catalog shaping evidence

## What was observed

WS4 was broad and mixed several independent outcomes: structured error fields, migration of all construction sites, suggestions, renderer integration, and generated docs.

Existing source inspection found:

- `crates/cdf-cli/src/output.rs` centralizes `CliError`, `ErrorBody`, JSON error envelopes, and human error formatting.
- Current JSON errors include `kind`, `message`, `exit_code`, and `not_supported`.
- `CliError::usage` maps parser/usage failures to exit 2.
- `CliError::not_supported` maps unsupported implementation paths to exit 78.
- `From<CdfError> for CliError` maps shared `ErrorKind` values to existing exit codes.
- Direct error construction sites exist across `args.rs`, command modules, and `system_sql.rs`.

Governing records establish:

- `.10x/decisions/cli-command-grammar-and-parser.md` requires parser suggestions and JSON compatibility.
- `.10x/decisions/cli-design-language-and-renderer.md` requires renderer-owned human display, redaction, and per-kind error snapshots.
- `.10x/specs/project-cli-observability-security.md` requires the shared error taxonomy and redaction.

## Procedure

Read the WS4 parent, CLI grammar decision, renderer decision, `output.rs`, and a source inventory of `CliError` construction sites. Then wrote:

- `.10x/specs/cli-error-experience-catalog.md`
- `.10x/tickets/2026-07-08-p1-product-ws4a-error-envelope-foundation.md`
- `.10x/tickets/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`
- `.10x/tickets/2026-07-08-p1-product-ws4c-error-suggestions.md`
- `.10x/tickets/2026-07-08-p1-product-ws4d-error-rendering-docs.md`

## What this supports or challenges

This supports executing WS4 in bounded, reviewable slices. The envelope foundation can land before renderer completion; final human presentation and docs remain blocked on the renderer/docs lanes.

## Limits

No implementation or tests were run for this shaping slice. Child tickets own implementation evidence and quality gates.
