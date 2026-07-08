Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws4-error-experience-catalog.md
Depends-On: .10x/specs/cli-error-experience-catalog.md

# P1 product WS4A: Error envelope foundation

## Scope

Add the structured error-code and remediation foundation to `cdf-cli` without migrating every construction site.

Primary write scope is `crates/cdf-cli/src/output.rs`, a focused `crates/cdf-cli/src/error_catalog.rs` or equivalent module, tests, and this ticket's records.

## Acceptance criteria

- `CliError` and `ErrorBody` carry stable `code` and optional structured `remediation` fields while retaining existing JSON fields.
- Conversions from `CdfError` use a documented generic mapping when a call site has not yet supplied a more specific code.
- `CliError::usage` and `CliError::not_supported` preserve exit codes 2 and 78.
- JSON error envelope compatibility tests prove existing fields still exist and new fields are additive.
- Human plain-text errors remain at least as informative until WS3 renderer integration lands.

## Evidence expectations

Record focused `cdf-cli` tests for JSON compatibility, usage errors, not-supported errors, and generic lower-layer conversion. Run scoped fmt/test/clippy plus required `QUALITY.md` checks for touched Rust, including jscpd and complexity output.

## Explicit exclusions

Do not migrate every `CliError` construction site. Do not implement suggestions or generated docs. Do not change success envelopes or exit-code semantics.

## Progress and notes

- 2026-07-08: Split from WS4 after creating `.10x/specs/cli-error-experience-catalog.md`.

## Blockers

None.
