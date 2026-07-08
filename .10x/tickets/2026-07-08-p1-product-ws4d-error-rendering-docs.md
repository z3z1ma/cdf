Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws4-error-experience-catalog.md
Depends-On: .10x/specs/cli-error-experience-catalog.md, .10x/tickets/2026-07-08-p1-product-ws4b-error-construction-site-migration.md, .10x/tickets/2026-07-08-p1-product-ws4c-error-suggestions.md, .10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md, .10x/tickets/2026-07-08-p1-product-ws6b-generated-reference-freshness.md

# P1 product WS4D: Error rendering and generated docs

## Scope

Render structured errors through the WS3 renderer and generate the error catalog docs consumed by WS6.

Primary write scope is renderer integration points in `crates/cdf-cli/src/**`, generated error reference sources under `docs/**` or `tools/**`, freshness checks, snapshots, and this ticket's records.

## Acceptance criteria

- Human errors render through the CLI renderer in TTY-rich and headless modes.
- Snapshots cover each error kind, not-supported errors, suggestions, remediation, and redaction.
- The generated error-code reference lists code, area, kind, exit code, meaning, remediation, and representative command.
- CI or a local freshness command fails when catalog docs are stale.
- JSON error output remains additive-only and stable.

## Evidence expectations

Record renderer snapshots, generated-doc freshness output, docs diff proof, redaction adversarial output, and required scoped quality checks from `QUALITY.md`, including jscpd and complexity output for touched source.

## Explicit exclusions

Do not change code assignments unless WS4B/WS4C left an explicit blocker. Do not redesign unrelated command output; WS3 migration children own non-error output.

## Progress and notes

- 2026-07-08: Split from WS4. Final closure depends on the renderer foundation and WS6 generated-reference freshness lane.

## Blockers

Blocked until WS4B, WS4C, WS3B, and WS6B land.
