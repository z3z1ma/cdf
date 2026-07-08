Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws3-rendering-system-design-language.md
Depends-On: .10x/tickets/2026-07-08-p1-product-ws3b-renderer-foundation.md, .10x/decisions/cli-design-language-and-renderer.md

# P1 product WS3D: Recovery, state, and backfill rendering

## Scope

Migrate recovery-heavy command families to the renderer: `resume`, `state show/history/rewind/migrate/recover`, `backfill`, and `inspect run`.

## Acceptance criteria

- Recovery guidance renders as structured panels with failed phase, durable artifacts, mutation performed, and next command.
- State history/show/rewind/migrate/recover outputs use tables or panels instead of raw sentences.
- Backfill planner output uses the same plan/panel primitives as `cdf plan` where applicable.
- `inspect run` uses renderer redaction and explicitly shows missing artifacts.
- TTY-rich and headless snapshots exist for each migrated command family.
- JSON output remains stable.

## Evidence expectations

Snapshot tests, focused command tests, redaction adversarial checks, fmt/clippy, source-only Gitleaks, direct unsafe scan, and focused `jscpd`.

## Explicit exclusions

No live progress. No parser grammar changes. No migration of project/contract/package/doctor/status/sql outputs.

## Blockers

Depends on WS3B.
