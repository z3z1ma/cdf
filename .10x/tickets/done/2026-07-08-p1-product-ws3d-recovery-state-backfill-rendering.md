Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws3-rendering-system-design-language.md
Depends-On: .10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md, .10x/decisions/cli-design-language-and-renderer.md

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

None.

## Progress and notes

- 2026-07-08: Worker inspected the owning ticket, renderer decision, WS3B/WS3C ticket/evidence/review records, `QUALITY.md`, dirty worktree status, renderer primitives, output plumbing, and relevant backfill/inspect-run/resume/state command modules and tests before editing. Existing unrelated dirty records/source were left untouched.
- 2026-07-08: Migrated `backfill`, `inspect run`, `resume`, and `state show/history/rewind/migrate/recover` human output to renderer documents while preserving JSON report structs/envelopes, parser grammar, and runtime mutation behavior.
- 2026-07-08: Added headless and forced-rich snapshot-style tests for WS3D command families, JSON compatibility coverage through existing sentinels, missing-artifact display checks, and adversarial rendered URI-userinfo redaction checks.
- 2026-07-08: Closure evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md`; review recorded in `.10x/reviews/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering-review.md`.
- 2026-07-08: Parent review normalized state follow-up commands from lossless `--scope-json` objects into the P1-taught `--scope key=value` grammar and added focused coverage.
