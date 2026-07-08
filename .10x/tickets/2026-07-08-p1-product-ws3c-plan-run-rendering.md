Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws3-rendering-system-design-language.md
Depends-On: .10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md, .10x/decisions/cli-design-language-and-renderer.md

# P1 product WS3C: Plan, run, and replay rendering

## Scope

Migrate the highest-value human outputs to the renderer: `cdf plan`, `cdf explain`, `cdf run`, and `cdf replay package`.

Owns `scan_command` report rendering, `reports.rs` run/replay rendering, and focused snapshots.

## Acceptance criteria

- `cdf plan`/`cdf explain` render dense operator panels with pushdown, destination, guarantee, contract, migration, and next-command sections where data exists.
- `cdf run` final output renders a checkpoint-gated run panel with run/package/rows/segments/verdicts/receipt/gate information where data exists.
- `cdf replay package` renders replay/duplicate/receipt/checkpoint facts through the same panel language.
- TTY-rich and headless snapshots exist for each migrated command.
- JSON output remains unchanged except for additive fields if explicitly needed and tested.
- Redaction checks cover destination URI/secret-like values in the rendered output.

## Evidence expectations

Snapshot tests, CLI command tests, JSON compatibility tests, redaction adversarial checks, fmt/clippy, source-only Gitleaks, direct unsafe scan, and focused `jscpd`.

## Explicit exclusions

No live progress; WS5 owns progress. No parser grammar changes. No migration of all remaining command families.

## Blockers

Depends on WS3B.
