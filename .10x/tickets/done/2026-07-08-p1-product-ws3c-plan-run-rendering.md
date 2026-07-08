Status: done
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

## Progress and notes

- 2026-07-08: Worker inspected the WS3C ticket, renderer decision, WS3B ticket/evidence/review, `QUALITY.md`, dirty worktree status, renderer foundation modules, plan/explain scan code, run/replay report code, replay command handoff, output plumbing, and relevant CLI tests before editing. Existing dirty WASM `.10x` records are unrelated and remain untouched.
- 2026-07-08: Migrated plan/explain, run, and replay package human output to renderer documents; added URI userinfo redaction for rendered display values; added rich/headless/static command-family tests, JSON compatibility tests, and redaction checks.
- 2026-07-08: Corrected plan/explain next-command rendering to avoid system-minted `--package-id`/checkpoint identifiers. Default-target plans render `cdf run <resource>`; non-default targets render `cdf run <resource> --target <target>`.
- 2026-07-08: Corrected explicit-destination next-command rendering to preserve `--to <destination>` with URI userinfo redacted, then aligned existing `cdf-cli` run-ledger assertions with the WS1C event/schema expansion so the integrated `cdf-cli` suite stays green.
- 2026-07-08: Closure evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws3c-plan-run-rendering.md`; review recorded in `.10x/reviews/2026-07-08-p1-product-ws3c-plan-run-rendering-review.md`.
- 2026-07-08: Integrated parent review correction for explicit plan/explain destinations. Next-command rendering now preserves `--to <destination>` with URI userinfo redacted, while still omitting system-minted package/checkpoint IDs; focused main-workspace verification passed and records were updated.
