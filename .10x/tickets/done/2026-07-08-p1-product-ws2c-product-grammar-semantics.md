Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-08-p1-product-ws2-command-grammar-redesign.md
Depends-On: .10x/tickets/done/2026-07-08-p1-product-ws2b-clap-parser-foundation.md, .10x/decisions/cli-command-grammar-and-parser.md

# P1 product WS2C: Product grammar semantics

## Scope

Implement the P1 human grammar semantics after the clap parser foundation exists.

Owns command-module behavior for `run`, `plan`, `explain`, `state`, `resume`, `replay package`, and `backfill` where shorter forms need runtime resolution or minted defaults.

## Acceptance criteria

- `cdf run [resource] [--to dest]` runs with system-minted run/package/checkpoint identifiers when omitted.
- Legacy explicit `--pipeline`, `--target`, `--package-id`, and `--checkpoint-id` script forms remain accepted.
- `cdf plan [resource]` and `cdf explain [resource]` resolve destination from the selected environment unless `--to`/legacy `--target` overrides it.
- `cdf state show/history [resource] --scope key=value` works and legacy `--scope-json` remains accepted.
- `cdf state rewind [resource] --to checkpoint` mints the marker checkpoint by default while retaining explicit `--marker-checkpoint` for scripts.
- Bare `cdf resume` scans the run ledger and reports/drains interrupted work according to the run-spine rules; `cdf resume <run-id>` remains accepted.
- `cdf replay package <pkg-or-dir> [--to dest]` preserves Postgres explicit target/dedup safety.
- `cdf backfill <resource> --from ... --to ...` preserves current planner behavior and compatibility aliases.
- JSON output and exit-code compatibility remain stable.

## Evidence expectations

Shortest-command parser/resolution tests, no-write regression tests for rejected paths, focused command tests for each changed command, redaction checks where destination secrets are involved, and quality gates over touched command modules.

## Explicit exclusions

No parser framework migration; WS2B owns it. No completions/man pages; WS2D owns them. No human rendering redesign beyond preserving existing output.

## Blockers

None.

## Progress and notes

- 2026-07-08: Implemented scoped WS2C grammar semantics in cdf-cli parser and command modules. Short `plan`/`explain`, `run`, `state show/history/rewind`, bare `resume`, optional replay destination, and backfill default target behavior are covered by focused tests. Legacy explicit script forms remain covered by the full cdf-cli suite.
- 2026-07-08: Bare `resume` drains exactly one interrupted run through the existing resume path, reports no-op when no interrupted runs exist, and fails closed with exit 78 when multiple interrupted runs require lower-layer multi-run drain semantics.
- 2026-07-08: jscpd still reports pre-existing duplication in `args.rs`/`tests.rs`; WS2C-specific duplicated setup was reduced and the final jscpd JSON reports `newClones: 0`.

## Evidence

- `.10x/evidence/2026-07-08-p1-product-ws2c-product-grammar-semantics.md`

## Review

- `.10x/reviews/2026-07-08-p1-product-ws2c-product-grammar-semantics-review.md`
