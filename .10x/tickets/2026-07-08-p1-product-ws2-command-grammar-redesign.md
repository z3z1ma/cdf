Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/specs/project-cli-observability-security.md

# P1 product WS2: Command grammar redesign

## Scope

Ratify and implement a coherent CLI grammar using a mature argument parser, replacing the hand-rolled parser and making the primary noun positional, system-defined identifiers minted by the system, project/environment defaults first-class, and flags consistent.

This workstream MUST ratify a grammar decision record before implementation. If parser migration, generated completions, man pages, and compatibility tests cannot be completed safely as one slice, split executable child tickets before code changes.

## Required outcomes

- Adopt clap v4 derive or a ratified equivalent mature parser.
- Delete the hand-rolled parser once all callers and tests migrate.
- Provide per-subcommand help, typo suggestions, shell completions for bash/zsh/fish/PowerShell, man-page generation, and styled help.
- Ratify precedence: explicit flag, then env var, then project/environment config, then minted or derived default.
- `--to` is the destination flag everywhere, `--env` is global, and `--scope key=value` replaces raw JSON for human entry.
- Destructive or ambiguous actions get confirmations and `--yes`.
- `cdf run` with no arguments runs project resources against the environment destination; `--resource` remains only for script compatibility.

## Normative shortest forms

The grammar decision MUST extend this table to every command:

- `cdf run`
- `cdf run github.issues`
- `cdf run github.issues --to duckdb://local.duckdb`
- `cdf plan [github.issues]`
- `cdf preview github.issues [--limit 500]`
- `cdf state show [github.issues]`
- `cdf state rewind github.issues --to chk_01J...`
- `cdf replay pkg_01J... [--to postgres://...]`
- `cdf resume`
- `cdf backfill github.issues --from 2026-01-01 --to 2026-07-01`
- `cdf sql "select ..."`

## Acceptance criteria

- A grammar decision record contains the full command table, precedence rules, compatibility posture, and destructive-action confirmation policy.
- Exhaustive parser tests cover legacy accepted forms and the normative shortest forms.
- Generated completions and man pages are created by CI/release machinery.
- Help snapshots cover command and subcommand help.
- Existing JSON output contracts remain stable.

## Evidence expectations

Record parser test output, help snapshots, generated artifact checks, and a migration proof showing no active CLI path uses the old parser.

## Explicit exclusions

No human-output redesign beyond parser/help styling owned by the ratified parser stack; WS3 owns command rendering. No behavioral weakening of existing run/replay/resume guarantees.

## Progress and notes

- 2026-07-08: Opened from P1 product directive after the current CLI grammar was found operational but hostile to operators.
- 2026-07-08: Split decision child `.10x/tickets/done/2026-07-08-p1-product-ws2a-cli-grammar-decision.md` before parser implementation.
- 2026-07-08: Active grammar/parser decision recorded in `.10x/decisions/cli-command-grammar-and-parser.md`; implementation children split as WS2B parser foundation, WS2C product grammar semantics, and WS2D completions/man/help artifacts.

## Blockers

None for shaping. Implementation must first ratify the grammar decision.
