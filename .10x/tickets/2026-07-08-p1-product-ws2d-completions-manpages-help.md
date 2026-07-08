Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws2-command-grammar-redesign.md
Depends-On: .10x/tickets/2026-07-08-p1-product-ws2b-clap-parser-foundation.md, .10x/decisions/cli-command-grammar-and-parser.md

# P1 product WS2D: Completions, man pages, and help snapshots

## Scope

Generate and verify shell completions, man pages, and parser help snapshots from clap definitions.

Owns generation code/scripts, CI/release artifact hooks for completions/man pages, and help snapshot tests.

## Acceptance criteria

- Bash, zsh, fish, and PowerShell completions are generated from the clap command definition.
- Man pages are generated from the clap command definition.
- Command and subcommand help snapshots exist.
- A freshness check fails when generated help/completion/man artifacts drift.
- The generated artifacts are wired for release consumption without hand-maintained command reference drift.

## Evidence expectations

Generated artifact diff/freshness output, help snapshots, CI or local equivalent proof, focused Gitleaks, and `jscpd` over generation scripts if any are added.

## Explicit exclusions

No grammar semantics changes. No docs quickstart; WS6 owns docs beyond generated command artifacts.

## Blockers

Depends on WS2B parser foundation.
