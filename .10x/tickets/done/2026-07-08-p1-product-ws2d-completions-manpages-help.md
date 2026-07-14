Status: done
Created: 2026-07-08
Updated: 2026-07-13
Parent: .10x/tickets/done/2026-07-08-p1-product-ws2-command-grammar-redesign.md
Depends-On: .10x/tickets/done/2026-07-08-p1-product-ws2b-clap-parser-foundation.md, .10x/decisions/superseded/cli-command-grammar-and-parser.md

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

## Progress and notes

- 2026-07-08: Implemented `cdf-generate-cli-artifacts` behind the `cli-artifacts` feature. It generates completions, man pages, and help snapshots from `cdf-cli`'s clap command definition and checks committed generated artifacts for byte-for-byte freshness.
- 2026-07-08: Added committed generated artifacts under `crates/cdf-cli/generated/`: bash, zsh, fish, and PowerShell completions; 42 man pages; and 42 help snapshots.
- 2026-07-08: Wired fast quality to run the freshness test and release artifact CI to check committed freshness, generate `target/generated`, and package generated completions/man pages.
- 2026-07-08: Updated release artifact smoke tests and supply-chain vet exemptions for the new generator-only crates.
- 2026-07-08: Closure evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws2d-completions-manpages-help.md`; review recorded in `.10x/reviews/2026-07-08-p1-product-ws2d-completions-manpages-help-review.md`.

## Evidence

- `.10x/evidence/2026-07-08-p1-product-ws2d-completions-manpages-help.md`

## Review

- `.10x/reviews/2026-07-08-p1-product-ws2d-completions-manpages-help-review.md`

## Blockers

None.
