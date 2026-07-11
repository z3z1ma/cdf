Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p1-ws9-cli-experience-excellence.md
Depends-On: .10x/specs/cli-interaction-excellence.md

# P1 CX1: terminal policy, channels, and help

## Scope

Implement centralized quiet/verbose, color, progress, pager, Unicode, terminal-width, and stdout/stderr policy. Complete short/long help descriptions and keep generated artifacts derived from clap authority.

## Acceptance criteria

- Global policy parses exactly, including conflicts and compatibility aliases.
- Terminal size/display width and TTY/headless channel behavior follow the spec.
- Short and long help are useful and generated artifacts remain fresh.
- Focused snapshots cover TTY, redirection, widths, Unicode/ASCII, color, and pager eligibility.

## Exclusions

No command-family visual migration or live progress redesign; CX2/CX3 own those.

## Blockers

None.

## Evidence expectations

Parser tests, TTY/headless channel snapshots, width/display tests, help/man/completion freshness, redaction checks, and focused CLI quality commands.

## References

- `.10x/decisions/cli-progressive-disclosure-terminal-contract.md`
- `.10x/decisions/cli-command-grammar-and-parser.md`
