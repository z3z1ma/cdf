Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p1-ws9-cli-experience-excellence.md
Depends-On: .10x/tickets/2026-07-11-p1-cx1-terminal-policy-help.md

# P1 CX2: compact renderer and actionable errors

## Scope

Add activity, compact summary, borderless table/list, narrow-stack, contextual error/help, and value-expansion primitives; migrate static command families without changing report/JSON truth.

## Acceptance criteria

- Ordinary output removes ornamental rules and unnecessary boxed grids.
- Default hierarchy is outcome, essential evidence, next action; verbose adds proof detail.
- Errors render stable code/context/help through renderer primitives.
- Unicode display width, narrow fallback, redaction, and JSON stability are proven.

## Blockers

Depends on CX1.

## Evidence expectations

Representative normal/verbose/narrow/ASCII/error snapshots, JSON compatibility tests, truncation-detail access, redaction adversarial cases, and focused CLI quality commands.

## References

- `.10x/decisions/cli-design-language-and-renderer.md`
- `.10x/specs/cli-error-experience-catalog.md`
