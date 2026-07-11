Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-08-p1-product-ws4-error-experience-catalog.md
Depends-On: .10x/specs/cli-error-experience-catalog.md, .10x/tickets/done/2026-07-08-p1-product-ws4a-error-envelope-foundation.md, .10x/tickets/done/2026-07-08-p1-product-ws2c-product-grammar-semantics.md

# P1 product WS4C: Error suggestions

## Scope

Add deterministic, redaction-safe suggestions for unknown commands, resources, and destinations.

Primary write scope is `crates/cdf-cli/src/args.rs`, project-context lookup helpers where needed, destination resolution error handling, tests, and this ticket's records.

## Acceptance criteria

- Unknown command and subcommand errors suggest the nearest valid command when confidence is high enough.
- Unknown resource errors suggest nearest configured project resource ids.
- Unknown destination/target errors suggest configured environment destination names or expected URI shape without revealing secrets.
- Suggestions are deterministic, bounded, and omitted when confidence is low.
- JSON errors expose suggestions additively, and human errors display suggestions once the renderer integration is available.

## Evidence expectations

Record parser and command tests for high-confidence, low-confidence, no-inventory, and redacted-secret cases. Run scoped fmt/test/clippy and required `QUALITY.md` checks, including focused jscpd and complexity output.

## Explicit exclusions

Do not change command grammar beyond WS2C decisions. Do not add interactive prompts. Do not implement docs generation.

## Progress and notes

- 2026-07-08: Split from WS4. This depends on WS2C so suggestions target the ratified product grammar rather than the intermediate parser-only grammar.
- 2026-07-08: Worker started implementation after reading governing WS4C, WS4A, WS4B, WS2C, spec, decision, evidence, and review records.
- 2026-07-08: Implemented additive JSON suggestions for command/subcommand, resource, and destination errors; evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws4c-error-suggestions.md`.
- 2026-07-08: Closure review passed in `.10x/reviews/2026-07-08-p1-product-ws4c-error-suggestions-review.md`; ticket moved to done.
- 2026-07-08: Parent integration review reran quality gates and appended parent evidence in `.10x/evidence/2026-07-08-p1-product-ws4c-error-suggestions.md`.

## Blockers

None. WS4A and WS2C are complete.

## Evidence

- `.10x/evidence/2026-07-08-p1-product-ws4c-error-suggestions.md`

## Review

- `.10x/reviews/2026-07-08-p1-product-ws4c-error-suggestions-review.md`
