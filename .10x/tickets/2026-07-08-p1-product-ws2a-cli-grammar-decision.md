Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws2-command-grammar-redesign.md
Depends-On: .10x/specs/project-cli-observability-security.md

# P1 product WS2A: Ratify CLI grammar decision

## Scope

Ratify the command grammar decision required before replacing the hand-rolled parser.

Owns:

- Inventorying the current command/subcommand grammar, flags, positional forms, help behavior, JSON behavior, and exit compatibility.
- Writing an active decision record in `.10x/decisions/` that selects the parser stack, command grammar, precedence model, compatibility posture, destructive-action confirmation policy, generated completion/man-page posture, and full command table.
- Opening follow-up executable child tickets for parser migration after the decision is active.

## Acceptance criteria

- The decision record uses Nygard-style context, decision, alternatives considered, and consequences.
- The full command table covers every command in `.10x/specs/project-cli-observability-security.md`: `init`, `validate`, `plan`, `explain`, `run`, `preview`, `sql`, `inspect`, `diff schema`, `contract freeze/show/test`, `state show/history/rewind/migrate/recover`, `resume`, `replay package`, `backfill`, `package ls/gc/verify`, `doctor`, `status`, and `package archive`.
- The decision ratifies the precedence model: explicit flag, then environment variable, then project/environment config, then minted or derived default.
- The decision preserves the stable JSON envelope and exit-code taxonomy; JSON additions are additive only.
- The decision states how legacy flag-heavy forms remain script-compatible while the shortest operator forms become primary.
- Parser migration child tickets are opened with disjoint write scopes.

## Evidence expectations

Record the current grammar inventory, decision record, review of the decision, and no-code quality checks over the new/updated records.

## Explicit exclusions

No parser implementation, no dependency changes, no command behavior changes, no generated completions or man pages in this ticket.

## Progress and notes

- 2026-07-08: Opened as the required decision slice before WS2 parser migration. A read-only CLI inventory explorer was dispatched concurrently to inform this ticket.

## Blockers

Pending current-command inventory before the final decision can be written.
