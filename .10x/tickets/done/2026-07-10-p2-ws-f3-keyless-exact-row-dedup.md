Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-08-p2-ws-f-keys-dispositions.md
Depends-On: .10x/decisions/keyless-exact-row-deduplication.md

# P2 WS-F3 keyless exact-row dedup

## Scope

Add the explicit Tier-0 exact-row dedup option, carry it through kernel resource semantics, compile it into validation evidence, execute it for append packages, and prove null-safe multi-batch behavior.

## Acceptance criteria

- `deduplicate = "exact_row"` is accepted only with append and needs no key.
- The validation program records an all-column, keep-first package-order rule.
- Identical rows across batches/partitions are emitted once; different rows and null-bearing identical rows are preserved/dropped correctly.
- Package dedup evidence is deterministic and destinations receive only retained rows.
- Without the option, append behavior is unchanged.

## Explicit exclusions

Cross-package/destination-state deduplication and approximate deduplication are excluded.

## Evidence expectations

Compiler negative tests, contract evaluator coverage, engine/package evidence, CLI project run coverage, strict lint.

## Blockers

None.

## Progress and notes

- 2026-07-10: Opened after self-ratifying the exact-row semantic under the user's explicit decision authority.
- 2026-07-10: Closed after compiler, null-safe typed-row evaluator, engine/package, project CLI, workspace all-target compile, focused suite, and strict lint evidence passed. Evidence: `.10x/evidence/2026-07-10-p2-f3-keyless-exact-row-dedup.md`. Review: `.10x/reviews/2026-07-10-p2-f3-keyless-exact-row-dedup-review.md`.
