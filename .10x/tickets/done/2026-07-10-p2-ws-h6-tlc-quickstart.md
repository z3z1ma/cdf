Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md
Depends-On: .10x/specs/docs-onboarding-surface.md, .10x/specs/data-onramp-source-experience-cli.md

# P2 WS-H6 TLC quickstart

## Scope

Replace the toy local-NDJSON quickstart with the canonical public TLC path covering add/run, monthly manifest incrementality, governed drift, and package replay.

## Acceptance criteria

- Commands use the current parser and generated Tier-0 resource shape.
- S1 explains zero-schema footer pinning and commit-gated run.
- S2 explains monthly logical partitions, manifest no-op, and new-file-only behavior.
- S6 explains typed quarantine and explicit repinning without claiming mutable public data will drift on demand.
- Replay is source-free and uses a clean ledger to avoid checkpoint collision.
- Network-independent exact regression commands are named and pass.

## Evidence expectations

Exact S1/S2/S6 tests and P2 matrix suite, link/path inspection, adversarial docs review.

## Blockers

None.

## Progress and notes

- 2026-07-10: Closed with `.10x/evidence/2026-07-10-p2-h6-tlc-quickstart.md` and `.10x/reviews/2026-07-10-p2-h6-tlc-quickstart-review.md`.
