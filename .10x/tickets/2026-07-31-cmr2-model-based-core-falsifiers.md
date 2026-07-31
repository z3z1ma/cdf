Status: open
Created: 2026-07-31
Updated: 2026-07-31
Parent: `.10x/tickets/2026-07-31-connector-mode-readiness-program.md`
Depends-On: `.10x/tickets/2026-07-31-cmr1-reliable-deep-quality-certificate.md`

# Add model-based core falsifiers

## Scope

Add bounded generative tests for deterministic package identity across execution-shape variation
and for receipt-gated settlement across crash/retry/duplicate recovery sequences, using explicit
reference models and the current production authorities.

## Non-goals

- No TLA+/Kani/Loom program, distributed scheduler model, or production state-machine rewrite.
- No new product semantics or weakening of current deterministic and checkpoint contracts.
- No unbounded fuzz campaign in ordinary pull-request checks.

## Acceptance criteria

- Generated equivalent logical inputs vary batch/partition boundaries and permitted completion or
  scheduling order while asserting identical canonical package/segment identity.
- Generated settlement sequences vary durable receipt, checkpoint proposal/commit, crash/reopen,
  duplicate replay, and stale/tampered inputs against a small explicit reference model.
- The settlement model never admits a checkpoint before a verified matching receipt, never
  accepts conflicting duplicate authority, and converges under valid recovery.
- Case counts and shrink behavior are deterministic and bounded for scheduled CI.
- Focused mutation or deliberately faulty self-tests demonstrate that the harness detects at least
  the core fences it claims to protect.

## References

- `.10x/tickets/2026-07-31-connector-mode-readiness-program.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/knowledge/product-integration-and-closure-gate.md`
- `.10x/knowledge/quality-gate-execution.md`

## Assumptions

- Record-backed: existing fixed failpoint and chaos tests remain valuable examples but do not
  generate multi-action sequences.
- Record-backed: randomized tests must use fixed reproducible seeds/case limits and remain
  evidence within their generated domain rather than a universal correctness claim.

## Journal

- 2026-07-31: Shaped as two narrow falsifiers around CDF's most consequential invariants, not a
  request to model the whole runtime.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending program review.

## Retrospective

Pending execution.
