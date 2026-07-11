Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-e-hashing-package-io.md
Depends-On: .10x/tickets/2026-07-11-p3-e3-streaming-verification-replay-io.md

# P3 E4: package I/O envelope and triage closeout

## Scope

Run small/large/many-segment/high-cardinality package build/verify/replay cases, publish roofline/hash/sync breakdowns, prove crash/golden/memory laws, and close the original package-I/O triage into measured evidence.

## Acceptance criteria

- Package build reaches ≥70% write roofline and hashing ≤5% wall on named hosts.
- Production construction reports zero redundant content reread bytes.
- High-cardinality build/verify memory is bounded.
- Triage hypotheses are each closed with before/after or measured no-action rationale.

## Evidence expectations

Host reports/profiles/syscall counts, crash/golden suite, memory stress, triage reconciliation, and adversarial filesystem review.

## Explicit exclusions

No hash/artifact semantic change.

## Blockers

Depends on E1-E3.

## References

- `.10x/tickets/2026-07-07-package-io-hashing-overhead-triage.md`
- `.10x/specs/package-io-hashing-durability.md`
