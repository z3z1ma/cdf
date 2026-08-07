Status: open
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/2026-08-06-state-backed-schema-authority-program.md`
Depends-On: `.10x/tickets/done/2026-08-06-s1-state-schema-authority-foundation.md`, `.10x/tickets/done/2026-08-06-s2-state-backed-preparation-portable-plan.md`, `.10x/tickets/done/2026-08-06-s3-schema-drift-dispositions.md`

# S4 state-backed promotion settlement

## Scope

Move schema promotion from lockfile CAS plus ledger publication to one state-backed fenced
promotion lifecycle:

- add generation-bound renewable ordinary-run settlement permits acquired only immediately before
  destination mutation and held through receipt verification/checkpoint commit;
- transition exact Active(V1,G1) to fenced Promoting(V1→V2), block new V1 permits, drain/expire old
  permits, and establish the complete committed V1 correction cutoff;
- persist promotion plan, version, target settlements, checkpoints, residual summary references,
  recovery status, and ordered events in state;
- reconcile complete retained top-level exact residual history through compiler-produced coercions,
  framework row provenance, and declared in-place/sidecar/rematerialization strategies;
- preserve unrelated residual paths and null empty envelopes;
- atomically validate all target settlements and publish Active(V2,G2) plus publication event;
- replace `installed_lock_sha256` and lockfile recovery artifacts with state version/head identity;
- make dry promotion report every concurrency/evidence/DDL/correction/recovery precondition and
  keep `--execute` exact.

## Non-goals

- Postgres store, distributed scheduler, or long-held database transaction;
- automatic, future-only, lossy-without-explicit-authority, nested, or partial promotion;
- raw residual values in state;
- arbitrary business keys or destination schema as logical authority;
- changing immutable old package bytes.

## Acceptance criteria

1. Dry promotion writes nothing and reports current/proposed authority, residual inventory,
   promotability, target strategy/DDL, cutoff, fence, and recovery state.
2. Run settlement permit validates exact domain/key/generation immediately before destination
   mutation and fences receipt/checkpoint commit after expiry/head change.
3. Promotion blocks new old-generation permits and cannot set its correction cutoff until earlier
   permits settle or expire safely.
4. A run packaged under V1 cannot commit an uncorrected V1 result after V2 publication.
5. Complete top-level reconciliation fills historical typed values, removes only promoted paths,
   preserves unrelated paths, and nulls empty envelopes.
6. Redacted/tombstoned/missing/unverified required evidence blocks execution precisely.
7. All declared correction strategies pass destination conformance; false capabilities fail.
8. Every target receipt and promotion checkpoint is verified before one atomic head/event advance.
9. Failpoints at every lifecycle boundary resume idempotently; stale/expired/foreign-domain
   executors cannot publish.
10. Old packages remain byte-identical/replayable and repeated completed promotion is a no-op.

## References

- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/specs/schema-drift-dispositions.md`
- `.10x/specs/residual-variant-capture.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/decisions/compact-lossless-destination-row-provenance.md`
- `.10x/decisions/promotion-correction-value-authority.md`
- `.10x/knowledge/fenced-lease-lock-publication.md`
- `.10x/knowledge/destination-receipt-authority.md`

## Assumptions

- User-ratified: state-atomic head/publication, complete top-level reconciliation, and no
  future-only/nested initial escape hatch.
- Record-backed: ordinary run permit is short and never held during extraction.
- Record-backed: verified packages or declared verified destination readback own exact values.
- Record-backed: after any durable target correction, recovery rolls forward unless evidence proves
  a complete safe no-effect abort.

## Journal

- 2026-08-06: Opened behind S1–S3; implementation intentionally deferred.

## Blockers

S1–S3 must close with state heads, state-backed preparation, and total residual dispositions.

## Evidence

Pending execution.

## Review

Pending handback review.

## Retrospective

Pending execution.
