Status: active
Created: 2026-08-06
Updated: 2026-08-06
Supersedes: `.10x/specs/superseded/schema-promotion-corrections-lockfile.md`

# State-backed schema promotion and historical correction

## Purpose

This specification governs the only transition between established logical schema versions. It
preserves dry-plan-first UX, immutable packages, exact variant residuals, framework row provenance,
destination-specific correction strategies, fenced leases, receipt-gated checkpoints, and
evidence-driven recovery while making schema-head advancement and publication one state
transaction.

## Command contract

```text
cdf schema promote RESOURCE [--type /field=ARROW_TYPE ...] [--execute]
```

The default is dry planning and MUST write nothing. It MAY read active state authority, fresh
bounded discovery, retained verified packages, residual indexes, destination capability sheets,
physical installation/readback, and checkpoint/receipt history.

The plan MUST report:

- authority domain/key, current generation/version, and exact CAS preconditions;
- proposed immutable version and predecessor;
- new/changed/removed top-level fields and selected target types;
- residual paths, observed types, promotable/non-promotable counts, redaction status, and retained
  evidence availability;
- affected committed packages/rows and destination targets;
- exact correction strategy, DDL, settlement cutoff, and recovery status per target;
- unavailable, redacted, tombstoned, or unverified evidence.

`--execute` performs that exact plan after full revalidation. It MUST refuse ambiguous/lossy types
without explicit existing lossy authority, missing evidence, unsupported target correction, stale
head generation, incomplete target inventory, or unsafe settlement.

Only top-level `/field` promotion is supported initially. A promoted field is nullable unless
complete historical non-nullness is proven. No default is invented.

## Immutable versions and promotion head state

Execution creates the proposed immutable version before changing the head, then atomically CASes
the active head into a fenced `promoting` state carrying promotion id, from/to hashes, generation,
lease owner, and fencing token.

The state store MUST persist ordered promotion events and target settlements. Once any target
correction is durable, recovery normally rolls forward; it cannot quietly restore the previous
active state while leaving destination changes unexplained.

The final transition from `promoting` to the next `active` generation MUST atomically verify the
fence, every required target settlement/checkpoint, and the complete correction cutoff, then write
the new head and publication event in one store transaction.

## Settlement barrier

Ordinary execution and promotion participate in one backend-neutral reader/writer barrier.

An ordinary run:

```text
extract/package under generation G
→ acquire renewable settlement permit bound to exact G
→ recheck active head and permit fence
→ mutate destination
→ verify receipt
→ commit checkpoint under the permit
→ release permit
```

The permit is acquired only immediately before destination mutation; extraction does not hold it.

Promotion:

```text
CAS Active(V1,G1) -> Promoting(V1→V2)
→ block new G1 settlement permits
→ drain or fence existing permits
→ establish complete committed V1 cutoff
→ settle corrections/rematerializations
→ verify receipts and promotion checkpoints
→ atomically publish Active(V2,G2) plus event
```

Requirements:

- state-store time owns expiry;
- permits and promotion fences carry the store authority-domain id;
- expired/stale executors cannot mutate or checkpoint;
- promotion cannot establish its correction cutoff until prior permits drain;
- no V1 destination commit can occur after V2 publication;
- crashes in `promoting` remain resumable from state and verified receipts;
- this is not a long database transaction or distributed 2PC.

Backend-neutral contracts belong in `cdf-kernel`; SQLite implements them now. Postgres is excluded
from this tranche.

## Stable row provenance and strategies

The public logical address remains:

```text
(original_package_hash, original_segment_id, original_row_ordinal)
```

Physical compact keys must remain bijective framework-owned mappings. They are never business keys.
Correction packages reference the original logical address and are independently immutable,
receipt-gated, checkpointed, and idempotent.

Destination sheets declare exact support for:

- `in_place_update`;
- `correction_sidecar`;
- `versioned_rematerialization`;
- canonical variant readback and provenance targeting.

Unsupported capability fails planning. Arbitrary UPDATE predicates, inferred business keys, and
unrecorded target rewrites are forbidden.

## Historical variant reconciliation

For a promoted top-level field, execution MUST:

1. Add the typed field to the proposed logical version.
2. Prepare exact destination DDL or a versioned target.
3. Locate every committed reconstructable residual containing the path before the fenced cutoff.
4. Decode exact `residual-json-v1` values.
5. Coerce through the shared compiler-produced program.
6. Address and write the historical typed value using framework row provenance.
7. Remove only the promoted path from `_cdf_variant`.
8. Set `_cdf_variant` to null only when its envelope becomes empty.
9. Preserve every unrelated residual path.
10. Produce and settle immutable correction/rematerialization packages.

State stores bounded residual summaries and verified content references, never raw residual values.
Each summary carries package hash, schema version, path, type set/hash, row count, content
reference, and redaction/promotability status. Exact values come from retained verified packages or
destination readback whose capability and bytes are verified.

Redacted/non-reconstructable, tombstoned, missing, or unverifiable evidence blocks complete
promotion and names re-extraction/rematerialization when possible. There is no `--future-only`
escape hatch.

## Recovery

Recovery is driven solely by persisted promotion state, correction packages, receipts,
checkpoints, permits/fences, and verified target readback:

- before durable correction: rebuild/discard uncommitted staging under the current fence;
- package without receipt: redrive the exact package idempotently;
- receipt without checkpoint: verify and commit under the live promotion fence;
- partial target settlement: resume remaining targets and never repeat verified effects blindly;
- all settlements complete: atomically publish the new head/event;
- published head/event: repeat execution is a no-op.

A stale lease, foreign authority domain, changed head/predecessor, changed target capability, or
unexplained physical installation fails closed.

## Scenarios

Given retained exact residuals, top-level promotion fills historical columns, removes only the
promoted path, nulls empty envelopes, settles every target, and advances the head exactly once.

Given redacted or tombstoned required residuals, dry planning reports them and execution refuses.

Given Run A packaged V1 before promotion, promotion fences new settlement, waits for or expires A's
permit, includes every committed V1 package in its cutoff, and proves A cannot commit after V2.

Given a crash at any lifecycle boundary, resume reaches one active V2 head or remains explicitly
`promoting`; it never claims completion while a target settlement is missing.

Given an append-only destination, correction sidecar or versioned rematerialization is used only
when declared; otherwise planning fails.

## Acceptance criteria

- Dry plan performs zero writes and contains every authority/evidence/strategy precondition.
- Store tests prove fenced Active→Promoting→Active transitions and append-only ordered history.
- Settlement-permit concurrency tests prove no late old-generation commit escapes the cutoff.
- Crash tests cover each transition and stale/expired/foreign-domain actors.
- Target conformance tests cover all three correction strategies and false capability claims.
- Residual tests cover exact coercion, unrelated-path preservation, empty-envelope nulling,
  redaction/tombstone refusal, and verified readback substitution.
- All target settlements complete before atomic head/publication advancement.
- Old packages remain byte-identical and replayable.

## Explicit exclusions

- automatic promotion, same-run epochs, or source-discovery-driven DDL;
- future-only or partial historical reconciliation;
- nested path promotion;
- arbitrary business-key updates;
- raw residual values in state;
- Postgres state implementation or distributed scheduler.

## Ratification status

The user ratified state-atomic promotion publication, settlement permits/fencing, complete
top-level reconciliation, and the initial exclusions on 2026-08-06.
