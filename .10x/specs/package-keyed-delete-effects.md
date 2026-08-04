Status: active
Created: 2026-08-03
Updated: 2026-08-03

# Package keyed delete effects

## Purpose and authority

This specification defines CDF's source-neutral, destination-neutral package representation for
keyed upserts and deletes. It governs source deletion coverage, package content/identity,
cross-effect winner selection, disposition admission, destination application policy, receipts,
replay, errors, and conformance.

It implements `.10x/decisions/package-native-keyed-delete-effects.md` and is further governed by:

- `.10x/specs/spillable-package-dedup.md`;
- `.10x/specs/canonical-package-row-ordinal.md`;
- `.10x/specs/destination-receipts-guarantees.md`;
- `.10x/specs/streaming-destination-ingress.md`;
- `.10x/knowledge/net-new-no-compatibility-policy.md`.

This is the common handoff contract for log CDC, SaaS deletion feeds, tombstone-bearing APIs,
future snapshot-diff sources, and every other source that can prove a keyed deletion fact.

## Ubiquitous language

- **Delete capture**: whether a source observes and emits deletion facts available from its native
  protocol or API.
- **Keyed effect**: exactly one logical `upsert` or `delete` addressed by one complete declared key
  tuple.
- **Upsert effect**: a complete output row that establishes the desired value for its key without
  claiming whether the destination will physically insert or update.
- **Delete effect**: a complete declared key tuple that establishes logical absence/deletion for
  its key without carrying non-key values.
- **Effect reduction**: deterministic selection of at most one surviving effect per exact typed key
  across both effect families.
- **Delete application**: the compiled destination behavior `ignore`, `hard`, or `soft` applied to
  captured package delete effects.
- **Event-history resource**: an ordinary append resource that intentionally preserves every
  intermediate source event. It is not a keyed-change package.

## Package content model

The current package format MUST replace its single untyped segment family with one closed content
model equivalent to:

```rust
enum PackageContentIdentity {
    Rows {
        segments: Vec<RowSegmentIdentity>,
    },
    KeyedChanges {
        key: KeyAuthority,
        reduction: KeyedEffectReductionAuthority,
        upserts: Vec<KeyedEffectSegmentIdentity>,
        deletes: Vec<KeyedEffectSegmentIdentity>,
    },
}

enum KeyedEffectKind {
    Upsert,
    Delete,
}
```

Exact Rust names MAY follow established crate naming, but the closed alternatives and invariants
MUST remain. Optional delete fields added to ordinary row segments are forbidden because they
would permit invalid mixed states.

`append` and `replace` MUST produce `Rows`. `merge` and `cdc_apply` MUST produce `KeyedChanges`.
A `merge` input row lowers to `upsert`; a `cdc_apply` insert/update/replace after-image lowers to
`upsert`; a captured deletion lowers to `delete`.

`append` and `replace` MUST reject a source plan capable of emitting admitted delete effects. A
replacement's complete-target semantics are not a delete-effect stream and MUST NOT be represented
as one.

## Key authority and delete schema

Every `KeyedChanges` package MUST bind a nonempty ordered key vector from the compiled resource
primary/merge-key authority. Each key field MUST resolve to exactly one normalized output field
with identical Arrow type, nullability, semantic metadata, and source-name authority.

Key fields MUST be non-null for every admitted effect. A missing, null, ambiguous, quarantined,
lossily coerced, or otherwise untrustworthy key MUST fail the affected package before effect
segment publication. A row with an invalid key MUST NOT be converted into an unaddressed delete or
silently omitted when that would allow the source frontier to advance.

The delete logical schema MUST be derived mechanically by projecting the complete output schema
onto the ordered key vector. Users and source adapters MUST NOT author a separate delete schema.
Delete segments MUST contain only those key fields plus the package's reserved effect-provenance
field. They MUST NOT contain invented null/default values for non-key output fields.

Exact key equality and canonical encoding MUST use the versioned `cdf-dedup-key-v1` semantics from
`.10x/specs/spillable-package-dedup.md`. Hash collisions require exact comparison. Display strings,
destination coercions, JSON rendering, collation, and source-specific equality are forbidden as
package winner authority.

Package deletes are equality-by-key. Predicate, range, partition, source-file-position, or native
destination-expression deletes MUST fail compilation as unsupported package effects.

## Effect reduction

Before keyed-effect segment finalization, package construction MUST evaluate upserts and deletes in
one shared exact-key winner domain. Exactly zero or one effect may survive for each key.

The compiled reduction authority MUST record:

- exact key fields and encoding version;
- winner policy `fail`, `first`, or `last`;
- the authoritative input-order source and its identity/version;
- input upsert/delete counts;
- duplicate-key count across all effect-kind combinations;
- surviving upsert/delete counts;
- spill/provenance format and deterministic artifact identities.

Required policy follows:

- ordinary `merge` defaults to `fail` when no explicit keyed dedup rule exists;
- explicit ordinary-merge `first`/`last` uses canonical package input order only when that order is
  a compiled, reproducible semantic authority;
- `cdc_apply` MUST use `last` under the source protocol's exact event order;
- a source combining separate upsert and deletion feeds MUST prove one total change order before
  selecting `first`/`last`; otherwise a repeated key across or within feeds MUST fail;
- destination arrival, partition scheduling, batch boundaries, spill crossover, and worker jobs
  MUST NOT select a winner.

The same rule applies to `upsert/upsert`, `upsert/delete`, `delete/upsert`, and `delete/delete`.
Examples under authoritative order:

| Input effects | Surviving package effect |
|---|---|
| insert A, update A | upsert with final complete A |
| update A, delete A | delete A |
| delete A, insert A | upsert with final complete A |
| delete old key A, insert new key B | delete A and upsert B |

An event-history consumer that requires both input effects MUST use an append resource. Keyed
effect reduction is not optional evidence loss; it is the declared current-state product semantic.

Effect reduction MUST satisfy the memory/spill, collision, cancellation, jobs-invariance, and
provenance laws of `.10x/specs/spillable-package-dedup.md`. Its identity artifacts MUST distinguish
input event counts from surviving package effects. Replay MUST consume finalized effects and MUST
NOT re-run winner selection.

## Canonical physical identity

Upsert and delete segments MUST be distinct typed segment families. Every effect segment identity
MUST include at least:

- effect kind;
- segment id and ordinal within its effect family;
- canonical package-effect ordinal start and row count;
- byte count and SHA-256;
- logical effect-schema hash;
- complete output-schema hash;
- key-authority hash;
- state/output-position authority required by the enclosing checkpoint transition.

The package MUST use a versioned deterministic effect ordering after winner selection. Version 1
orders all surviving upserts by exact encoded key, followed by all surviving deletes by exact
encoded key, and assigns one dense zero-based package-effect ordinal across that identity order.
This order defines package identity and provenance only. It MUST NOT require a destination to
physically apply upserts before deletes.

Upsert storage schema is the complete logical output schema plus the exact reserved effect ordinal.
Delete storage schema is the derived key schema plus that same reserved effect ordinal. The
ordinal field MUST be framework-reserved, non-null `UInt64`, semantically tagged, and removed from
the user-visible target just like existing package row provenance.

The package manifest, state delta, destination commit preimage/request, staged-ingress request and
acknowledgement, archive index, and receipt segment acknowledgement MUST carry effect kind
directly. A consumer MUST reject an omitted, unknown, duplicated, reordered, cross-kind-reused, or
schema-mismatched effect identity before destination mutation or checkpoint advance.

Package hash MUST cover content kind, key authority, reduction authority/evidence, delete capture
coverage, exact effect segment identities, and the identity-bearing destination delete-application
policy. A pathname such as `data/deletes/` MAY be a mechanical layout but MUST NOT be the only
effect-kind authority.

## Source deletion capture

Every source sheet that can expose deletion facts MUST declare capture support as one of:

- `unsupported`: the native source contract cannot provide authoritative deletes;
- `optional`: a compiled source option selects whether the deletion feed is observed;
- `inherent`: the selected source stream includes deletes as part of its truthful history and they
  cannot be discarded without changing stream semantics.

The compiled source plan MUST record the selected capture state and every native option, scope,
query/feed identity, time window, and protocol behavior that changes deletion coverage. This
authority MUST participate in plan/lock/package identity and source-position scope where required.

An optional source such as a future Salesforce connector MAY expose a source-specific ergonomic
option, but it MUST lower into the common capture authority. Capture disabled means CDF makes no
claim that the target reflects source deletions. Plan, run, package, and receipt evidence MUST make
that coverage limitation visible. Capture enabled requires every observed deletion to become an
admitted delete effect or a package-failing typed error before source checkpoint advance.

Source adapters MUST emit logical delete keys and native ordering/position evidence. They MUST NOT
choose hard/soft application, name destination marker fields, execute destination deletes, or
construct destination-native tombstones.

## Delete application policy

Every delete-capable `merge` or `cdc_apply` resource binding MUST explicitly select exactly one:

```text
ignore
hard
soft(marker_field)
```

There is no default. The selected value MUST be validated against the destination sheet, recorded
in the compiled destination policy/commit plan, included in package and plan identity, rendered by
plan/inspect/replay surfaces, and compared exactly on replay before mutation.

### Ignore

The destination MUST acknowledge and settle the exact delete effect segments without mutating
target rows for them. The receipt MUST report the exact ignored delete-effect count. Delete effects
remain in the package and remain covered by its package hash, source frontier, and receipt.

### Hard

The destination MUST remove every existing target row whose key equals a package delete key under
the compiled destination mapping. An absent key is a successful no-op. The destination MUST NOT
turn hard deletion into a user-visible soft marker merely because its internal engine uses physical
tombstones; ordinary target reads under the declared destination contract must observe logical
absence after settlement.

### Soft

Version 1 soft deletion MUST bind one destination-owned marker field. Preflight MUST prove the
field exists or can be created under the ordinary migration policy, is non-null Boolean, is not a
source/output/key field, and can be written atomically with the package effects.

For an existing matching target row, delete MUST preserve every key and non-marker value and set
the marker to `true`. For an absent target row, delete MUST insert nothing. Repeating the delete is
an idempotent no-op after the marker is already true. Every package upsert MUST write the complete
mapped row and force the marker to `false`, including resurrection after a prior soft delete.

Custom marker values, multiple marker fields, automatic `deleted_at`, application timestamps,
source deletion timestamps without explicit schema authority, and sparse tombstone insertion are
not part of version 1.

## Destination application and capability

Destination sheets MUST separately declare support for keyed upserts, hard deletes, and Boolean
soft deletes under each ingress mode and disposition. Planning MUST reject the selected policy
before package/destination mutation when the destination cannot prove it.

Destinations MAY lower keyed effects to transactions, bulk staging tables, joins, native merge
engines, destination-internal tombstones, copy-on-write publication, object manifests, or another
measured path. The lowering MUST preserve:

- the one-final-effect-per-key package authority;
- complete upsert values and exact typed delete keys;
- selected hard/soft/ignore semantics;
- package-token idempotency and duplicate no-op behavior;
- truthful package/target atomicity or explicitly weaker declared guarantee;
- independent receipt verification;
- no checkpoint advance before the exact receipt is accepted.

Package construction is winner authority. Destinations MUST still reject duplicate or conflicting
effect identities as a corruption/safety check, but MUST NOT apply a private first/last policy.

## Receipts and replay

The current undifferentiated segment acknowledgement and commit count shapes MUST be replaced for
keyed-change packages. A keyed-change receipt MUST contain:

- exact package intent counts: surviving upserts and deletes;
- one exact ordered acknowledgement for every typed effect segment, including effect kind,
  segment id, row count, and byte count;
- exact ignored-delete count under `ignore`;
- optional destination-proven outcome counts for inserts, updates, hard-delete transitions,
  soft-delete transitions, and missing delete keys;
- the ordinary package hash/token, target, disposition, schema/key authority, transaction or
  publication evidence, and independently executable verification clause.

Intent counts are mandatory and derive from verified package identity. Outcome fields MUST be
present only when the destination can prove their stated meaning without an unbounded or
throughput-destroying target scan. `rows_written` MUST NOT ambiguously combine complete upserts and
key-only deletes in the replacement format.

Re-driving the same package token MUST produce no additional logical target mutation and return
the same logical receipt evidence. A later distinct package may legitimately repeat a delete for
an already absent/hard-deleted/soft-deleted key; that effect settles successfully with zero target
transition where the destination reports outcomes.

Verified package replay MUST consume the stored final effect segments and recorded application
policy. It MUST NOT contact the source, re-evaluate capture, re-run reduction, infer hard/soft from
target schema, or change delete application mode.

## Failure behavior

- Delete-capable merge/CDC planning without explicit application policy fails `Contract`.
- Delete effects under append/replace fail `Contract` before extraction or package mutation.
- Missing, null, ambiguous, unsupported, or lossy delete keys fail before effect publication and
  source checkpoint advance.
- Duplicate keys under `fail`, or first/last without compiled authoritative order, fail before
  finalized effect-segment or destination mutation.
- A source claiming enabled/inherent capture that cannot decode a deletion or its key fails the
  package; it MUST NOT silently omit the effect and advance the frontier.
- Unsupported destination policy or incompatible soft marker fails destination preflight.
- An absent hard/soft target key is not an error.
- Any effect-kind/schema/identity mismatch discovered during verification or replay is `Data` and
  blocks destination mutation/checkpoint advancement.

## Required scenarios

1. **Ordinary merge duplicate fails**
   - Given unordered merge rows repeat one key and no explicit ordered winner exists,
   - when package reduction runs,
   - then finalization fails before any effect segment or staged destination segment is durable.

2. **CDC last effect wins across kinds**
   - Given a protocol-ordered upsert then delete for one key,
   - when the CDC package is finalized,
   - then exactly one delete effect survives and the input/reduction counts remain identity
     evidence.

3. **Delete then recreation**
   - Given a delete then complete upsert for one key under authoritative order,
   - when reduction completes,
   - then exactly one upsert survives with the final complete row.

4. **Source deletion capture disabled**
   - Given an optional deletion-feed source with capture disabled,
   - when planning and running succeeds,
   - then no deletion-completeness claim is emitted and every inspect/package evidence surface
     records that limitation.

5. **Captured delete intentionally ignored**
   - Given a verified delete segment and `ignore`,
   - when the destination settles the package,
   - then the target is unchanged, the delete remains in package identity, the receipt acknowledges
     it, and ignored count is exact.

6. **Hard delete absent key**
   - Given a delete for a key absent from the target,
   - when a hard-delete package commits,
   - then commit succeeds, target remains absent, and replay remains a package-token no-op.

7. **Soft delete and resurrection**
   - Given an existing live row and Boolean soft marker,
   - when a delete commits and a later complete upsert commits,
   - then the first package preserves values and marks the row true, while the later package writes
     the complete row and marks it false.

8. **Soft delete absent key**
   - Given no matching target row,
   - when a soft delete commits,
   - then no sparse row is inserted and the effect settles successfully.

9. **Cross-ingress identity**
   - Given the same finalized keyed-change package and application policy,
   - when a finalized-only destination and an equivalent staged-ingress destination consume it,
   - then package/effect identities and logical receipt intent are identical while truthful
     destination-physical evidence may differ.

10. **Jobs/spill invariance**
    - Given mixed upsert/delete duplicates and forced spill,
    - when jobs, batches, memory pressure, and spill crossover vary,
    - then winners, effect schemas/order, package hash, intent counts, and state frontier are
      byte-for-byte identical.

## Acceptance criteria

- One closed package content model represents rows versus keyed changes without optional invalid
  combinations.
- Upsert and delete effects have distinct schemas and typed identities; no `_cdf_op` or invented
  non-key nulls are required in finalized package rows.
- Package-wide exact-key reduction covers both effect families with record-backed winner order and
  bounded spill.
- Source capture and destination application are independent, identity-bearing authorities.
- `ignore`, `hard`, and Boolean `soft` satisfy the exact semantics above and have no implicit
  default.
- Manifest, state, staging, commit, receipt, replay, archive, inspection, and verification paths
  preserve effect kind and fail closed on mismatch.
- Current artifacts are replaced coherently with no compatibility reader, migration, alias, or
  optional legacy field.
- Permanent conformance covers every required scenario and at least one non-CDC optional deletion
  source fixture, one transactional destination, and one non-transactional/copy-on-write or native
  tombstone capability boundary.

## Explicit exclusions

- arbitrary predicate/range/partition/position deletes;
- patch/sparse upserts;
- event-log preservation under `merge` or `cdc_apply`;
- source-selected hard/soft behavior;
- automatic soft-delete timestamps or sparse tombstone insertion;
- cross-resource/cross-target transactions;
- package payload availability, externally recoverable segments, metadata-only packages, and the
  finalized-only versus staged-ingress partial-package lifecycle, which require the next focused
  design after this contract.
