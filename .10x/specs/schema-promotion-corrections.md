Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Schema promotion and destination corrections

## Purpose and scope

This specification governs explicit promotion of retained `_cdf_variant` residual paths into typed schema fields and correction of rows previously committed by CDF. It refines package, destination, and checkpoint specifications while preserving plan-first execution, immutable packages, receipts, and the commit gate.

## Command contract

The command family is:

```text
cdf schema promote RESOURCE [--type JSON_POINTER=ARROW_TYPE ...] [--execute]
```

The default is dry planning. Dry planning MAY inspect verified retained packages, fresh schema discovery, pinned snapshots, destination sheets, and checkpoint/receipt evidence, but MUST NOT write snapshots, lockfiles, packages, destinations, checkpoints, leases, or run-ledger events.

The plan lists residual paths, observed type set/counts, proposed target types, affected packages/rows/targets, correction strategy per target, retained-evidence availability, migrations, conflicts, and exact recovery command. An unambiguous compatible type from fresh discovery MAY be proposed automatically. Ambiguous or lossy cases require explicit `--type`; lossy promotion also requires the existing lossy allowance.

`--execute` is refused unless every selected path has one target type, every affected target has a safe correction strategy, required residual evidence is readable, and no current destination mapping is unsupported.

## Stable row provenance

The logical address of a CDF-loaded row is:

```text
(original_package_hash, original_segment_id, original_row_ordinal)
```

Relational destinations persist this as the reserved tuple `_cdf_load`, `_cdf_segment`, `_cdf_row`. `_cdf_load` is the original package/idempotency token, `_cdf_segment` is the canonical package segment id, and `_cdf_row` is the zero-based row ordinal within that segment. This operational identity is not a semantic merge key and MUST NOT make append require a user key.

Corrections MUST reference the original address even when delivered in a later correction package. Destinations MUST enforce or verify uniqueness of the tuple before declaring in-place correction support. Replaying the same original package or correction package MUST preserve the address and remain idempotent.

## Destination capabilities and strategies

Destination sheets MUST add versioned correction capabilities declaring:

- whether row provenance is persisted and targetable;
- whether canonical `_cdf_variant` can be read back;
- supported strategies: `in_place_update`, `correction_sidecar`, `versioned_rematerialization`;
- transaction/idempotency guarantees for each strategy.

Strategy selection is plan-time and serialized.

- `in_place_update` updates only rows addressed by the original provenance tuple inside the destination's atomic package transaction.
- `correction_sidecar` writes an immutable addressed delta containing provenance plus promoted fields; it MUST NOT pretend the base target was mutated.
- `versioned_rematerialization` writes a complete new target version from verified packages/residuals and atomically advances a destination-owned target pointer or manifest only after receipt verification.

If no safe strategy exists, planning fails with the unsupported capability and available remediation. Arbitrary UPDATE predicates, inferred business keys, or unrecorded target rewrites are forbidden.

## Promotion lease and transaction order

Execution acquires an exclusive fenced lease for the resource's schema-contract scope. The lease abstraction is kernel/store-level and uses the existing `ScopeKey::SchemaContract`; it MUST be implementable by local SQLite/in-memory stores and future Postgres/object-store stores without CLI or filesystem semantics in the model.

Under the lease, execution follows this order:

1. Verify the current pinned snapshot/lock hash equals the dry plan's input authority.
2. Write the proposed snapshot and promotion-plan artifacts to content-addressed staged paths without changing `cdf.lock`.
3. Build immutable correction/rematerialization packages referencing the old/new schema hashes, residual source packages, original row addresses, and selected strategy.
4. Commit each package through the ordinary destination session, independently verify its receipt, and commit its checkpoint under the promotion/schema-contract scope.
5. After every required target checkpoint is committed, atomically compare-and-swap `cdf.lock` from the recorded old bytes/hash to the exact staged snapshot reference.
6. Append or verify the idempotent promotion-publication ledger event keyed by promotion-plan id, then release the lease.

The lockfile write MUST use temporary-file, fsync where supported, and atomic rename-over semantics. A concurrent lockfile change causes a named conflict and MUST NOT be overwritten.

Crash recovery is evidence-driven:

- before packaged correction: discard/rebuild staged work;
- packaged without receipt: replay package;
- receipt without checkpoint: verify receipt then commit checkpoint;
- all target checkpoints committed but lock not advanced: verify the staged plan and compare-and-swap the lock;
- lock advanced but publication event absent: append the idempotent event from the verified staged plan/checkpoints without touching destinations;
- lock and publication event agree: promotion is complete and replay is a no-op.

Lease fencing prevents an expired executor from advancing the lock or promotion checkpoint. Promotion is reported complete only when the lock reference and publication event agree; their narrow crash window is recoverable from the verified plan/checkpoints rather than falsely claimed atomic across Git working-tree bytes and the ledger. This is not distributed 2PC: destinations settle independently, and the final lock advance publishes schema authority after their verified checkpoints.

## Retention and availability

Promotion does not create indefinite retention. Its planner reports each residual source as:

- `retained_package`: exact canonical package bytes available;
- `destination_readback`: the sheet and verification probe can reproduce canonical residual envelopes and row addresses;
- `tombstone_only`: hashes/manifest remain but value bytes are gone;
- `missing`: required evidence cannot be verified.

`tombstone_only` and `missing` cannot drive correction. The CLI names source re-extraction or versioned rebuild when possible.

`cdf package gc` MUST report when collection removes the last locally retained promotable residual bytes. It need not retain them beyond configured policy, and it MUST NOT claim destination readback without a verified destination capability.

## Scenarios

Given retained packages and a Postgres target with unique provenance tuples, when promotion executes, then addressed rows update atomically, correction receipts/checkpoints commit, and only then does the lockfile pin advance.

Given a crash after destination receipt but before checkpoint, when promotion resumes, then it verifies the receipt, commits the checkpoint, and never rewrites destination rows blindly.

Given an append-only target without addressable updates, when its sheet declares correction sidecars, then the plan writes an immutable delta and reports that the base target is unchanged.

Given only tombstones remain, when promotion is planned, then execution is refused with exact unavailable evidence and remediation.

## Acceptance criteria

- Dry-run performs zero writes and renders the complete correction plan.
- Lease fencing and lockfile compare-and-swap tests prevent stale/concurrent publication.
- Crash tests cover every lifecycle boundary above.
- Destination conformance falsifies incorrect correction/readback claims.
- Append resources require no semantic key.
- Old packages remain byte-immutable and replayable.

## Explicit exclusions

This specification does not define arbitrary user-authored UPDATE SQL, cross-resource migrations, indefinite evidence retention, automatic promotion, or a distributed scheduler.
