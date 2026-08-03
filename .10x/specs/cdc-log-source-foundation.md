Status: draft
Created: 2026-08-03
Updated: 2026-08-03

# CDC log-source foundation

## Status and ratification boundary

This draft converts the 2026-08-03 core-readiness findings into a proposed shared CDC contract. It
is not active implementation authority. The user has established CDC, PostgreSQL/MySQL log sources,
and MongoDB change streams as product direction; exact position wire shapes, row-image semantics,
and transaction limits below remain recommendations pending ratification and fresh protocol
research.

## Purpose

Define the kernel/runtime foundation that every first-party CDC source MUST reuse so CDF can drain
an ordered change log into immutable packages and advance a checkpoint only at a source-proven
transaction boundary. The foundation extends the existing finite drain-epoch, rolling-spool,
package, receipt, and checkpoint commit gate. It MUST NOT introduce a parallel streaming runtime.

## Scope

- versioned position authority for ordered transaction logs;
- a typed MongoDB resume-token position rather than `ForeignState`;
- position validation, equivalence, reachability, aggregation, and checkpoint transition laws;
- transaction-aligned source safe frontiers;
- canonical CDC operation metadata and `_cdf_op` protection;
- a reusable log-source runtime/conformance archetype;
- package/receipt/checkpoint behavior for finite CDC drain commands;
- destination-facing `cdc_apply` input requirements, without implementing a concrete destination.

## Non-goals

- resident process supervision, scheduling, leader election, or a daemon lifecycle;
- a universal wire protocol, replication client, schema registry, or database log model;
- PostgreSQL slot/publication, MySQL binlog/GTID, or MongoDB change-stream protocol details;
- a first-party `cdc_apply` destination implementation;
- cross-database transaction coordination;
- compatibility readers for pre-production position/checkpoint artifacts unless separately
  ratified;
- hiding first-party positions inside `ForeignState`;
- emitting a checkpoint for a partially observed source transaction.

## Existing authority retained

- `ExecutionExtent::Drain` remains the first CDC execution mode.
- `DrainEpochController` remains the cadence/package-rotation/termination authority.
- package finalization, destination receipts, and checkpoint commit remain the only durable advance
  gate.
- `BatchHeader::cdc` and `CdcMetadata` are extended rather than replaced.
- `WriteDisposition::CdcApply` remains the destination disposition.
- Arrow remains the canonical data representation; source-specific log records are normalized
  before entering the engine.
- the source driver remains a leaf and generic runtime code MUST NOT branch on `postgres`, `mysql`,
  or `mongodb`.

## Position model requirements

### Position categories

CDF MUST distinguish at least these first-party categories:

1. **Ordered log commit position**: a coordinate in a linearly comparable source log that is legal
   only after a complete transaction. PostgreSQL WAL/LSN and MySQL binlog/GTID use this category
   after protocol-specific projection is defined.
2. **Opaque scoped resume token**: a source-issued token whose contents CDF preserves exactly but
   does not order numerically. MongoDB change streams use this category.

Both categories MUST remain versioned `SourcePosition` variants and MUST participate in canonical
serialization and typed artifact validation.

### Ordered log position

The replacement for the current `LogPosition` MUST encode, either directly or through a typed
nested coordinate:

- source protocol/version;
- stable stream identity and scope;
- a protocol-preserving coordinate capable of representing the full native range without signed
  truncation;
- committed transaction-boundary evidence;
- an optional protocol transaction identity only when the source can provide a stable one;
- sufficient information for protocol-specific equality, ordering, and resume.

The persisted form MUST represent only safe restart positions. A `mid_transaction` state MUST NOT
be serializable as a checkpoint/output position. In-progress decoder state is invocation-local
until a transaction commit is observed.

The exact Rust field layout is deliberately not ratified by this draft. Fresh protocol research
MUST determine whether one tagged coordinate enum can losslessly represent PostgreSQL and MySQL or
whether protocol-specific typed variants are safer. The design MUST NOT force MongoDB resume tokens
through numeric ordering.

### MongoDB resume-token position

A first-party MongoDB token position MUST include:

- position artifact version;
- an exact change-stream scope identity derived from deployment, database/collection scope,
  pipeline/options, and any start-mode semantics that change token validity;
- the source-issued resume token in an opaque byte representation with a canonical SHA-256;
- token kind or invalidate/end semantics when required by the official protocol;
- optional cluster/operation time only if official semantics establish it as restart or diagnostic
  authority, never as a guessed total ordering.

CDF MUST validate token hash, scope, and protocol version without parsing undocumented token
internals. Equality/reachability is exact token/scope equality unless official MongoDB semantics
prove a stronger relation. `SourceFrontier` termination by arbitrary future resume token SHOULD be
unsupported because CDF cannot truthfully prove numeric reachability.

### Position algebra

Every new position kind MUST implement and test these operations through one kernel authority:

- `validate(position)` — structural and version correctness;
- `same_scope(left, right)` — whether comparison/advance is meaningful;
- `equivalent(left, right)` — exact restart-authority equality;
- `reaches(observed, target)` — monotone termination comparison where the protocol supports it;
- `join(input, emitted[])` — the canonical non-regressing output/continuation authority;
- `slice_invariance(position, batch split)` — whether a position remains exact after canonical
  segment slicing.

Drain termination and checkpoint aggregation MUST call this shared algebra. They MUST NOT retain
separate pattern matches with subtly different log semantics.

Given an ordered log position sequence in one scope, joining must return the greatest committed
position. Given mixed scopes, incomparable coordinate forms, a regression, or an opaque token set
without an exact terminal token, joining MUST fail before package/checkpoint publication.

## Transaction-aligned epoch law

The central invariant is:

> A package/checkpoint epoch may close only at a source-proven complete transaction boundary, and
> every row admitted before that boundary belongs to a transaction at or before that boundary.

Consequences:

- row, byte, elapsed-time, package-rotation, and command-termination triggers request closure but do
  not manufacture a boundary;
- when a trigger fires mid-transaction, the source continues until the transaction commit and the
  epoch records the deterministic overshoot;
- no later transaction may enter the epoch after closure is requested;
- cancellation/error before commit leaves the prior checkpoint authoritative and discards or
  recovers invocation-local partial transaction work according to the package workspace contract;
- settlement pauses source admission exactly as existing drain epochs require;
- receipt/checkpoint advance uses the same boundary carried by the package and source continuation;
- a retry from the previous committed position may replay a complete transaction, but destination
  idempotency and `cdc_apply` ordering MUST make that replay explicit and safe.

### Large transaction policy

Memory MUST remain bounded even when one source transaction spans many Arrow batches. The source
may stream transaction rows into the existing rolling package workspace, but it cannot settle or
commit the destination before the transaction boundary.

One transaction can exceed the configured package-rotation target. Rotation is therefore a soft
target for CDC and the overshoot MUST be reported. A separate maximum admitted transaction byte/row
policy is required before production CDC. Exceeding that maximum MUST fail closed without advancing
state; silently splitting a transaction across committed checkpoints is forbidden. Whether the
limit is a required authored value or a host policy is unratified.

## Canonical CDC row contract

### Operation field

Every CDC batch MUST carry `CdcMetadata` and one canonical operation field. The default canonical
name is `_cdf_op`; any alternate physical input name MUST be normalized before contract execution.
The field is control-critical and MUST survive projection, schema reconciliation, transforms,
contract evaluation, normalization, package encoding, and destination planning unchanged.

The first operation vocabulary SHOULD be a closed enum encoded as a dictionary or compact integer
with canonical textual explain values:

- `insert`;
- `update`;
- `delete`.

Snapshot/read events and truncate/DDL/schema events MUST NOT be silently mapped to one of these
three. They require explicit later semantics or a typed unsupported-event failure.

`CdcMetadata` MUST validate that:

- the operation field exists exactly once in the materialized Arrow schema;
- its physical encoding and values match the canonical vocabulary;
- its position uses an admitted CDC position kind;
- its position is compatible with the batch/source position and transaction boundary;
- contract/transforms cannot remove or rewrite the field.

### Recommended row-image model

The recommended first contract is:

- insert: complete after-image plus all destination key fields;
- update: complete after-image plus all destination key fields;
- delete: all destination key fields; non-key values are null/absent according to one canonical
  schema rule;
- every event uses the output resource schema, not an adapter-specific envelope.

Sparse patch updates are excluded initially because they require a second presence lattice beyond
Arrow nullability and complicate contracts, merge semantics, and destination verification. Sources
that cannot obtain a complete after-image MUST fail planning or require an explicitly different
future capability.

This row-image recommendation is a semantic blocker and requires user ratification before an
executable source/destination ticket.

### Ordering and keys

- Event order within one transaction and transaction order within one stream MUST be preserved.
- The resource MUST declare a primary/merge key sufficient for update/delete application.
- A key field is control-critical and cannot be quarantined while the event is admitted.
- Schema changes that make a key or operation undecodable MUST stop before the affected transaction
  is checkpointed.
- Multi-partition execution MUST NOT reorder events that share a destination key. Initial CDC SHOULD
  use one ordered source partition per log stream unless a protocol proves safe keyed partitioning.

## Log-source archetype

The shared implementation SHOULD live in a focused source/runtime commons crate or module selected
by dependency-wall evidence. It MUST expose typed mechanics, not a universal database trait.

### Shared state machine

```text
committed checkpoint
→ source-specific open/resume
→ ordered event decode
→ transaction begin/admission
→ accounted Arrow batch emission with CDC metadata
→ transaction commit observed
→ canonical safe frontier published
→ drain controller close/continue decision
→ package finalization
→ destination cdc_apply receipt
→ checkpoint commit
→ next transaction or command termination
```

### Shared responsibilities

- validate initial position kind/scope and select a legal start;
- ensure one ordered transaction admission path;
- publish safe frontiers only after source-specific commit evidence;
- construct/validate CDC batch metadata and operation arrays;
- account emitted rows/bytes and threshold overshoot;
- integrate cancellation, pause-for-settlement, completion, and source continuation;
- classify source errors without discarding adapter provenance;
- provide synthetic deterministic log fixtures and conformance laws.

### Adapter responsibilities

- connect/authenticate and verify deployment prerequisites;
- create/use slots, publications, binlog settings, replica-set/shard streams, or resume tokens;
- decode the native protocol and preserve native position precision;
- map native row/change events and schema changes into the canonical row contract;
- define retryability and token/slot invalidation;
- report topology/retention gaps that make resume impossible;
- prove protocol-specific transaction-boundary evidence.

## Destination-facing `cdc_apply` requirements

Before any destination advertises `cdc_apply`, its sheet and implementation MUST prove:

- ordered application of canonical operation rows;
- transactional or otherwise truthful package/transaction atomicity;
- deterministic key validation and duplicate handling;
- package/segment token idempotency and replay behavior;
- delete count and update/insert count receipt evidence where the backend can observe it;
- independent receipt verification against destination state;
- failure behavior for missing targets, schema drift, unsupported operations, and ambiguous commit;
- no checkpoint advance until the destination receipt covers the exact transaction-aligned frontier.

`cdc_apply` is not equivalent to existing `merge`: it includes deletes and preserves ordered source
change semantics. A destination MAY lower it to a maximally efficient native mutation/bulk protocol
only when the resulting receipt and replay guarantees remain truthful.

## Artifact version transition

The first implementation ticket MUST inventory and update every artifact that embeds source
positions. At minimum:

- `SOURCE_POSITION_VERSION` and canonical source-position JSON;
- `CHECKPOINT_STATE_VERSION`, state delta, segments, continuation, and carryover;
- `CHECKPOINT_STORE_SCHEMA_VERSION` and SQLite CHECK constraints/row validation;
- package state preimage and manifest identities;
- declarative source-position declarations;
- portable worker/task position encoding and hashes;
- golden fixtures, examples, system-SQL decoding, inspect/status rendering, and conformance.

Recommended pre-production policy: replace the current schema coherently, fail closed on old
versions, and do not add legacy readers/migrations. This recommendation requires ratification.

## Failure behavior

- Mid-transaction cancellation, decode failure, schema failure, or hook failure advances no
  checkpoint.
- Missing/expired WAL, binlog, or resume-token authority is Data or Environment according to the
  adapter's source-proven cause; it MUST retain protocol provenance and remediation.
- Position scope mismatch, regression, or impossible aggregation is Data if observed from durable
  source/artifact state and Contract if authored configuration requests an unsupported frontier.
- An unsupported source event fails before destination mutation; it is not silently dropped or
  quarantined when doing so would break transaction completeness.
- A destination unable to apply or verify the exact operation contract fails before checkpoint
  commit.

## Acceptance scenarios

1. **Mid-transaction cadence:** Given a row/byte/timer threshold fires halfway through a
   transaction, when the source reaches commit, then exactly one closure occurs at the commit
   position and overshoot is recorded; no mid-transaction checkpoint exists.
2. **Crash before commit observation:** Given rows were spooled but the source commit was not
   observed, when the process restarts, then the prior checkpoint is used and the partial
   transaction has no committed destination/checkpoint effect.
3. **Crash after destination commit:** Given the destination committed and receipt recovery is
   possible but checkpoint publication crashed, when resume runs, then generic receipt/package
   recovery commits the same frontier without reopening the source.
4. **Advancing log aggregation:** Given multiple batches from transactions at positions P1..Pn in
   one epoch, aggregation returns Pn and never the equality-only fallback.
5. **Mixed stream rejection:** Given positions from different slot/binlog/stream scopes, join and
   reachability fail before package finalization.
6. **Opaque token:** Given a MongoDB token, CDF round-trips its bytes and scope exactly, detects
   tampering, and never claims numeric ordering from token internals.
7. **Control-field protection:** Given a projection, contract, or hook tries to remove or modify
   `_cdf_op` or a key field, planning/execution fails before package publication.
8. **Large transaction:** Given a transaction crosses ordinary package rotation while within the
   admitted maximum, memory stays bounded, closure waits for commit, and the package records
   overshoot. Given it crosses the maximum, the run fails with no state advance.
9. **Unsupported event:** Given a truncate/DDL or incomplete update image, the adapter fails with
   typed remediation rather than inventing insert/update/delete semantics.
10. **Destination replay:** Given the same finalized package is applied twice, the destination
    returns/verifies the same receipt and no operation is applied twice.

## Required conformance families

- position serialization/tamper/scope/ordering/aggregation properties;
- synthetic log transaction model with randomized batch and epoch boundaries;
- cancellation and failure injection before/after every state-machine transition;
- package/receipt/checkpoint crash recovery;
- operation/key control-field preservation through every operator stage;
- adapter-specific live protocol tests;
- destination `cdc_apply` replay/verification tests;
- memory and transaction-overshoot envelope evidence.

## Open blockers

1. Exact ordered-log coordinate/transaction Rust shape after PostgreSQL/MySQL protocol research.
2. Exact MongoDB token encoding and scope fields after official-driver research.
3. User ratification of complete after-image updates and key-only deletes.
4. User ratification of current-schema replacement versus legacy artifact readers.
5. Maximum transaction policy ownership and default.
6. First destination(s) authorized to implement `cdc_apply`.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/stream-epochs-watermarks.md`
- `.10x/decisions/kernel-owned-stream-epoch-policy.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/tickets/cancelled/2026-07-05-cdc-and-streaming-supervisor.md`
- `VISION.md` §§6.5, 13.3, 25.3 and D-8/D-16
