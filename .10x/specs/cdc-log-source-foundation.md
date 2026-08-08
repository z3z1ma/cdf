Status: active
Created: 2026-08-03
Updated: 2026-08-07

# CDC log-source foundation

## Status and ratification boundary

This active contract converts the 2026-08-03 core-readiness findings and official protocol
research into the shared CDC runtime contract. Position/artifact, complete-image, package-native
keyed-effect, large-transaction, destination, bootstrap, and continuous-run boundaries are now
ratified. MongoDB event-prefix segmentation is not transaction-grouped.

## Purpose

Define the kernel/runtime foundation that every first-party CDC source MUST reuse so CDF can drain
changes into immutable packages and advance a checkpoint only at a source-proven safe frontier:
a committed transaction boundary for PostgreSQL/MySQL, or a receipt-covered event-prefix resume
token for MongoDB. The foundation extends the existing finite drain-epoch, rolling-spool, package,
receipt, and checkpoint commit gate. It MUST NOT introduce a parallel streaming runtime.

## Scope

- versioned position authority for ordered transaction logs;
- a typed MongoDB resume-token position rather than `ForeignState`;
- position validation, equivalence, reachability, aggregation, and checkpoint transition laws;
- transaction-aligned source safe frontiers;
- canonical CDC operation metadata, control protection, and lowering into package-native keyed
  effects governed by `.10x/specs/package-keyed-delete-effects.md`;
- a reusable log-source runtime/conformance archetype;
- package/receipt/checkpoint behavior for finite CDC drain epochs;
- destination-facing `cdc_apply` input requirements, with PostgreSQL and DuckDB as the first
  implementations.

## Non-goals

- daemon/service supervision, leader election, or a resident operator-graph lifecycle; the
  process-local loop over finite drain epochs is governed by
  `.10x/specs/cdc-resource-authoring-and-continuous-run.md`;
- a universal wire protocol, replication client, schema registry, or database log model;
- PostgreSQL slot/publication, MySQL binlog/GTID, or MongoDB change-stream protocol details;
- a first-party `cdc_apply` destination implementation;
- cross-database transaction coordination;
- compatibility readers or migrations for pre-production position/checkpoint artifacts;
- hiding first-party positions inside `ForeignState`;
- emitting a checkpoint for a partially observed source transaction.

## Existing authority retained

- `ExecutionExtent::Drain` remains the first CDC execution mode.
- `DrainEpochController` remains the cadence/package-rotation/termination authority.
- package finalization, destination receipts, and checkpoint commit remain the only durable advance
  gate.
- `BatchHeader::cdc` and `CdcMetadata` remain the source/runtime operation and position authority;
  their exact current pre-production shape MAY be replaced to support typed keyed-effect lowering.
- `WriteDisposition::CdcApply` remains the destination disposition.
- Arrow remains the canonical data representation; source-specific log records are normalized
  before entering the engine.
- the source driver remains a leaf and generic runtime code MUST NOT branch on `postgres`, `mysql`,
  or `mongodb`.

## Position model requirements

### Position categories

CDF MUST distinguish at least these first-party categories:

1. **Ordered log commit position**: a protocol-specific coordinate in a linearly comparable source
   log that is constructible only after a complete transaction. PostgreSQL WAL/LSN and MySQL
   binlog/GTID use separate typed variants in this category.
2. **Opaque scoped resume token**: a source-issued token whose contents CDF preserves exactly but
   does not order numerically. MongoDB change streams use this category.

Both categories MUST remain versioned `SourcePosition` variants and MUST participate in canonical
serialization and typed artifact validation.

### Ordered log position

The replacement for the current `LogPosition` MUST be a closed `CommittedLogPosition` enum with
`PostgreSql(PostgresCommitPosition)` and `MySql(MySqlCommitPosition)` variants. It MUST NOT use a
protocol string plus generic offset.

`PostgresCommitPosition` MUST contain version, typed scope, `commit_lsn: u64`, `end_lsn: u64`, and
`xid: u32`. Its scope MUST bind cluster system identifier, database OID, logical slot, output
plugin, and a canonical semantics hash over sorted publications and every behavior-changing
`pgoutput` option. `end_lsn` is restart/order authority; commit LSN and XID corroborate the boundary.
The first version MUST reject two-phase decoding and persist only normal/streamed commit messages.

`MySqlCommitPosition` MUST contain version, typed scope, native binlog filename, parsed numeric file
sequence, terminal commit-event `end_log_position: u64`, canonical executed GTID set, and the
transaction GTID. Its scope MUST bind the CDF deployment, active server UUID, binlog basename, and
a canonical capture-semantics hash. The first version MUST require ROW/FULL logging, GTID mode ON,
and GTID consistency. Filename ordering is legal only inside one validated server/binlog lineage;
cross-server failover is an explicit rebind until separately proven.

The persisted form MUST represent only safe restart positions. A `mid_transaction` state MUST NOT
be serializable as a checkpoint/output position. In-progress decoder state is invocation-local
until a transaction commit is observed.

Persisted log variants represent a committed boundary by construction. A `tx_boundary: bool` MUST
NOT be added because it would make an illegal false checkpoint serializable. In-progress decoder
and transaction state remains invocation-local or in accounted replay storage until commit.

### MongoDB resume-token position

A first-party `ResumeTokenPosition::MongoChangeStream` MUST include:

- position artifact version;
- typed scope containing the CDF deployment binding, watch level, namespace target when applicable,
  canonical pipeline SHA-256, and semantics-changing options SHA-256;
- the exact BSON token document bytes encoded as canonical base64 plus SHA-256;
- `resume_mode: ResumeAfter | StartAfter`, because invalidate tokens cannot use `resumeAfter`;
- `token_source: Event | PostBatch`, because post-batch tokens can advance the scanned prefix even
  when no matching event is returned.

CDF MUST validate base64, token hash, scope, version, and mode without parsing undocumented token
internals. Equality/reachability is exact token/scope/mode equality. `SourceFrontier` termination
by arbitrary future resume token MUST be unsupported because CDF cannot truthfully prove numeric
reachability. An adapter MAY publish an explicitly ordered terminal token from its own retained
event prefix; generic set aggregation MUST NOT guess which opaque token is later.

The public MongoDB change-stream contract identifies transaction events with `lsid` and
`txnNumber` but does not expose a documented transaction-end marker. Mongo CDC MUST therefore use
event-prefix semantics: accumulate ordered changes into segments/packages, retain the terminal
event/post-batch token as the proposed frontier, deliver the package, and advance that token only
after the exact destination receipt is accepted. It MUST NOT group by source transaction or inspect
undocumented token internals.

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

For PostgreSQL/MySQL ordered logs, the central invariant is:

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
target for CDC and the overshoot MUST be reported. `transaction_limit_bytes` MUST be a mandatory
compiled CDC capability bounded by host spill/replay policy. A project/resource MAY lower it but
MUST NOT raise it above host authority. Byte count is the hard resource limit; row count is
telemetry. Exceeding the bound MUST fail closed without advancing state. The kernel MUST NOT invent
a universal numeric default; the host profile supplies the concrete deterministic bound carried by
the compiled plan.

## Canonical CDC change contract

### Transient operation authority

Every CDC batch MUST carry `CdcMetadata` and one canonical row-level operation authority. A source
physical field MAY be normalized to an internal `_cdf_op` representation, but the operation is
runtime control rather than a field in the finalized logical destination row schema. It MUST
survive projection, schema reconciliation, transforms, contract evaluation, and normalization
unchanged until the package keyed-effect reduction consumes it.

The first operation vocabulary SHOULD be a closed enum encoded as a dictionary or compact integer
with canonical textual explain values:

- `insert`;
- `update`;
- `delete`.

Snapshot/read events and truncate/DDL/schema events MUST NOT be silently mapped to one of these
three. They require explicit later semantics or a typed unsupported-event failure.

The source/runtime authority MUST validate that:

- every admitted row has exactly one operation value in the canonical vocabulary;
- its physical/internal encoding is typed and unambiguous;
- its position uses an admitted CDC position kind;
- its position is compatible with the batch/source position and transaction boundary;
- contract/transforms cannot remove or rewrite the field.

Finalized package segments MUST NOT persist `_cdf_op` beside full destination rows. Package
construction lowers insert/update into complete `upsert` effects and delete into key-only `delete`
effects under `.10x/specs/package-keyed-delete-effects.md`.

### Row-image input to keyed effects

The recommended first contract is:

- insert: complete after-image plus all destination key fields, lowered to `upsert`;
- update: complete after-image plus all destination key fields, lowered to `upsert`;
- delete: all destination key fields, lowered to the mechanically derived key-only `delete`
  schema without invented non-key values;
- source-specific envelopes end at the adapter/runtime boundary and do not enter finalized package
  or destination schemas.

Sparse patch updates are excluded initially because they require a second presence lattice beyond
Arrow nullability and complicate contracts, merge semantics, and destination verification. Sources
that cannot obtain a complete after-image MUST fail planning or require an explicitly different
future capability.

Protocol consequences are explicit:

- MySQL MUST require ROW/FULL logging and is the recommended first end-to-end CDC proof.
- PostgreSQL MUST use a consistent-snapshot-seeded row materializer to reconstruct unchanged
  TOASTed values; post-commit lookups are not exact.
- MongoDB MUST require server 6.0+, collection post-images, and
  `fullDocument: "required"`; `updateLookup` is not exact.
- MongoDB `replace` maps to update. Truncate/DDL and missing images/keys fail before checkpoint.

This complete-after-image/key-only-delete contract and its package-native lowering were
user-ratified on 2026-08-03.

### Ordering and keys

- Event order within one transaction and transaction order within one stream MUST be preserved.
- The resource MUST declare a primary/merge key sufficient for update/delete application.
- A key field is control-critical and cannot be quarantined while the event is admitted.
- Schema changes that make a key or operation undecodable MUST stop before the affected transaction
  is checkpointed.
- Multi-partition execution MUST NOT reorder events that share a destination key. Initial CDC SHOULD
  use one ordered source partition per log stream unless a protocol proves safe keyed partitioning.
- Package finalization MUST reduce the ordered event sequence to one last effect per exact typed key
  and retain identity-bearing input/reduction evidence; consumers that require intermediate events
  use an ordinary append resource.

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
- map native row/change events and schema changes into the canonical complete-image/key operation
  contract;
- define retryability and token/slot invalidation;
- report topology/retention gaps that make resume impossible;
- prove protocol-specific transaction-boundary evidence.

## Destination-facing `cdc_apply` requirements

Before any destination advertises `cdc_apply`, its sheet and implementation MUST prove:

- exact application of the package's final keyed upsert/delete effects;
- transactional or otherwise truthful package/transaction atomicity;
- deterministic exact-key validation and proof that package construction selected at most one
  effect per key;
- package/segment token idempotency and replay behavior;
- exact package intent counts plus delete/update/insert outcome evidence where the backend can
  observe it truthfully and economically;
- independent receipt verification against destination state;
- failure behavior for missing targets, schema drift, unsupported operations, and ambiguous commit;
- no checkpoint advance until the destination receipt covers the exact committed-log or opaque
  event-prefix frontier.

`cdc_apply` and `merge` share the package-native keyed-effect handoff. CDC remains distinct because
source protocol order mandates last-change-wins and its receipt gates a log/resume-token frontier;
ordinary unordered merge duplicates fail unless an explicit authoritative winner rule exists. A
destination MAY lower either to a maximally efficient native mutation/bulk protocol only when the
resulting delete application, receipt, and replay guarantees remain truthful.

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

Because CDF is net-new and customer zero, the artifact schemas MUST be replaced coherently, fail
closed on old versions with direct remediation, update every fixture/renderer/hash, and add no
legacy readers or migrations. A partial transition is forbidden. This policy was user-ratified on
2026-08-03.

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
   Given accumulated Mongo changes are segmented and delivered, the terminal token advances only
   after a receipt for the exact segment/package is accepted; no transaction grouping is required.
7. **Control-field protection and lowering:** Given a projection, contract, or hook tries to remove
   or modify the transient operation authority or a key field, planning/execution fails before
   package publication. Given valid ordered changes, finalized package content contains typed
   upsert/delete effects and no `_cdf_op` destination column.
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

## Ratified execution choices

- The resolved host spill budget is the hard maximum PostgreSQL/MySQL single-transaction byte
  authority. A resource may only lower it. Exceeding it advances no checkpoint.
- PostgreSQL and DuckDB are the first `cdc_apply` destinations. Delete application is explicit
  `HARD`, `IGNORE`, or Boolean-marker `SOFT` with no default, as governed by the focused authoring
  spec.
- First-use CDC explicitly chooses integrated `snapshot` or source-proven `latest`; no bootstrap
  default exists.
- `cdf run` provides continuous behavior by repeating settled finite drain epochs. First interrupt
  settles the next safe frontier; second interrupt aborts unfinished work without checkpoint
  advance.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/research/2026-08-03-cdc-protocol-position-contract.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/specs/stream-epochs-watermarks.md`
- `.10x/decisions/kernel-owned-stream-epoch-policy.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/cdc-resource-authoring-and-continuous-run.md`
- `.10x/specs/retention-aware-package-collection.md`
- `.10x/tickets/cancelled/2026-07-05-cdc-and-streaming-supervisor.md`
- `VISION.md` §§6.5, 13.3, 25.3 and D-8/D-16
