Status: done
Created: 2026-08-03
Updated: 2026-08-03

# CDC protocol position and row-image contract research

## Question

What exact, source-proven position, transaction-boundary, resume-token, and row-image facts can CDF
use for PostgreSQL logical replication, MySQL row-based binary logs, and MongoDB change streams?
Which facts are comparable in the kernel, which must remain opaque, and which product semantics
still require ratification before the source-position artifact can change?

## Sources and methods

Research was performed on 2026-08-03 against live CDF source at revision `3487de68` and current
official protocol/server/driver documentation. The investigation was read-only; no database was
contacted and no protocol fixture was captured. CDF source inspected included:

- `crates/cdf-kernel/src/position.rs` and `position_aggregation.rs`;
- `crates/cdf-kernel/src/checkpoint.rs`, `batch.rs`, and `execution_extent.rs`;
- `crates/cdf-runtime/src/drain_epoch.rs`, `source_frontier.rs`, and `rolling_replay.rs`;
- `.10x/specs/cdc-log-source-foundation.md` and its program owner.

Official sources:

- PostgreSQL [logical replication message formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html),
  [streaming replication protocol](https://www.postgresql.org/docs/current/protocol-replication.html),
  [logical streaming parameters](https://www.postgresql.org/docs/current/protocol-logical-replication.html),
  and [`pg_replication_slots`](https://www.postgresql.org/docs/current/view-pg-replication-slots.html);
- MySQL 8.4 [binary log overview](https://dev.mysql.com/doc/refman/8.4/en/binary-log.html),
  [C API binary-log data structures](https://dev.mysql.com/doc/c-api/8.4/en/c-api-binary-log-data-structures.html),
  [GTID format and storage](https://dev.mysql.com/doc/refman/8.4/en/replication-gtids-concepts.html),
  [row-image settings](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html),
  and [transaction statements](https://dev.mysql.com/doc/refman/8.4/en/commit.html);
- MongoDB [change streams](https://www.mongodb.com/docs/manual/changestreams/),
  [`$changeStream` options](https://www.mongodb.com/docs/manual/reference/operator/aggregation/changestream/),
  [change events](https://www.mongodb.com/docs/manual/reference/change-events/), and the accepted
  [driver change-stream specification](https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md).

## Existing CDF facts

- `SourcePosition::Log(LogPosition)` is currently `{ version, log: String, offset: i64,
  sequence: Option<String> }`. It does not name a protocol or scope, truncates protocols whose
  native coordinate is unsigned, and does not prove a legal transaction boundary.
- non-cursor/non-file/non-snapshot aggregation is equality-only. Two advancing log positions or
  resume tokens therefore fail with “divergent segment source positions”; there is no shared
  scope/equality/reachability/join algebra.
- `ForeignState` preserves opaque bytes and a SHA-256 but is intentionally source-neutral and has
  no change-stream scope or resume-mode semantics.
- drain epochs already separate closure requests from settlement and retain exact source frontiers,
  but they do not know whether a source frontier is transaction-safe.
- rolling replay already provides bounded, accounted, source-encoded disk retention and exact
  checkpoint-frontier eviction. It is the correct storage primitive for large in-flight source
  units; CDC does not need an unbounded in-memory transaction buffer.

## PostgreSQL findings

### Native position and boundary

- `XLogRecPtr`/LSN values are 64-bit WAL byte addresses. They must be represented as `u64`, not the
  current signed `i64` offset.
- a normal `Begin` message carries the transaction's final LSN and XID. The matching `Commit`
  carries both the commit LSN and the transaction end LSN. Streamed transactions have an explicit
  `Stream Commit` with XID, commit LSN, and end LSN. Aborts have distinct messages.
- the safe restart/acknowledgement frontier is the end LSN (the byte after the processed WAL
  prefix), while commit LSN and XID are retained as corroborating transaction evidence. CDF must
  not publish a position from `Begin`, `Stream Stop`, or row messages.
- receiver status messages acknowledge write/flush/apply locations as “last WAL byte + 1”. A
  logical `START_REPLICATION` begins at the greater of the requested LSN and the slot's
  `confirmed_flush_lsn`; a client should compare the slot value to its own checkpoint before
  starting because the server can legally advance it.
- `IDENTIFY_SYSTEM` supplies a cluster system identifier. Logical slots are database-specific and
  bind an output plugin. `pgoutput` behavior also depends on protocol version, publication names,
  binary/text mode, streaming, two-phase, messages, and origin options. These facts belong in the
  stream scope or its canonical semantics hash.

### Initial supported subset

The first adapter should use ordinary committed and streamed committed transactions, with
`two_phase = false`. Prepared-transaction positions require a separate state model because prepare
is durable but not a destination-visible commit. Treating prepare as commit would violate CDF's
checkpoint law.

### Row images

`pgoutput` sends a new tuple for updates and replica-identity/old tuple data when configured, but a
TupleData column may be `u`, meaning an unchanged TOASTed value whose bytes are not sent. Therefore
the wire update is not always a complete Arrow after-image. `REPLICA IDENTITY FULL` does not by
itself eliminate the need to reconstruct unchanged values. Exact complete after-images require a
stateful row materializer seeded by the consistent initial snapshot (or a future explicit patch
presence lattice). A post-commit table lookup is not exact because later commits may already have
changed or deleted the row.

## MySQL findings

### Native position and boundary

- the binary log is an ordered sequence of files with a base name and numeric extension. The
  native reader starts from `(file_name, start_position)`; the first event begins at byte 4.
  Event metadata exposes `End_log_pos`, the position at which the next event begins.
- transactions are written to the binary log as one chunk at commit, in commit order. An XID event
  identifies commit for transactional engines; explicit/implicit COMMIT query events cover other
  legal transaction forms. The safe checkpoint is the end position of the terminal commit event,
  never a row-event end position.
- GTIDs are transaction identities, not a single global scalar. A GTID is source UUID, optional
  MySQL 8.4 tag, and positive signed-64 transaction sequence. A GTID set may contain multiple
  origins and disjoint ranges; reachability is set inclusion, while observed delivery order is the
  binlog `(file sequence, end position)`.
- a checkpoint intended to support exact reconnect on the same server and future controlled
  failover must retain both the binlog coordinate and the canonical executed GTID set. A lone last
  GTID is insufficient for multiple origins or disjoint history.

### Initial supported subset

The first adapter should require `binlog_format = ROW`, `binlog_row_image = FULL`, `gtid_mode = ON`,
and `enforce_gtid_consistency = ON`. This excludes anonymous transactions and sparse/nondeterministic
row images from the initial contract. Cross-server failover should remain an explicit rebind until
live fixtures prove GTID-set continuation and topology identity; filename comparison is valid only
inside one validated server/binlog lineage.

### Row images

With `binlog_row_image = FULL`, row-based update events provide full before and after images;
deletes provide the before image. CDF can emit the complete after-image for inserts/updates and
only the declared destination key for deletes. `MINIMAL`, `NOBLOB`, statement, and mixed logging
must fail the initial CDC capability probe rather than invent absent values.

## MongoDB findings

### Token authority

- every change event `_id` is a BSON document used as an opaque resume token. Its internal fields
  vary by server and feature-compatibility version; CDF must not parse them for order.
- aggregate/getMore responses can provide `postBatchResumeToken`. It represents the oplog prefix
  scanned by the server, even when no matching event was returned, and the driver specification
  requires caching it in specific empty/last-document cases. Persisting only the last event `_id`
  can cause expensive rescans or make resume impossible after oplog truncation.
- `resumeAfter` starts after an ordinary token. It cannot continue after an `invalidate` event;
  `startAfter` can. The required resume mode is therefore part of checkpoint authority, not merely
  invocation state.
- resume requires the same pipeline and options. Scope must bind the CDF deployment/source binding,
  watch level and namespace, canonical pipeline identity, and semantic change-stream option
  identity. Secrets and transient cursor/server-selection state must not enter that hash.
- exact BSON document bytes should be stored as canonical base64 with a SHA-256. A parsed JSON value
  is not lossless for BSON types, integer widths, binary subtypes, or document bytes.

### Transaction boundary limitation

Change events identify operations in a multi-document transaction with `lsid` plus `txnNumber`, and
the source transaction is already majority-committed before events are visible. However, the public
change-stream event contract does not expose a transaction-end event or documented last-operation
flag. A resume token is a source-proven safe restart point after an event, but official documents
inspected do not prove that CDF can group all events from one source transaction into one package
without relying on undocumented token internals or server implementation details.

Consequently the kernel must distinguish:

- **ordered committed-log positions**, which prove source transaction-aligned boundaries; and
- **opaque resume-token positions**, which prove server-supported restart after a scanned/event
  prefix but do not claim numeric ordering or source-transaction package atomicity.

MongoDB CDC may truthfully provide event-level at-least-once replay with package atomicity. If CDF
requires preservation of source multi-document transaction atomicity at the destination, the Mongo
CDC adapter remains blocked until a documented boundary proof or a raw-oplog design exists.

### Row images

`fullDocument: "updateLookup"` is not exact: it returns a later majority-committed version and can
be missing. MongoDB 6.0+ with collection pre/post-images enabled and
`fullDocument: "required"` returns the document as it appeared immediately after the change and
fails when unavailable. That is the only documented mode compatible with CDF's proposed complete
after-image update contract. Deletes carry `documentKey`; CDF must prove it contains every declared
destination key field. This exactness has source storage/processing cost and should be explicit in
the adapter capability sheet.

## Recommended exact kernel shape

Use protocol-specific committed position variants instead of a generic string/offset bag:

```rust
enum SourcePosition {
    // existing variants ...
    Log(CommittedLogPosition),
    ResumeToken(ResumeTokenPosition),
}

enum CommittedLogPosition {
    PostgreSql(PostgresCommitPosition),
    MySql(MySqlCommitPosition),
}
```

`PostgresCommitPosition` should contain version, typed scope, `commit_lsn: u64`,
`end_lsn: u64`, and `xid: u32`. Its scope should contain system identifier, database OID, slot,
output plugin, and a canonical hash over sorted publication names and all semantics-changing
protocol options. `end_lsn` is ordering/restart authority; commit LSN and XID corroborate the
boundary but are not independent ordering axes.

`MySqlCommitPosition` should contain version, typed scope, native binlog file, parsed numeric file
sequence, commit-event `end_log_position: u64`, canonical executed GTID set, and the transaction
GTID. Its scope should contain the CDF deployment binding, active server UUID, binlog basename, and
a canonical hash over prerequisite/capture semantics. Same-server ordering uses file sequence plus
end position; GTID-set inclusion proves non-regression. Active-server changes are an explicit
rebind, not an implicit same-scope comparison in the first version.

`ResumeTokenPosition` should contain version, `MongoChangeStreamScope`, canonical BSON bytes encoded
as base64, SHA-256, `resume_mode: ResumeAfter | StartAfter`, and
`token_source: Event | PostBatch`. The scope should contain the CDF deployment binding, watch level,
database/collection target where applicable, pipeline SHA-256, and semantics-changing options
SHA-256. Equality is exact scope/mode/token equality; arbitrary ordering and future-target
reachability are unsupported.

Persisted log variants represent committed boundaries by construction. Do not add a
`tx_boundary: bool` that permits serializing `false`; in-progress transaction state belongs only in
the adapter invocation/replay workspace.

## Recommended algebra

- `validate`: exact version, native range, canonical name/hash/token/GTID syntax, and protocol
  invariants.
- `same_scope`: exact typed scope equality. PostgreSQL timeline is not scope because logical
  failover can preserve system identity/slot; MySQL active-server change is not same scope in v1.
- `equivalent`: exact restart authority equality within one scope.
- `reaches`: PostgreSQL compares `end_lsn`; MySQL compares ordered same-lineage binlog coordinates
  and requires non-regressing GTID-set inclusion; MongoDB supports equality only.
- `join`: select the greatest committed PostgreSQL/MySQL position in one scope while rejecting
  regression/incompatible evidence. MongoDB must use an explicitly ordered terminal frontier from
  the source; unordered set aggregation must not guess which opaque token is later.
- `slice_invariance`: a committed transaction/replay unit position is slice-invariant only while
  all canonical slices remain one settlement unit. MongoDB event/post-batch tokens must follow the
  same retained-prefix rule rather than being copied onto independently committable prefixes.

## Recommended row and resource policy

- canonical operations: insert, update, delete; map Mongo replace to update;
- inserts/updates: complete exact after-image and all destination keys;
- deletes: all destination keys, with non-key output columns null under one canonical schema rule;
- MySQL is the best first end-to-end CDC proof because official ROW/FULL events satisfy this
  contract directly;
- PostgreSQL requires a snapshot-seeded row materializer before it can claim complete after-images;
- MongoDB requires server 6.0+, collection post-images, and `fullDocument: "required"`;
- truncate, DDL, incomplete images, missing key fields, and schema changes that invalidate decoding
  fail before the affected frontier is checkpointed.

## Recommended large-transaction policy

Make `maximum_transaction_bytes` a mandatory compiled CDC capability bounded by host spill/replay
policy. A project/resource may lower it but cannot raise it above host authority. Bytes, not row
count, are the hard resource limit; row count remains evidence/telemetry. Transaction data streams
through accounted rolling replay/package storage rather than accumulating in memory. Ordinary
package rotation is soft inside a transaction. Crossing the hard byte bound fails closed and keeps
the prior checkpoint. A universal numeric default should not be invented in the kernel; the host
profile must publish the concrete bound used by the compiled plan.

## Artifact transition recommendation

CDF is pre-production and current artifacts are current-schema-only. Replace the position and all
embedding schemas coherently, bump source-position/checkpoint/store/package identities, update all
fixtures and renderers, reject old versions with a direct incompatibility error, and add no legacy
reader or migration. A partial version bump would be more dangerous than a clean replacement.

## Conclusions

1. One stringly `LogPosition` cannot safely represent these protocols. PostgreSQL and MySQL need
   typed committed coordinates; MongoDB needs a separate opaque resume-token kind.
2. Transaction completion should be encoded by the persisted type, not a boolean that can be
   false.
3. MySQL ROW/FULL/GTID is the shortest truthful path to the first complete-image CDC proof.
4. PostgreSQL CDC needs stateful reconstruction for unchanged TOAST values if complete after-images
   are non-negotiable.
5. MongoDB exact post-images are possible, but official change-stream APIs do not establish
   source-transaction package boundaries. The ratified model intentionally accumulates ordered
   events into CDF segments/packages and advances the terminal resume token only after receipt; it
   does not group Mongo events by transaction.
6. The current finite drain/package/receipt/checkpoint architecture remains suitable; the missing
   work is typed source authority and conformance, not a second streaming engine.

## Limits

- no live server, failover, slot, GTID, large-transaction, post-image, or resume-token fixture was
  exercised;
- MySQL cross-primary/failover continuation remains deliberately excluded from same-scope v1;
- MongoDB source-transaction grouping remains unproven by the official public contract inspected;
- numeric host transaction limits remain a deployment/profile decision, not a protocol fact;
- exact Rust naming may change during implementation if it preserves every field and law above.

## Ratification outcome

On 2026-08-03 the user ratified:

1. protocol-specific committed log variants plus a distinct opaque Mongo resume-token variant;
2. complete after-images/key-only deletes and MySQL-first proof, accepting PostgreSQL stateful
   reconstruction and MongoDB 6.0+ required post-images;
3. clean replacement of all pre-production artifacts with no compatibility readers or migrations,
   because CDF is net-new and customer zero;
4. MongoDB event-prefix processing: accumulate changes into segments/packages and advance the
   terminal resume token only after the destination receipt, without transaction grouping.

The only unresolved CDC runtime policy from the prior checkpoint is the hard resource behavior for
one PostgreSQL/MySQL transaction that exceeds ordinary package rotation. It does not apply to
MongoDB event segmentation.
