Status: active
Created: 2026-08-02
Updated: 2026-08-08

# SQLite table source

## Purpose and scope

This specification governs a first-party finite SQLite table source. It is further governed by
`.10x/specs/source-extension-runtime-contract.md`,
`.10x/specs/resource-authoring-planning-batches.md`, and
`.10x/decisions/non-file-window-close-checkpoint-semantics.md`.

## Source contract

`cdf-source-sqlite` MUST be a leaf adapter distinct from `cdf-state-sqlite`. Its driver id and
source kind are both `sqlite`, and it accepts local `sqlite://` locations. The compiled plan MUST
contain a normalized project-relative or explicitly portable database locator, a validated table
identifier, schema/capability authority, and no open connection. Absolute coordinator-local paths
MUST fail portable-plan validation.

The base resource is one table. Native read-query resources are governed by
`.10x/specs/sqlite-native-query-source.md`. Table discovery MUST use SQLite catalog and declared-
type metadata through a read-only connection and MUST retain original names. Runtime MUST
open a read-only transaction before querying and hold that transaction through stream completion,
giving every emitted batch one database snapshot. It MUST NOT copy the database, force WAL, change
pragmas with persistent effects, or open parallel snapshot connections.

Projection, limit, and the adapter's typed comparison predicates MAY be pushed exactly only when
SQLite semantics agree with Arrow null, collation, numeric, and temporal semantics. Unsupported or
inexact predicates remain for the engine. Identifiers MUST be validated and quoted by the adapter;
values MUST be bound parameters.

## Schema and cursors

SQLite's dynamic storage classes MUST cross through the existing discovery, schema-freeze, and
normalization authorities. Declared column types guide discovery but do not override observed
values. A runtime value outside the pinned Arrow schema follows the configured contract
normalization/quarantine behavior; it never changes the schema silently.

Snapshot reads and bounded numeric, timestamp, and date cursor reads are supported. Cursor queries
MUST use a deterministic ascending cursor plus stable key tie-breaker. Numeric cursor values bind
as SQLite numeric values. Date/timestamp cursors require an explicit compiled encoding of
`iso8601_text`, `unix_seconds`, `unix_milliseconds`, `unix_microseconds`, or `unix_nanoseconds`;
there is no implicit temporal encoding. Incompatible storage values, missing stable tie-breakers,
page-token semantics, and unsupported cursor kinds fail before reading. Lag and checkpoint
advancement use the shared window-close rules.

## Execution and performance

The native `rusqlite` connection and statement are run-owned, thread-affine blocking state on one
declared source lane. Rows MUST stream into byte-accounted Arrow builders with bounded fetch/batch
sizes and cancellation checks between bounded row groups. The adapter MUST not materialize the
table or create a private worker pool. Runtime retry may reopen only from the last committed cursor
window; an uncommitted snapshot cannot be represented as resumable.

The direct-library roofline and evidence follow `.10x/specs/database-connector-roofline.md`.

## Error behavior

Missing files/tables, invalid identifiers, schema drift, ambiguous temporal encoding, lock/busy
conditions, corruption, and host I/O failures MUST retain the shared error taxonomy and path
redaction. A busy database may use bounded host-owned retry only when the operation is safe to
reopen; it MUST otherwise fail with remediation. No query may mutate the database.

## Scenarios and acceptance criteria

- Given a concurrent writer, when a WAL or rollback-journal database is read, then one run observes
  one transaction snapshot and emits no mixed versions.
- Given a numeric or explicitly encoded temporal cursor, when multiple batches close a window,
  then canonical order and the committed position match shared window-close semantics.
- Given a dynamic value outside the pinned schema, when it is read, then the configured contract
  behavior applies without schema mutation.
- Source discovery, add, plan, preview, run, replay, redaction, cancellation, jobs invariance, and
  connector certification pass by leaf/catalog/fixture additions only.
- The SQLite source macro cell meets the 0.90 direct-`rusqlite` roofline.

## Explicit exclusions

Resident CDC, trigger installation, SQLite session-extension changesets, network filesystems,
persistent pragma changes, and multi-connection snapshot partitioning are excluded. Native
read-query resources are governed by `.10x/specs/sqlite-native-query-source.md`.
