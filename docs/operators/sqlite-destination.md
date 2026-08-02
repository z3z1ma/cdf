# SQLite destination

CDF writes finalized packages to a local SQLite database through the built-in
`sqlite` destination. It is a single-file, single-writer destination intended
for local and coordinator-attached storage. It is separate from the SQLite
checkpoint store even when both files live under `.cdf/`.

## Configure the destination

Use a project-relative literal path when the database belongs to the project:

```toml
[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "sqlite://.cdf/output.sqlite"
```

The URI accepts a project-relative or absolute local path. It rejects parent
traversal, query strings, fragments, percent escapes, and control characters.
Network filesystems, attached databases, and cross-file atomicity are outside
the connector contract.

The resource's normalized target name becomes the SQLite table. CDF validates
and quotes it; it never accepts arbitrary destination SQL. The resource chooses
`append`, `replace`, or `merge`. Merge additionally requires normalized merge
keys. CDC application is not supported.

## Commit and schema behavior

One `BEGIN IMMEDIATE` transaction covers all target rows, compact
`_cdf_row_key` provenance, quarantine rows, the load receipt, immutable commit
evidence, segment mirrors, and current plus historical checkpoint-state mirror
data. CDF uses one prepared-statement writer on the
accounted `sqlite.destination.sync` lane. It does not create a private pool,
executor, or parallel writer.

New target tables are `STRICT`. Existing target columns must have compatible
SQLite storage types and exact nullability. CDF may add a newly declared
nullable column, but it refuses to invent a default for a newly required
column. Names beginning with `_cdf_` are reserved for connector metadata. The
only package-schema exception is CDF's generated nullable UTF-8 `_cdf_variant`
field with its exact residual semantic and `residual-json-v1` encoding metadata;
a user field or metadata impostor with that name is rejected.
Scalar Arrow values use the mappings published by `cdf inspect destinations`;
nested, union, dictionary, and run-end-encoded values fail preflight.
Float16, Float32, and Float64 use canonical big-endian IEEE-754 bit-pattern
`BLOB`s so NaN payloads and signed zero remain bit-exact; they are not coerced
through SQLite `REAL`.

`replace` deletes the prior target contents only inside the package transaction,
including when the replacing package contains zero rows.
`merge` stages the package in the same connection, rejects duplicate package
keys and ambiguous target matches before target mutation, then updates matches
and inserts misses. The connector does not silently deduplicate.

CDF reads the existing `journal_mode` and `synchronous` values as receipt
evidence and leaves them unchanged. It does not enable WAL. Choose and manage
SQLite durability settings outside CDF according to the host's operational
policy.

## Receipts, replay, and recovery

The database contains typed connector-owned tables:

- `_cdf_loads` stores the exact receipt by target, package hash, and package
  idempotency token.
- `_cdf_state` stores receipt-gated checkpoint mirror data.
- `_cdf_state_history` stores the immutable typed state identity and position
  lineage for each receipt while `_cdf_state` remains the mutable current head.
- `_cdf_segments` binds package segments to half-open target row-key ranges.
- `_cdf_quarantine` stores exact rejected-row evidence.
- `_cdf_commit_evidence` stores the immutable typed schema, index, segment,
  state, and collision-resistant quarantine multiset commitment for each receipt.
- `_cdf_row_key_allocator` allocates compact, non-overlapping provenance.

After commit, verification opens a fresh read-only connection and independently
checks the exact receipt, immutable evidence hash, target schema, unique partial
provenance index, typed state history, full segment JSON and scalar ranges, and
the order-independent multiplicity-preserving quarantine commitment. Historical
receipts remain verifiable after a later replace or overlapping merge because
verification does not pretend the mutable current target is immutable commit
evidence. Checkpoint commit occurs only after verification succeeds.

Replay requires an explicit table target because a package does not authorize
CDF to infer one from the database:

```bash
cdf --project /path/to/project replay package \
  /path/to/package \
  --to sqlite://.cdf/replay.sqlite \
  --target events
```

Replaying the same finalized package to the same target returns the exact stored
receipt and does not rewrite rows. A process failure before SQLite commit leaves
no target or mirror mutation. If the process exits after SQLite commit but
before returning the receipt, replay discovers and verifies the committed
receipt without source contact.

## Failure ownership and remediation

- A busy or locked database is `Transient`. Remove the competing writer or let
  its transaction finish, then retry from durable package/checkpoint evidence.
- Permission, read-only media, missing parent directories, full disks, host
  I/O, and memory exhaustion are `Environment` failures. Repair the host or
  path; CDF does not relabel them as bad data.
- Corruption, a non-database file, incompatible existing target shape, damaged
  mirror JSON, or receipt/provenance mismatch is a `Destination` failure. Stop
  writes, preserve the file, and restore or inspect the durable artifact before
  recovery.
- Invalid URI syntax, identifiers, disposition, merge-key declaration, or
  unsupported Arrow mappings are `Contract` failures and must be corrected
  before execution.
- Duplicate merge keys in the finalized package are `Data` failures. Repair the
  producer or its declared identity; the package is not partially applied.
- A payload null that contradicts a declared required Arrow field is `Data`;
  an existing target uniqueness or other durable target constraint is
  `Destination`.

Injected run cancellation is checked across session, writer, mirror, merge, and
verifier work. A SQLite VM progress handler interrupts long-running statements;
the open transaction rolls back on cancellation.

Do not delete `_cdf_*` tables or edit their JSON manually. Use `cdf package
verify`, `cdf inspect package`, `cdf state history`, and the standard
[recovery](recovery.md) and [replay](replay.md) workflows to establish which
artifact is authoritative.

## Performance evidence

`sqlite-destination-roofline` is the release-only local evidence harness. It
records five raw 1,000,000-row samples for the production generic runtime and a
direct `rusqlite` prepared-statement reference under the same file, schema,
`DELETE` journal mode, `FULL` synchronous setting, explicit transaction, Arrow
conversion, mirror writes, durable commit, and fresh verification boundary.
The direct reference serializes the same full typed mirror/evidence shapes and
uses the production verifier; only finalized-package governance is omitted.
The report records host and executable identity, variance, and known semantic
biases; it is evidence for that host, not a universal throughput promise.

## Related contracts

- [SQLite destination specification](../../.10x/specs/sqlite-destination.md)
- [Destination receipts and guarantees](../../.10x/specs/destination-receipts-guarantees.md)
- [Database connector roofline](../../.10x/specs/database-connector-roofline.md)
- [Troubleshooting](troubleshooting.md)
