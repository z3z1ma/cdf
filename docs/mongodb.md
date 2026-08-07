# MongoDB collection source

CDF reads one finite MongoDB 8.0+ collection through the built-in `mongodb` source and the
official asynchronous Rust driver's raw-BSON cursor. Change streams, resume tokens, arbitrary
aggregation pipelines, map-reduce, and ObjectId-only cursor positions are not part of this
connector.

## Configure a collection

Declare the shared connection in `cdf.toml`. The endpoint is a credential-free topology authority;
put credentials in secret references rather than URI userinfo or query parameters:

```toml
[sources.warehouse]
type = "mongodb"
endpoint = "mongodb://mongo.internal:27017"
database = "analytics"
username = "secret://env/MONGODB_USER"
password = "secret://env/MONGODB_PASSWORD"
auth_source = "admin"
```

Reference one collection from `cdf/<namespace>/<resource>.cdf.sql`:

```sql
RESOURCE
DISPOSITION APPEND
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT *
FROM upstream(source => 'warehouse', collection => 'events');
```

Compilation and portable-plan validation do not contact MongoDB. Schema discovery samples at most
1,000 documents and 16 MiB by default, records those limits, and caches the resulting physical
observation. A sample is evidence about the sampled documents, not a claim that an unvalidated
schemaless collection is globally uniform. Execution applies the active state-backed schema and
compiled drift policy to every later document.

## Cursor reads

Finite numeric, UTC DateTime, and schema-established date cursors are supported. The collection must
include a non-null `_id` field in the active schema; CDF uses it as the stable tie-breaker and sorts
every cursor query by `cursor ASC, _id ASC`:

```sql
RESOURCE
DISPOSITION MERGE(id)
CURSOR updated_at
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT *
FROM upstream(source => 'warehouse', collection => 'events');
```

ObjectId itself is not a checkpoint cursor. Strings are not guessed to be dates or timestamps.
Exact supported comparisons, cursor bounds, and finite snapshot limits use typed BSON documents;
field paths and collection names are validated rather than interpolated into user SQL. Projection
governs the Arrow output, while the source reads complete BSON documents so unknown-field drift
cannot be hidden by a server projection.
Predicates whose missing/null, array, numeric, collation, or timezone behavior is not exactly Arrow
equivalent remain residual CDF work.

The default is an idempotently reopenable bounded cursor window, not a claim of a cross-shard
transaction snapshot. A failed or incomplete window cannot advance the checkpoint. Retry reopens
the typed window, and package identity/deduplication governs any overlap.

## BSON and Arrow types

CDF maps Boolean, signed integer, finite double, string, binary, array, nested document, and null
values directly to their pinned Arrow forms. BSON DateTime becomes a UTC millisecond timestamp.
ObjectId becomes 12-byte fixed-size binary with the `mongodb.object_id@1` semantic tag.

BSON Decimal128 becomes Arrow Decimal128 only when declared schema or validator authority proves
one complete precision-and-scale domain. Otherwise it remains canonical exact UTF-8, including
native special values, with the `mongodb.decimal128_value_text@1` semantic tag. Decimal128 never
becomes floating point, and the exact text is BSON Decimal128 value spelling rather than Extended
JSON.

Unsupported BSON kinds, duplicate document keys, heterogeneous arrays, invalid UTF-8, a missing
non-null field, or a value outside the pin fail under the active variant/quarantine policy. CDF does
not silently stringify through Extended JSON or widen the compiled schema.

## Runtime and operations

One resource owns one reusable official client and native pool. The measured defaults are 65,536
cursor rows, one pool connection, one logical query, and a one-batch queue. Queue, producer, and
consumer can retain at most three output batches. Each poll admits at most 64 MiB of raw BSON plus
a 128 MiB decode working set covering construction scratch, retained Arrow output, and drift
evidence; the emitted batch is capped at 64 MiB. Reduce `batch_rows` or project fewer fields if a
document shape exceeds the output bound. The host owns async execution, memory, cancellation,
egress, and retries. The
connector creates no private runtime, worker pool, semaphore, retry loop, or unbounded queue.

- Invalid configuration, collection/schema drift, malformed BSON, and unsupported values are typed
  contract or data failures.
- Authentication failures remain `Auth`; server throttling remains `RateLimited`; recoverable
  transport and timeout failures remain `Transient` with driver retry metadata preserved.
- Host permission, DNS, TLS, socket, and resource failures remain `Environment` failures.
- Errors, plans, reports, and debug output retain only credential-safe topology and secret
  references; they do not echo secret values.

The release-mode local 100,000-row mixed BSON sweep uses MongoDB 8.0.13 from the digest-pinned
`mongo` image. Five samples compare the shipped raw-BSON/Arrow path with the same official client,
complete fixed-fixture documents, stable sort, duplicate-key validation, field conversion, Arrow
construction, and full content verification. The selected 65,536-row, one-connection cell measured a 0.905 median
throughput ratio against the favorable direct path, above the required 0.900 roofline. The raw,
host-labelled report is
[`2026-08-04-mongodb-source-roofline.json`](../.10x/evidence/.storage/2026-08-04-mongodb-source-roofline.json).
