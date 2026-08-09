# MongoDB collection source

CDF reads one finite MongoDB 7.0+ collection through the built-in `mongodb` source and the
official asynchronous Rust driver's raw-BSON cursor. A resource may use a native `find` filter or
a read-only aggregation pipeline before its surrounding CDF SQL. Change streams, resume tokens,
server-side writes, map-reduce output, and ObjectId-only cursor positions are separate concerns.

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
schema_depth = 1
discovery_records = 1000
discovery_bytes = 16777216
cursor_batch_rows = 8192
output_batch_rows = 65536
max_time_ms = 300000
read_concern = "majority"
read_preference = '{"mode":"secondaryPreferred"}'
```

Atlas IAM authentication uses the same credential-free endpoint boundary. Keep the access key,
secret key, and optional STS session token behind independent secret references:

```toml
[sources.atlas]
type = "mongodb"
endpoint = "mongodb+srv://cluster.example.mongodb.net"
database = "analytics"
username = "secret://env/MONGODB_AWS_ACCESS_KEY_ID"
password = "secret://env/MONGODB_AWS_SECRET_ACCESS_KEY"
auth_source = "$external"
auth_mechanism = "MONGODB-AWS"
aws_session_token = "secret://env/MONGODB_AWS_SESSION_TOKEN"
```

`cdf add` accepts the corresponding standard Atlas connection string, removes its credential and
authentication query material from the endpoint, and publishes the three credential values as
owner-only private secret files. Unsupported URI query options fail rather than being silently
discarded.

Reference one collection from `cdf/<namespace>/<resource>.cdf.sql`:

```sql
RESOURCE
DISPOSITION APPEND
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT *
FROM upstream(source => 'warehouse', collection => 'events', schema_depth => 1);
```

Compilation and portable-plan validation do not contact MongoDB. Schema discovery samples at most
1,000 documents and 16 MiB by default, records those limits, and caches the resulting physical
observation. A sample is evidence about the sampled documents, not a claim that an unvalidated
schemaless collection is globally uniform. Execution applies the active state-backed schema and
compiled drift policy to every later document. `schema_depth` accepts `1..=32` and defaults to `1`.

## Native filters and aggregations

`filter` is a duplicate-free Extended JSON document. It becomes the base MongoDB `find` selector;
exact CDF predicates are combined with it without changing its meaning:

```sql
RESOURCE
DISPOSITION REPLACE
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT id, status, amount
FROM upstream(
  source => 'warehouse',
  collection => 'events',
  filter => '{"status":{"$in":["open","ready"]}}',
  hint => '"status_1"',
  max_time_ms => 30000
);
```

`pipeline` is a duplicate-free Extended JSON array. CDF runs the authored stages in order, then
applies exact outer CDF filters, ordering, projection, and limit to the produced relation:

```sql
RESOURCE
DISPOSITION REPLACE
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT customer_id, total
FROM upstream(
  source => 'warehouse',
  collection => 'events',
  pipeline => '[{"$match":{"status":"posted"}},{"$group":{"_id":"$customer_id","total":{"$sum":"$amount"}}},{"$project":{"_id":0,"customer_id":"$_id","total":1}}]',
  allow_disk_use => true,
  let => '{"minimum_amount":0}'
);
```

`filter` and `pipeline` are mutually exclusive. CDF recursively rejects `$out`, `$merge`, and
`$changeStream` before contact. A pipeline using `$lookup`, `$unionWith`, or `$graphLookup` runs
locally, but portable plan export fails until every referenced collection can be independently
attested.

Schema/discovery budgets, cursor/output batching, operation timeout, read concern, and read
preference may be source defaults or resource overrides. Resolution is built-in default, then
source default, then explicit resource override. `allow_disk_use`, `hint`, `collation`, `let`, and
`comment` remain resource-only because they define one operation rather than shared connection
policy. Resolved controls apply identically to discovery and execution:

| Option | Default | Purpose |
| --- | ---: | --- |
| `discovery_records` | `1000` | Maximum sampled documents |
| `discovery_bytes` | `16777216` | Maximum sampled BSON bytes |
| `cursor_batch_rows` | `8192` | MongoDB wire-cursor request; measured Atlas default |
| `output_batch_rows` | `65536` | Maximum rows considered for one adaptive Arrow output batch |
| `max_time_ms` | unset | MongoDB operation deadline, up to one hour |
| `allow_disk_use` | `false` | Permit aggregation spill; invalid for `find` |
| `hint` | unset | Extended JSON index name or compound key document |
| `collation` | unset | Extended JSON MongoDB collation document |
| `let` | unset | Extended JSON aggregation variable document |
| `comment` | unset | Profiler comment, at most 1024 UTF-8 bytes |
| `read_concern` | unset | `local`, `majority`, `linearizable`, `available`, or `snapshot` |
| `read_preference` | unset | Extended JSON mode, optional tag sets, and max staleness |

Native BSON input is stored in compiled plans as exact BSON bytes so numeric widths and ordered
documents survive plan export/import. Human evidence contains only its shape and digest, never
filter, pipeline, variable, hint, or comment literals.

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
field paths and collection names are validated rather than interpolated into user SQL. The source
projects the exact compiled output, filter, order, stable-key, and cursor dependencies on the
server. Discovery owns source-shape observation; runtime does not transfer unrelated wide fields.
Predicates whose missing/null, array, numeric, collation, or timezone behavior is not exactly Arrow
equivalent remain residual CDF work.

The default is an idempotently reopenable bounded cursor window, not a claim of a cross-shard
transaction snapshot. A failed or incomplete window cannot advance the checkpoint. Retry reopens
the typed window, and package identity/deduplication governs any overlap.

## BSON and Arrow types

Discovery types consistent top-level primitive fields directly. Boolean, signed integer, double,
double, string, binary, and null retain their pinned Arrow forms; Int32 and Int64 observations widen
only to Int64. BSON DateTime becomes a UTC millisecond timestamp. ObjectId becomes 12-byte
fixed-size binary with the `mongodb.object_id@1` semantic tag.

At the default `schema_depth => 1`, a top-level BSON document or array is deliberately opaque. CDF
stores it as deterministic Canonical Extended JSON UTF-8 with the
`mongodb.document_extended_json@1` or `mongodb.array_extended_json@1` semantic tag. Nested keys do
not become columns and do not create schema drift. This avoids the common failure where UUID-like
map keys turn into an unbounded inferred schema.

Increasing `schema_depth` expands documents and arrays only through the configured level. A child
document or array at the boundary remains opaque, so `schema_depth => 2` can expose stable direct
children while keeping their maps and lists intact. Empty complex values remain opaque. If sampled
non-null values for one field cannot reconcile to one lossless typed domain, the whole field becomes
Canonical Extended JSON UTF-8 tagged `mongodb.value_extended_json@1` instead of inventing a lossy
union.

BSON Decimal128 becomes Arrow Decimal128 only when declared schema or validator authority proves
one complete precision-and-scale domain. Otherwise it remains canonical exact UTF-8, including
native special values, with the `mongodb.decimal128_value_text@1` semantic tag. Decimal128 never
becomes floating point, and the exact text is BSON Decimal128 value spelling rather than Extended
JSON.

Unsupported top-level scalar BSON kinds still require an explicit policy. Duplicate document keys,
invalid UTF-8, a missing non-null field, or a later value outside a primitive pin follow the active
variant/quarantine policy. Canonical Extended JSON is used only for the explicitly tagged opaque
domains above; CDF does not silently stringify ordinary typed fields or widen the compiled schema.

## Runtime and operations

One resource owns one reusable official client and native pool. The measured defaults are an
8,192-row wire cursor request, a 65,536-row adaptive Arrow output ceiling, one pool connection, one
logical query, and a one-batch queue. Queue, producer, and consumer can retain at most three output
batches. Each poll admits at most 64 MiB of raw BSON plus a 128 MiB decode working set covering
construction scratch, retained Arrow output, and drift evidence; the emitted batch is capped at 64
MiB. Reduce `output_batch_rows` or project fewer fields if a document shape exceeds the output
bound. The host owns async execution, memory, cancellation,
egress, and retries. The
connector creates no private runtime, worker pool, semaphore, retry loop, or unbounded queue.

- Invalid configuration, collection/schema drift, malformed BSON, and unsupported values are typed
  contract or data failures.
- Authentication failures remain `Auth`; server throttling remains `RateLimited`; recoverable
  transport and timeout failures remain `Transient` with driver retry metadata preserved.
- Host permission, DNS, TLS, socket, and resource failures remain `Environment` failures.
- Errors, human reports, and debug output retain only credential-safe topology, secret references,
  and native-input hashes; they do not echo secret or native-query literal values.

The release-mode local 100,000-row mixed BSON sweep uses MongoDB 8.0.13 from the digest-pinned
`mongo` image. Five samples compare the shipped raw-BSON/Arrow path with the same official client,
complete fixed-fixture documents, stable sort, duplicate-key validation, field conversion, Arrow
construction, and full content verification. The final clean 32,768-row, one-connection cell
measured a 0.922 median throughput ratio against the favorable direct path, above the required
0.900 roofline. Operational defaults independently use the live-Atlas-selected 8,192-row wire
request and a byte-adaptive output ceiling of 65,536 rows. The raw, host-labelled report is
[`2026-08-04-mongodb-source-roofline.json`](../.10x/evidence/.storage/2026-08-04-mongodb-source-roofline.json).
