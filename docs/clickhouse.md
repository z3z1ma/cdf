# ClickHouse source and destination

CDF reads one finite ClickHouse table through the built-in `clickhouse` source and the official
Rust client's ArrowStream extension. The connector does not accept arbitrary SQL, CDC streams, or
native-protocol configuration.

## Configure a destination

The built-in destination accepts a credential-safe `clickhouse://host:port/database` URI for HTTP
or `clickhouses://host:port/database` for HTTPS. Supply credentials through a `secret://` URI; URI
query parameters are not a configuration surface. The target table must already exist because its
engine, ordering, partitioning, codecs, projections, defaults, and operational tuning belong to
the operator.

CDF writes Arrow record batches directly through the official ArrowStream extension with LZ4,
synchronous acknowledgement, deterministic insert tokens, and one bounded writer. Every target
needs an explicit insert-deduplication window large enough for the package segment count and the
two compact provenance columns shown below. Non-replicated tables use
`non_replicated_deduplication_window`; replicated tables use
`replicated_deduplication_window`. For the default keyed merge mode, the target
must be an unversioned `ReplacingMergeTree` whose sorting key exactly equals the CDF merge key:

```sql
CREATE TABLE analytics.events
(
    id Int64,
    name Nullable(String),
    _cdf_package_hash FixedString(32),
    _cdf_package_row_ord UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS non_replicated_deduplication_window = 100000;
```

`replacing_merge_tree` is the default because it preserves ClickHouse's high-throughput write
path. CDF package finalization fails duplicate merge keys unless an explicit deterministic keyed
dedup rule selects the winner. Before settlement, CDF validates with `FINAL` that the package owns
the complete unique canonical row-ordinal set. Ordinary readers can observe old and new physical
versions until background merges finish. Consumers that require current keyed
results immediately must query with `FINAL` (or an equivalent operator-owned view).

When immediate physical uniqueness is more important than write amplification and latency, opt
into the copy-on-write mode:

```toml
[environments.prod.destination_policy.clickhouse]
merge_mode = "atomic_copy_on_write"
```

This mode validates incoming and existing key uniqueness, constructs the merged table with a
server-side anti-join and bulk insert, verifies it, and publishes it with one `EXCHANGE TABLES`.
It requires an `Atomic` database, a non-replicated row-preserving `MergeTree` target, and no dependent
materialized views. It rewrites the complete target and therefore is intentionally not the
throughput default. CDF declares one writer and serializes its own commits. Operators must also
exclude out-of-band writes to the target while replace or copy-on-write merge is building its
snapshot; ClickHouse has no non-Keeper compare-and-exchange primitive that could preserve an
uncoordinated concurrent write.

Append writes directly to a row-preserving `MergeTree` or `ReplicatedMergeTree` target. Replace
builds a complete structurally fingerprinted stage and publishes it with `EXCHANGE TABLES`; it has the same `Atomic` database and
non-replicated-target requirements as copy-on-write merge. CDF refuses nullable or missing merge
keys, sorting/partition drift, versioned replacement-engine guessing, ambiguous existing keys,
unsupported schemas, and targets whose dependent views would make replacement semantics false.
Receipts record the selected merge mode and physical-visibility contract. The merge mode also
participates in the finalized package identity, so policy drift fails before replay can mutate the
destination. Historical replace/copy-on-write receipts remain verifiable from immutable settlement
mirrors after a later valid CDF publication supersedes their target marker.

## Configure a source

Use `clickhouse://` for HTTP and `clickhouses://` for HTTPS. The endpoint is a credential-free
authority; database and table are separate validated identifiers. Usernames and passwords, when
needed, are secret references:

```toml
[source.warehouse]
kind = "clickhouse"
endpoint = "clickhouses://warehouse.example:8443"
database = "analytics"
username = "secret://env/CLICKHOUSE_USER"
password = "secret://env/CLICKHOUSE_PASSWORD"
max_threads = 4
max_block_rows = 65536
stream_buffer_batches = 1

[resource.events]
table = "events"
write_disposition = "append"
trust = "governed"
schema_mode = "discover"
```

Compilation and portable-plan validation make no network contact. Discovery contacts only the
configured table catalog and a `LIMIT 0` Arrow schema probe, then pins the exact Arrow schema.
Execution fails closed unless that catalog-backed physical observation is attached to the compiled
resource; a declared logical schema is never relabeled as physical evidence.
Credentials are never embedded in the endpoint or rendered as values. `cdf add` accepts an
authoring URL ending in exactly `/database/table`; any URL userinfo is split into private secret
files:

```bash
cdf add warehouse.events clickhouses://user:password@warehouse.example:8443/analytics/events
cdf plan warehouse.events
cdf plan warehouse.events
cdf preview warehouse.events --limit 20
cdf run warehouse.events
```

## Arrow types and queries

Production reads call the official Arrow extension's bounded `fetch_arrow_with_limits()` path
directly. There is no row-shaped Serde bridge. ClickHouse
`String` permits arbitrary bytes, so discovery requests Arrow `Binary` rather than assuming UTF-8.
Numeric widths, decimals, fixed strings, dates, timestamp scale/timezone, arrays, tuples, maps,
nullable, low-cardinality, enum, and IP values retain exact Arrow semantics from the selected
official extension. ClickHouse 25.8 exposes narrow `Date` and `DateTime` storage as UInt16/UInt32;
generated queries promote those two cases to Arrow `Date32` and second `Timestamp` so storage
width does not leak into schema or cursor semantics. Native `Date32` and `DateTime64` remain
unchanged. Physical `UUID` is the other explicit normalization: generated queries use ClickHouse
`toString()` and CDF validates the canonical lowercase hyphenated result as Arrow `Utf8`, while
preserving `UUID` physical metadata. These mappings are field-specific; ordinary ClickHouse
`String` remains arbitrary-byte Arrow `Binary`. `Dynamic`, `Variant`, aggregate-state, geo, or
another unsupported shape fails discovery with the field and physical type before the Arrow
schema probe; CDF does not stringify those types.

A configured `Int8`/`Int16`/`Int32` cursor is projected as `Int64`, and a configured
`UInt8`/`UInt16`/`UInt32` cursor is projected as `UInt64`. Discovery records that cursor-only cast
alongside the original physical type, and every projection, bound, filter, and ordering expression
uses the same cast. Identically typed non-cursor fields retain their native widths.

Projection and snapshot limits are pushed into the one generated table query. Cursor limits remain
generic-engine work because the shared checkpoint has no stable-key component: pushing a limit
could bisect an equal-cursor group and make its remainder unreachable. A limited cursor run is
therefore incomplete and cannot advance its checkpoint. Only typed comparisons whose
Arrow and ClickHouse semantics are exact are pushed; other predicates remain engine filters.
Identifiers are adapter-validated and quoted, and every value is a client binding. User SQL text
is never interpolated.

## Cursor sources

Numeric, `Date`/`Date32`, and `DateTime`/`DateTime64` cursor fields are supported. A cursor requires
a distinct, non-null stable key and always executes in `cursor ASC, stable_key ASC` order:

```toml
[resource.events]
table = "events"
stable_key = "event_id"
cursor = { field = "updated_at", ordering = "exact", lag = "0ms" }
write_disposition = "merge"
primary_key = ["event_id"]
merge_key = ["event_id"]
trust = "governed"
schema_mode = "discover"
```

CDF never infers a temporal cursor from a string. The shared checkpoint stores the cursor value;
the stable key supplies deterministic order inside an equal-cursor group but is not a second
checkpoint component. Consequently a durable frontier cannot represent the middle of one
equal-cursor group. Retry restarts the whole current cursor window and never splices a second query
into an already emitted stream. `DateTime64` cursor precision is limited to scale 0 through 6;
scale 7 through 9 fails configuration because the shared cursor stores microseconds and CDF does
not silently truncate a sub-microsecond frontier.

## Runtime and failures

One resource open owns one official HTTP client and one logical Arrow query stream. The server may
use at most `max_threads`. CDF carries narrow, opt-in local patches to the pinned official
`clickhouse`, `clickhouse-ext-arrow`, and Arrow IPC crates. Existing upstream APIs remain
backward-compatible; every CDF discovery and execution query opts into finite limits before its
first lazy poll. The patched transport caps its HTTP/1 read buffer at 64 KiB, and CDF holds a
separate `clickhouse-http1-transport` source lease in the same lifetime owner as that reusable
client/pool. Non-success response collection is capped at 1 MiB. HTTP input frames are capped at
64 KiB, Arrow metadata at 2 MiB, and the record body at 25 MiB before decoder scratch growth. One
64 MiB `clickhouse-arrow-decode` lease explicitly covers the split-body allocator's 32 MiB capacity,
one possible 25 MiB alignment copy, metadata, HTTP-frame overlap, and 4 MiB of batch-container
headroom. A separate 32 MiB `clickhouse-arrow-cursor-state` lease remains attached to each live
cursor. It covers one retained 25 MiB response chunk, a pre-conversion-bounded 4 MiB owned schema,
and message/decoder/container state; it is never reconciled into or transferred with an emitted
batch. The bounded path rejects Arrow
dictionary messages because dictionary IDs can
accumulate across messages before a record batch is emitted; pinned ClickHouse 25.8 materializes
the admitted `LowCardinality` fixture as its exact non-dictionary Arrow value type.

Production response compression is disabled, and the ArrowStream extension also requests no inner
Arrow compression. The bounded ClickHouse path rejects a contradictory compressed IPC record batch
before allocating decoded buffers, preventing encoded-body/decompressed/alignment-copy overlap.
The generic patched decoder separately validates declarations, caps actual LZ4 output at the
declaration, and applies its configured ceiling cumulatively across decoded buffers. For flat
scalar fixed-width projections, CDF derives a
block-row ceiling from every field's 64-byte-aligned validity and value buffers and rejects a
projection when even one row is too wide. Nested fixed-size projections join the conservative
variable-width path. That path additionally makes ClickHouse reject any row above 16 MiB and caps
blocks at one row. ClickHouse may coalesce small internal blocks while writing ArrowStream, so a
separate one-million-row decoder guard is intentionally independent of `max_block_rows`; body bytes
remain the allocation fence. Empty batches reuse the current decode lease. Retained nonempty
batches are reconciled to their exact bytes before emission.

Catalog-provided ClickHouse type declarations are preflighted iteratively before semantic matching:
64 KiB of text, 64 nesting levels, and 4,096 structural tokens are the hard parser limits. This
keeps hostile server metadata from turning recursive type validation into stack or quadratic-CPU
exhaustion. DateTime timezone metadata must be one bounded IANA-style literal and is rendered only
from its parsed characters. Execution also requires every effective physical-type string to equal
the catalog-backed observation before query construction. The patched Arrow boundary validates
schema enums, widths, decimal bounds, union IDs, nested child cardinality, record-batch ranges,
field nodes, variadic counts, and union prefixes fallibly. Before conversion, schemas are capped at
4,096 field nodes, 4,096 metadata entries, 64 levels, and a conservative 4 MiB owned-size estimate,
so malformed server schemas and batches return typed source-data errors rather than panicking or
escaping their lease.

`stream_buffer_batches` is the queued-batch capacity, not the complete retained-batch count. One
batch may also be held by the producer while it acquires the next decode lease and one by the
consumer, so the truthful overlap is `stream_buffer_batches + 2`: three batches by default and 66
at the configurable maximum. The host owns the async runtime,
cancellation, memory, egress, and retry authority. The adapter creates no executor, worker pool,
retry loop, or unbounded queue. The reusable official client owns its HTTP connection pool; the
Arrow extension disables redundant inner Arrow compression.

- Authentication failures are `Auth`; server quota codes are `RateLimited`; recoverable remote
  transport and timeout failures are `Transient`; nested host permission/resource/facility and
  client TLS-construction failures are `Environment`.
- Unsupported types, query rejection, catalog drift, malformed Arrow, and pinned-schema mismatch
  are `Data` failures. Generated parameter and official-client invariant failures are `Internal`.
- Server response text is not echoed, so credentials or source-returned text cannot leak through
  mapped errors. Embedded CDF errors retain their original kind and retry delay.
- A server failure after one emitted batch leaves the source invocation incomplete. Completion and
  checkpoint advancement occur only after the Arrow stream reaches successful EOF.

## Local live and roofline cells

Focused live conformance uses ClickHouse LTS 25.8.28.1 pinned by immutable platform digest. The
source release roofline compares the CDF adapter against the same official bounded Arrow query with the
same response/IPC ceilings, table, projection, ordering, no-compression setting, `max_threads`, and
effective block-row bound. Its content identity covers the connector, benchmark, workspace lock,
and all locally patched dependency sources. A report
is passing only at a CDF/direct median throughput ratio of at least 0.90. Until a current report is
recorded in the owning ticket, this document makes no passing performance claim. The destination
roofline separately measures the post-preflight delivery and settlement path against direct
official-client ArrowStream insertion with the same synchronous acknowledgement, LZ4 compression,
and insert-token settings for append and default native merge.
