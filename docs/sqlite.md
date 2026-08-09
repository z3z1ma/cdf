# SQLite source

CDF reads a local SQLite table or native read query through the built-in `sqlite` source. This
connector is distinct from CDF's SQLite checkpoint store: source configuration uses
`kind = "sqlite"` and a `sqlite://` database location, while checkpoint state remains an
environment concern.

## Configure a snapshot source

Keep the database below the project root so compiled plans remain portable:

```toml
[source.local]
kind = "sqlite"
location = "sqlite://data/events.sqlite"
output_batch_rows = 32768
busy_timeout_ms = 5000
cache_kib = 65536
mmap_bytes = 268435456

[resource.events]
table = "events"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
] }
```

Each resource declares exactly one of `table` or `query`. Identifiers are validated and quoted by
the adapter; predicate and cursor values are bound parameters.

`cdf add` can author the same source after validating that the file is inside the project:

```bash
cdf add local.events sqlite://data/events.sqlite --option table=events
cdf plan local.events
cdf preview local.events --limit 20
cdf run local.events
```

An absolute path can be used by a coordinator-local compile, but portable-plan admission rejects
it. Prefer a project-relative location from the start. Network filesystems are outside the
supported contract.

## Configure a native query

Use a native query when SQLite should own joins, aggregates, windows, JSON extraction, virtual
tables, or another read expression:

```toml
[source.local]
kind = "sqlite"
location = "sqlite://data/events.sqlite"
output_batch_rows = 32768
busy_timeout_ms = 5000
cache_kib = 65536
mmap_bytes = 268435456

[resource.enriched_events]
query = """
WITH ranked AS (
  SELECT e.id,
         e.category,
         json_extract(e.payload, '$.value') AS value,
         row_number() OVER (PARTITION BY e.category ORDER BY e.id) AS ordinal
  FROM events AS e
)
SELECT id, category, value, ordinal FROM ranked
"""
write_disposition = "replace"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "category", type = "string", nullable = false },
  { name = "value", type = "int64", nullable = true },
  { name = "ordinal", type = "int64", nullable = false },
] }
```

The authored statement must be one parameter-free, read-only SQLite statement. `SELECT`,
`WITH ... SELECT`, `VALUES`, joins, aggregates, windows, JSON functions, and read-only virtual
tables are supported. SQLite's statement authorizer and read-only proof reject DDL, DML, writable
pragmas, attach/detach, extension loading, and additional statements before execution.

`cdf add` preserves the query and resource controls while displaying only a query digest:

```bash
cdf add local.enriched_events sqlite://data/events.sqlite \
  --option query="SELECT id, category FROM events" \
  --option output_batch_rows=32768 \
  --option busy_timeout_ms=5000
```

The operational controls may be source defaults or resource overrides. Resolution is built-in
default, then source default, then resource override; the resolved values are identity-bearing:

| Option | Range and default | Purpose |
| --- | --- | --- |
| `output_batch_rows` | `1..=100000`; `32768` | Arrow row target; the default is the measured roofline setting |
| `busy_timeout_ms` | optional `1..=3600000` | Connection-local busy wait |
| `cache_kib` | optional `64..=1048576` | Connection-local SQLite cache budget |
| `mmap_bytes` | optional `0..=1073741824` | Connection-local SQLite mmap ceiling |

The cache, mmap, and busy settings do not mutate persistent database pragmas. Table discovery uses
catalog metadata and never samples rows. Query discovery uses prepared output metadata. Only an
SQLite expression without a declared result type may require observing runtime storage classes;
that observation uses the `cdf discover` request's own row/byte budget rather than an authored SQL
source or resource option. Mixed incompatible classes or columns whose type cannot be established
fail discovery instead of inventing a lossy schema.

## Configure a cursor source

A cursor requires a separate, non-null stable key backed by a single-column `PRIMARY KEY` or
non-partial `UNIQUE` constraint so rows with equal cursor values have a deterministic total order.
Use `cdf add`/catalog discovery to retain that uniqueness proof in the compiled schema:

```toml
[source.local]
kind = "sqlite"
location = "sqlite://data/events.sqlite"

[resource.events]
table = "events"
stable_key = "id"
cursor = { field = "updated_at", ordering = "exact", lag = "0ms" }
write_disposition = "merge"
primary_key = ["id"]
merge_key = ["id"]
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
  { name = "updated_at", type = "int64", nullable = false },
] }
```

Numeric cursors need no encoding. Date and timestamp cursors must declare exactly one compiled
storage encoding:

```toml
temporal_encodings = { updated_at = "unix_milliseconds" }
```

Supported values are `iso8601_text`, `unix_seconds`, `unix_milliseconds`, `unix_microseconds`, and
`unix_nanoseconds`. CDF never guesses a temporal encoding. The equivalent authoring command is:

```bash
cdf add local.events sqlite://data/events.sqlite \
  --option table=events \
  --option cursor=updated_at \
  --option stable_key=id \
  --option cursor_encoding=unix_milliseconds
```

Omit `cursor_encoding` for an integer cursor that is not a date or timestamp.

## Snapshot, schema, and pushdown behavior

Each partition uses one read-only SQLite connection and one explicit transaction held until the
Arrow stream reaches completion. A concurrent writer therefore cannot produce mixed versions
within one run. CDF does not force WAL mode; the database's existing journal mode determines
whether a writer can commit while the read snapshot remains open.

Table execution observes the live SQLite catalog inside the same read transaction used for data,
including resources with a declared schema. The declared schema remains the logical output
contract; it constrains and projects that physical observation rather than replacing it. Native
query execution validates the live output names/count against compiled authority and applies the
same runtime storage-class policy. Catalog discovery retains source column names and declared
SQLite types, and discovered resources pin the full and projected physical-schema hashes for drift
rejection. By default, a value outside the logical Arrow schema is a Data error. When the compiled
type policy enables coercion, the connector accepts only its explicit text, numeric, Boolean,
UTF-8, and binary conversions; lossy numeric conversion additionally requires the lossy-mapping
allowance. No conversion silently mutates the logical schema. Re-discover and deliberately update
the schema contract when physical output changes.

Projection is pushed to SQLite. Requested limits are enforced by the shared engine after residual
filtering and cursor-window handling; the adapter intentionally emits no SQL `LIMIT`, so an
inexact predicate or equal-cursor group cannot lose qualifying rows. Typed comparisons are pushed
exactly only where strict-table and Arrow semantics agree. String comparisons and predicates
without sufficient type authority remain inexact so the shared engine applies the residual
predicate.

## Operations and failures

- A missing database, missing table, incompatible storage value, or corrupt database is a Data
  failure. Error and debug output redact the database path.
- A locked or busy database is transient only where reopening is safe; otherwise CDF fails without
  an adapter-owned retry loop. Check long-lived writers and retry the run from the committed
  checkpoint.
- Permission, device, full-disk, out-of-memory, and host I/O failures are Environment failures.
- Invalid identifiers, unsupported schemas, missing stable-key declarations, ambiguous temporal
  encodings, and non-portable plans fail during contact-free compilation. Discovery retains
  conservative single-column primary-key/nonpartial-unique proof. Cursor execution re-observes
  the catalog inside its read snapshot and rejects an unproven or changed live constraint before
  querying data rows.

The adapter uses one accounted blocking lane, one connection, and bounded Arrow builders. It
checks cancellation every 256 returned rows and installs a SQLite VM progress handler every 8,192
virtual-machine operations, so query work before the next row is also interruptible. Variable
cells and the cumulative batch bound are checked before copying into Arrow. It has no private
runtime, pool, parallel snapshot reader, or unbounded queue.

## Measured local roofline

The release-mode 1,000,000-row warm local certificate records nine raw samples for both a table
scan and a CTE/join/window native query. Each CDF cell is compared with the same prepared statement,
explicit transaction, projection, ordering, type conversion, and Arrow consumption through direct
`rusqlite`. Each worker supplies its own CPU and peak-RSS counters. Portable physical-read-byte
counters are unavailable, so physical bytes are explicitly zero and no physical-throughput claim
is made. Reports bind the base Git revision, an enumerated workspace-content digest, and the
executable digest.

With the default 32,768-row batch target, the current nine-by-1M certificate measured 0.917 of
direct `rusqlite` for the table scan and 0.987 for the native query. The raw schema-v3 report is
[`2026-08-08-sqlite-source-roofline.json`](../.10x/evidence/.storage/2026-08-08-sqlite-source-roofline.json).
These are host-labelled warm-cache observations, not universal cold-cache or cross-platform
throughput claims.
