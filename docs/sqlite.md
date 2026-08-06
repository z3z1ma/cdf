# SQLite table source

CDF reads one local SQLite table through the built-in `sqlite` source. This connector is distinct
from CDF's SQLite checkpoint store: source configuration uses `kind = "sqlite"` and a
`sqlite://` database location, while checkpoint state remains an environment concern.

## Configure a snapshot source

Keep the database below the project root so compiled plans remain portable:

```toml
[source.local]
kind = "sqlite"
location = "sqlite://data/events.sqlite"

[resource.events]
table = "events"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
] }
```

The initial connector reads tables only. It does not accept arbitrary SQL, attach databases,
change persistent pragmas, install triggers, or copy the database before reading it. Identifiers
are validated and quoted by the adapter; predicate and cursor values are bound parameters.

`cdf add` can author the same source after validating that the file is inside the project:

```bash
cdf add local.events sqlite://data/events.sqlite --option table=events
cdf plan local.events
cdf plan local.events
cdf preview local.events --limit 20
cdf run local.events
```

An absolute path can be used by a coordinator-local compile, but portable-plan admission rejects
it. Prefer a project-relative location from the start. Network filesystems are outside the
supported contract.

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

Every execution observes the live SQLite catalog inside the same read transaction used for data,
including resources with a declared schema. The declared schema remains the logical output
contract; it constrains and projects that physical observation rather than replacing it. Catalog
discovery retains source column names and declared SQLite types, and discovered resources also pin
the full and projected physical-schema hashes for drift rejection. SQLite storage classes remain
dynamic at runtime. By default, a value outside the logical Arrow schema is a Data error. When the
compiled type policy enables coercion, the connector accepts only its explicit text, numeric,
Boolean, UTF-8, and binary conversions; lossy numeric conversion additionally requires the
lossy-mapping allowance. No conversion silently mutates the logical schema. Re-discover and
deliberately update the schema contract when the physical table changes.

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

The release-mode 1,000,000-row warm local cell records five raw samples for the production path
and the same prepared-statement, explicit-transaction, projection, ordering, type conversion, and
Arrow-consumption work through direct `rusqlite`. Each worker supplies its own CPU and peak-RSS
counters. Portable physical-read-byte counters are unavailable, so physical bytes are explicitly
zero and no physical-throughput claim is made. Reports bind the base Git revision, an enumerated
workspace-content digest, and the executable digest.

The last passing 32K-row-batch baseline measured a 0.917 ratio, but it predates the final schema
and module repairs and is not current closure evidence. Two current 32K-row-batch observations
measured 0.873 and 0.885, below the required 0.900 roofline. A bounded 64K-row-batch experiment
measured 0.889 and was reverted because it did not clear the gate and increased peak RSS. The raw
report currently stored at
[`2026-08-02-sqlite-source-roofline.json`](../.10x/evidence/.storage/2026-08-02-sqlite-source-roofline.json)
is that failed diagnostic experiment, not a passing or current-source certificate. These are
host-labelled local observations, not universal throughput claims. One fresh current-source
measurement is deferred to the final integration gate.
