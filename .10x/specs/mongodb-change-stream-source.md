Status: active
Created: 2026-08-09
Updated: 2026-08-09

# MongoDB change-stream source

## Purpose

Extend the existing first-party `mongodb` source with receipt-gated change-stream CDC for one
collection or one database. Database watches support both a homogeneous lossless envelope and a
typed routed family whose independently discovered schemas retain MongoDB's top-level field
inventory. Neither mode infers one union schema across unrelated collections.

The user ratified collection-scoped and database-scoped watches as first-class use cases on
2026-08-09. The generic target syntax and folding/settlement laws remain governed by
`.10x/specs/routed-destination-target-families.md`.

## Authoring contract

MongoDB CDC remains a mode of the existing source kind:

```sql
RESOURCE
TARGET warehouse.orders
DISPOSITION CDC_APPLY(_id)
DELETE HARD
EXECUTION DRAIN (...)
AS
SELECT _id, account_id, total
FROM upstream(
  source => 'mongo',
  mode => 'cdc',
  bootstrap => 'latest',
  collection => 'orders'
);
```

A database watch omits `collection`, declares `watch => 'database'`, and MUST use the protected
`source_collection` route field:

```sql
RESOURCE
TARGET warehouse.events
ROUTE BY source_collection MAX TARGETS 256
DISPOSITION CDC_APPLY(_id)
DELETE HARD
EXECUTION DRAIN (...)
AS
SELECT *
FROM upstream(
  source => 'mongo',
  mode => 'cdc',
  watch => 'database',
  representation => 'typed',
  include_collections => ARRAY ['orders', 'invoices_*'],
  exclude_collections => ARRAY ['invoices_tmp_*'],
  bootstrap => 'latest'
);
```

The envelope alternative keeps the same route declaration but selects the stable control/data
envelope and uses its canonical document key:

```sql
RESOURCE
TARGET warehouse.events
ROUTE BY source_collection MAX TARGETS 256
DISPOSITION CDC_APPLY(document_key)
DELETE HARD
EXECUTION DRAIN (...)
AS
SELECT source_database, source_collection, document_key, document
FROM upstream(
  source => 'mongo',
  mode => 'cdc',
  watch => 'database',
  representation => 'envelope',
  include_collections => ARRAY ['orders', 'invoices_*'],
  exclude_collections => ARRAY ['invoices_tmp_*'],
  bootstrap => 'latest'
);
```

`mode` is `snapshot` when omitted and `cdc` when explicit. A CDC resource MUST explicitly select
`bootstrap => 'snapshot'` or `bootstrap => 'latest'`; the generic continuous-run contract owns the
meaning after a first receipt-gated checkpoint. `watch` defaults to `collection` only when a
nonempty `collection` is supplied. `watch => 'database'` forbids `collection`. Cluster watches are
not part of this version.

The adapter MAY accept a resource-level `change_pipeline` as canonical BSON Extended JSON for a
bounded list of read-only event filters. The compiled pipeline is identity-bearing. It MUST NOT
remove, rewrite, or synthesize `_id`, `operationType`, `ns`, `documentKey`, or `fullDocument`, and
it MUST NOT contain output-writing, lookup, union, JavaScript, or nondeterministic stages.
`change_batch_rows`, `change_max_await_ms`, and `comment` are ordinary source defaults with
resource overrides. Correctness options (`fullDocument: required`, resumability, and expanded-event
policy) are fixed compiled behavior rather than weakening knobs.

`representation` is `typed` or `envelope`. The selected representation, collection admission
patterns, exact discovered collection inventory, and schema/output authorities are
identity-bearing. Collection admission uses ordered arrays of adapter-owned glob patterns;
patterns without metacharacters are exact names. Exclusion is applied after inclusion. Omitted
inclusion admits every eligible ordinary collection and omitted exclusion removes none. Duplicate,
empty, invalid, system, view, time-series, encrypted, or otherwise unsupported collection entries
fail or are reported explicitly rather than being silently watched under a false capability claim.

Database watches default to `representation => 'typed'`; `envelope` is always explicit. Collection
watches use the ordinary typed representation. `schema_depth` retains the existing `1..=32`
resource option, defaults to `1`, and applies independently to every collection discovered in a
typed database watch.

## Collection-watch row contract

Discovery and schema authority reuse `.10x/specs/mongodb-collection-source.md`. Inserts and
replaces decode `fullDocument`; updates require exact post-images through
`fullDocument: "required"`; deletes decode only the declared `CDC_APPLY` keys from `documentKey`.
Insert and replace map to insert/update respectively. The adapter MUST prove every requested key is
present and exactly decodable before publishing the event prefix.

Collection preflight MUST prove MongoDB 7.0+, replica-set or sharded change-stream topology, and
`changeStreamPreAndPostImages.enabled = true` for the collection. Missing/expired post-images,
missing keys, invalidation, drop, rename, DDL/expanded events, and incompatible schema drift fail
before the affected resume token can advance.

## Generic routed schema families

The existing schema authority key is extended with a generic output binding:

```text
project id + environment + resource id + output binding id
```

An unrouted resource has the distinguished `primary` output binding. A routed resource derives an
output binding from the route field's typed canonical value and folding version. It MUST NOT key
schema authority by a destination-specific physical table name. The complete ordered binding map,
schema generations/hashes, and target map are compiled plan, package, receipt, checkpoint, and
replay authority.

Each output binding may have a distinct logical Arrow schema, drift disposition, promotion
history, destination migration plan, and installation record. One routed package may consequently
contain schema-homogeneous segments per output while remaining one atomic package/receipt and one
source checkpoint. This is a generic runtime capability and is not owned by MongoDB or CDC.

The authored relational query is polymorphic for a heterogeneous routed family: CDF analyzes and
compiles the same authored query independently against every admitted output schema. `SELECT *`
therefore retains each collection's own discovered fields. Every explicit projection, predicate,
function, semantic binding, and disposition key MUST resolve and typecheck for every admitted
output. Collection-specific transformations belong in separately authored resources until a
distinct route-override language is specified.

## Database-watch typed representation

Typed discovery MUST list the database's eligible collections once, apply the compiled
include/exclude filters, sort the admitted names by exact UTF-8 bytes, and run the ordinary bounded
MongoDB collection discovery against each admitted collection with bounded concurrency and shared
memory/network budgets. Discovery reads collection documents; it never waits for or consumes a
change stream. Each collection becomes one routed output binding with its own discovery evidence
and schema authority. The configured `schema_depth` is applied independently to every collection;
it defaults to top-level-only depth `1` but is not otherwise restricted beyond the existing
`1..=32` contract. Discovery record/byte bounds, validators, BSON mapping, opaque boundary values,
and drift behavior remain governed by the ordinary collection source.

Discovery is bounded per collection and reports both per-collection and aggregate evidence. It
does not claim that sampled schemaless data is complete. A database with no admitted collections,
an admitted collection that yields no usable schema, or an inventory above `MAX TARGETS` fails
without partially publishing the routed schema family.

Execution decodes each event using the active schema for its collection output. The protected
`source_collection` routing authority travels beside the typed batch even when it is not a physical
destination column. Inserts/updates carry exact post-images; deletes carry the collection's
declared exact destination key. A change for a collection/output binding absent from the compiled
family fails before publishing that event prefix or advancing its token. Runtime never silently
creates schema authority, migrations, or targets from the distribution of observed changes.

## Database-watch envelope representation

Envelope discovery validates the admitted collection inventory but does not infer a union or
recursively inspect event payloads. Every output binding uses one fixed top-level schema:

- `source_database`: non-null UTF-8;
- `source_collection`: non-null UTF-8 and protected route authority;
- `document_key`: non-null deterministic MongoDB Canonical Extended JSON UTF-8 with a versioned
  MongoDB document-key semantic tag;
- `document`: non-null deterministic MongoDB Canonical Extended JSON UTF-8 with the existing
  opaque-document semantic tag for insert/update, absent from the mechanically derived key-only
  delete schema.

The envelope is lossless for BSON type/value meaning and intentionally opaque below its top level.
It prevents unbounded schema growth from arbitrary nested map keys and lets destinations with a
proven JSON/JSONB/VARIANT mapping retain semi-structured values natively. The route field is
control data and MAY be omitted from physical destination rows only after the generic router has
resolved the target.

The generic target family derives each physical target from the logical base target and the
`source_collection` value in either representation. A database stream with two collection values
remains one resource, package, receipt, and resume-token checkpoint. All routed targets settle
atomically under the generic destination contract.

## Resume and event-prefix lifecycle

The adapter MUST use `ResumeTokenPosition::MongoChangeStream`. Scope binds configured source name,
watch level and namespace, canonical pipeline hash, and a hash of all semantics-changing options.
Resume tokens preserve exact BSON bytes and source provenance. Ordinary events use `resumeAfter`;
an invalidate token is retained only as `startAfter` authority and the unsupported invalidation
event still terminates the current resource before mutation.

One source event is one proven event-prefix settlement unit. The adapter emits a begin marker,
one homogeneous insert/update/delete batch, and a terminal marker carrying the exact event token.
It MAY batch adjacent events only when it preserves source order, operation homogeneity, exact
terminal-token authority, memory bounds, and route assignment. Post-batch tokens MAY close an
explicit empty scanned prefix only through the neutral no-row frontier path; they never fabricate a
data event.

The runtime advances a resume token only after package publication, complete routed destination
receipt verification, and checkpoint commit. Retry resumes from the prior committed token. The
driver's automatic resumability may heal an in-flight cursor only while it cannot cross CDF's
receipt-gated authority or hide a history-loss error.

## Bootstrap

`latest` opens the scoped change stream, obtains a source-issued event or post-batch token, records
that token as the intentional starting frontier, and excludes earlier data. It MUST NOT substitute
wall-clock time or an unordered cluster timestamp for a resume token checkpoint.

`snapshot` is an integrated source-owned handoff. Collection scope opens the stream first, records
its start token, performs an exact collection snapshot under the pinned schema, then drains changes
after that token. Database scope opens one database stream first and snapshots the compiled
admitted collection set in canonical name order. Changes observed during snapshot are retained in
bounded, accounted spill/replay storage; exceeding the compiled spill authority fails with no
checkpoint. A newly matching collection is not silently admitted: its first event fails before its
token advances until explicit discovery/compilation establishes its output binding and schema.

## Failure and error ownership

- authored scope/pipeline/batch/await/bootstrap errors are `Contract` and fail without contact;
- authentication remains `Auth`; rate limiting and retryable server-selection/network failures
  preserve driver retry metadata and source provenance;
- malformed events, missing post-images/keys/tokens, resume-history loss, and unsupported source
  events are `Data`;
- local DNS/TLS/runtime/physical resource failures retain `Environment` ownership;
- adapter state-machine or counter invariants alone are `Internal`.

Diagnostics never echo endpoint credentials, resume-token bytes, document payloads, or sensitive
pipeline values.

## Acceptance scenarios

1. A collection watch applies exact insert/update/delete effects to one target and resumes from the
   receipt-covered event token without replaying a committed effect.
2. A typed database watch discovers two collections independently, compiles `SELECT *` twice, and
   routes their distinct schemas to deterministic physical targets under one atomic
   receipt/checkpoint; a failed target advances neither route nor token.
3. An envelope database watch never infers nested payload keys or creates columns from
   heterogeneous collections; Canonical Extended JSON round-trips representative BSON values.
4. `latest` records a source-issued token and visibly excludes history; `snapshot` has no gap
   between its admitted snapshot and stream.
5. Required post-image, topology, history-loss, invalidation, missing-key, malformed-token, and
   cancellation failures preserve typed provenance and advance no checkpoint.
6. Event batching/rechunking, requested jobs, retries, and routed target order do not change package
   bytes, route identities, or the terminal resume token.
7. Exact/include/exclude collection admission, an unseen newly matching collection, empty/oversized
   inventories, and per-route schema drift all fail or reconcile exactly as compiled.
8. Focused synthetic tests, local replica-set tests, Atlas collection-watch tests, both database
   representations, crash recovery, bounded-memory evidence, and a release throughput comparison
   pass.

## Explicit exclusions

- cluster-wide watches;
- sparse patch updates or `updateLookup`;
- source multi-document transaction atomicity claims;
- target-name template interpolation beyond generic `TARGET` plus `ROUTE BY`;
- per-route SQL override blocks inside one resource;
- silent drop/rename/invalidate/DDL handling or a default/overflow route.

## Ratification

The user ratified both `typed` and `envelope` database-watch representations on 2026-08-09.
`typed` is the default; an envelope is explicit. Typed discovery deterministically enumerates and
discovers admitted collections without consuming the stream, keys schema authority by resource
and output binding, applies the existing configurable `schema_depth` independently per collection
with default `1`, and compiles one authored query independently for every route. A newly matching
collection fails before token advancement until explicit discovery/compile establishes authority.
Collection-specific transforms remain separate resources.

## References

- `.10x/specs/cdc-log-source-foundation.md`
- `.10x/specs/cdc-source-position-artifacts.md`
- `.10x/specs/cdc-resource-authoring-and-continuous-run.md`
- `.10x/specs/mongodb-collection-source.md`
- `.10x/specs/routed-destination-target-families.md`
- `.10x/specs/retention-aware-package-collection.md`
- `.10x/research/2026-08-03-cdc-protocol-position-contract.md`
