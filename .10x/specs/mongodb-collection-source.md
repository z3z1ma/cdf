Status: active
Created: 2026-08-02
Updated: 2026-08-08

# MongoDB collection source

## Purpose and scope

This specification governs a first-party finite MongoDB collection source on MongoDB 7.0 or
later. It is further governed by `.10x/specs/source-extension-runtime-contract.md`,
`.10x/specs/resource-authoring-planning-batches.md`, and
`.10x/decisions/non-file-window-close-checkpoint-semantics.md`. Exact numeric fallbacks are governed
by `.10x/decisions/exact-value-text-fallbacks.md`.

The user ratified finite snapshot/cursor extraction and deferred change streams to a separate CDC
tranche on 2026-08-02. On 2026-08-08 the user superseded the initial 8.0 floor with MongoDB 7.0+
after the authorized Atlas environments were observed at 7.0.40. On the same date the user
superseded recursive schemaless discovery with resource-scoped depth, defaulting to top-level-only
discovery.

## Source contract

`cdf-source-mongodb` MUST be a leaf adapter with driver id and source kind `mongodb`, and
`mongodb`/`mongodb+srv` schemes. Credentials remain secret references. The compiled plan contains
only redacted topology/database authority, a validated collection target, logical
projection/filter/cursor policy, schema authority, and client capability floor. Compilation and portability validation
perform no contact.

The initial resource is one collection. Discovery MUST combine bounded collection metadata,
validator metadata when available, and bounded raw-document observation through the existing
schema discovery/freeze authority. Discovery MUST report sample limits and cannot imply that an
unvalidated schemaless collection is globally uniform.

Each resource MAY set `schema_depth` in its `upstream(...)` options. It MUST be an integer in
`1..=32` and defaults to `1`. The root BSON document's fields are level 1; traversing into a
document child or array element consumes one level. The resolved value MUST participate in source
identity, observation evidence, compiled-plan identity, and cache keys. It is not ambient source
configuration and one configured source MAY serve resources with different depths.

Discovery MUST retain field names only through the configured depth. A document or array at the
depth boundary is one opaque value; discovery MUST NOT inspect its keys or element shapes for
schema inference. Structural parsing may still enforce byte, element, nesting, duplicate-key, and
malformed-BSON safety bounds, but those checks MUST NOT accumulate nested schema fields.

At any retained level, consistent primitive BSON values infer their exact supported Arrow type;
BSON Int32 and Int64 MAY reconcile to Int64. Null makes the field nullable. Documents and arrays
within the retained depth may become structs and lists, but their descendants at the boundary are
opaque. If sampled non-null values for one field cannot reconcile to one typed shape, discovery
MUST retain the entire field as an opaque BSON value rather than invent a union or fail because of
ordinary schemaless heterogeneity. Unsupported BSON scalar kinds continue through explicit
variant/quarantine/fail policy rather than silently becoming strings.

Execution MUST use one reusable official asynchronous Rust `Client` and its native topology and
connection pools. It MUST consume a raw BSON cursor in wire batches and build byte-accounted Arrow
arrays without first materializing `Document`/JSON trees for the full batch. The adapter does not
create a client per partition, private executor, unbounded queue, semaphore, or retry loop.

## Query and cursor semantics

Limit, supported comparison filters, and cursor ranges compile to typed BSON filters. Logical
projection controls governed Arrow materialization, but execution reads complete BSON documents so
unknown-field drift remains observable; it MUST NOT use server projection to hide source shape.
A filter is exact only when MongoDB missing/null, array, numeric comparison, collation, and timezone
semantics match Arrow; otherwise CDF reapplies it. Field paths and collection names are validated,
and values are BSON bindings rather than JSON/string fragments.

Snapshot reads and numeric, timestamp, and date cursor incrementality are supported. A cursor
query MUST sort by the cursor field and a stable `_id` tie-breaker. BSON numeric values map to typed
numeric cursors; BSON DateTime maps to timestamp cursors; a schema-pinned canonical date field maps
to date cursors. Strings are not inferred as dates or timestamps. Window-close, lag, and checkpoint
advancement use shared semantics. ObjectId-only, resume-token, page-token, mixed, missing-key, and
unsupported cursor forms fail before execution.

Finite reads do not claim a cross-shard transaction snapshot unless the selected read concern and
deployment can prove it. The default contract is an idempotently reopenable bounded cursor window;
the cursor plus stable key prevents omission at page boundaries, and overlapping retry duplicates
remain governed by package identity/dedup. A later query instance is never spliced into an earlier
one while claiming one server snapshot.

## BSON-to-Arrow mapping

The source MUST map BSON exactly as follows where representable: bool; signed integers; double;
string; binary; DateTime to UTC millisecond timestamp; ObjectId to 12-byte fixed-size binary with
`cdf:semantic=mongodb.object_id@1`; arrays and documents within the configured retained depth to
lists and structs; boundary or heterogeneous values to canonical Extended JSON UTF-8; and BSON
null to Arrow nullability. Regex, JavaScript,
DBPointer, MinKey/MaxKey, undefined, symbols, timestamps used as replication tokens, duplicate
document keys, heterogeneous arrays, and values outside the pin MUST follow explicit variant or
quarantine policy and otherwise fail unless the field is already governed by the explicit opaque
Extended JSON contract. There is no untagged Extended JSON stringification.

Opaque document, array, and heterogeneous-value fields MUST use versioned MongoDB semantic tags
on Arrow `Utf8` plus exact `cdf:physical_type` evidence. Their encoding MUST be deterministic
MongoDB Canonical Extended JSON, preserve BSON type/value meaning, reject duplicate document keys,
and remain bounded by the ordinary decoder/output budgets. A destination with no proven native
semi-structured mapping receives the UTF-8 value. Native JSON/JSONB/VARIANT selection is allowed
only through an explicit lossless semantic mapping; destination behavior MUST NOT change discovery
or package bytes.

Once a consistent primitive field is frozen, a later incompatible source value follows the
compiled drift disposition: safe capture emits typed null plus exact `_cdf_variant` evidence,
quarantine rejects the row into quarantine, and fail stops the run. New keys or element shapes
inside an opaque value are not schema drift and MUST NOT create columns or residual candidates.

BSON Decimal128 maps to Arrow Decimal128 only when validator or user-declared schema authority
proves that the complete field domain fits one Arrow precision and scale. Schemaless observation
alone cannot prove that bound. Otherwise it maps to canonical exact `Utf8`, including native
special values, with `cdf:physical_type` retained and
`cdf:semantic=mongodb.decimal128_value_text@1`. This scalar spelling is the BSON Decimal128 value
contract, not Extended JSON. Decimal128 never becomes floating point; a value outside a pinned Arrow
decimal domain follows the explicit drift policy or fails before publishing a partial batch.

Unknown fields and shape drift use the existing residual/schema policy. Original field names and
BSON semantic annotations remain in Arrow metadata. Decimals never become floats and DateTime is
not given a local timezone.

## Execution and performance

Cursor batch size, logical projection, raw decode, pool size, and bounded in-flight work are selected
from measured capability data and injected host/memory pressure. One poll admits at most 64 MiB of
raw BSON plus a 128 MiB decode working set, including construction scratch and retained drift
evidence, before a retained output of at most 64 MiB crosses the source frontier. Cancellation
closes the cursor and joins all admitted tasks. The direct-library roofline follows
`.10x/specs/database-connector-roofline.md`.

## Scenarios and acceptance criteria

- Bounded discovery reports its evidence limit and execution applies the frozen schema to later
  heterogeneous documents without silent widening.
- With omitted `schema_depth`, a top-level document whose sampled values contain distinct UUID-like
  keys produces one opaque UTF-8 field and no UUID-named schema fields.
- With `schema_depth = 2`, direct children of a top-level document may be typed, while documents or
  arrays below that boundary remain opaque; invalid zero, negative, non-integer, and greater-than-32
  values fail as resource contract errors without contact.
- A heterogeneous sampled field becomes one opaque Extended JSON field. A consistent sampled
  primitive remains typed, and a later incompatible value follows variant/quarantine/fail policy
  without changing the active schema.
- Discovery-generated resource SQL enumerates the retained top-level projection. It never emits
  sampled map keys beyond `schema_depth`.
- Duplicate cursor values across wire batches remain complete and deterministic through `_id`.
- A partial cursor/network failure cannot advance the checkpoint; retry uses the same typed window.
- Projection/filter fidelity covers missing versus null, arrays, collation, numeric subtypes, and
  UTC DateTime adversarial cases.
- Source add/discovery/plan/preview/run/replay/redaction/cancellation/jobs-invariance, live BSON
  mapping, and connector certification pass.
- Decimal128 coverage proves schema-pinned Arrow decimals, schemaless tagged exact text, special
  values, and out-of-pin failure without floating-point conversion.
- The source macro cell meets the 0.90 official raw-BSON direct-library roofline.

## Explicit exclusions

Change streams, resume tokens, update/delete CDC operations, `cdc_apply`, ObjectId cursor positions,
arbitrary aggregation pipelines, map-reduce, unbounded discovery depth, and untagged Extended JSON
coercion are excluded.
