Status: active
Created: 2026-08-08
Updated: 2026-08-09

# MongoDB native extraction surface

## Purpose and authority

This specification extends `.10x/specs/mongodb-collection-source.md` with the user-ratified
connector-native extraction surface. It supersedes that specification's aggregation-pipeline
exclusion. MongoDB 7.0+ and all existing BSON, schema-depth, drift, memory, checkpoint, and roofline
requirements remain authoritative.

## Resource contract

A MongoDB resource MUST select exactly one configured database and collection. It MAY define one
of these native input forms:

- `filter`: an Extended JSON document used as the base `find` selector;
- `pipeline`: an Extended JSON array used as the aggregation pipeline.

`filter` and `pipeline` are mutually exclusive. Their JSON is parsed as BSON, rejects duplicate
keys and malformed/over-bound structure, and is canonicalized before compiled identity. A pipeline
MUST reject `$out`, `$merge`, `$changeStream`, and nested occurrences of write/change-stream stages
before contact. Other server-supported read stages, including joins, unions, search, grouping, and
source-native expressions, are allowed when the configured role/server permits them.

The configured source MAY provide defaults for `schema_depth`, `discovery_records`,
`discovery_bytes`, `cursor_batch_rows`, `output_batch_rows`, `max_time_ms`, `read_concern`, and
`read_preference`. A resource MAY override any of those defaults. Resolution order is built-in
default, then source default, then explicit resource override. The resource MAY additionally set
the following query-specific controls, which never inherit: `allow_disk_use`, `hint`, `collation`,
`let`, and `comment`.

The adapter-owned controls are:

| Option | Contract |
| --- | --- |
| `schema_depth` | `1..=32`, default `1` |
| `discovery_records` | `1..=100000`, default `1000` |
| `discovery_bytes` | `1024..=67108864`, default `16777216` |
| `cursor_batch_rows` | `1..=100000`, default `8192`; MongoDB wire cursor request |
| `output_batch_rows` | `1..=100000`, default `65536`; Arrow decode/output ceiling |
| `max_time_ms` | optional `1..=3600000`; server operation deadline |
| `allow_disk_use` | aggregation-only boolean, default `false` |
| `hint` | optional Extended JSON string or document |
| `collation` | optional Extended JSON collation document |
| `let` | optional Extended JSON document of aggregation variables |
| `comment` | optional UTF-8 server profiler comment, at most 1024 bytes |
| `read_concern` | optional MongoDB read-concern level supported by the server |
| `read_preference` | optional MongoDB read-preference mode with optional bounded tag sets |

The former ambiguous source option `batch_rows` is deleted. It has not shipped and receives no
alias or compatibility reader. Pool size and bounded in-flight stream capacity remain
source/connection-only controls because they govern shared transport.

## Discovery and execution

Discovery MUST run the exact resource filter or pipeline with the exact collation, hint, variables,
read concern, and read preference used by execution, adding only its bounded sampling limit. The
discovery evidence records the resolved limits and a secret-safe canonical native-input hash.
Changing any native input or control changes source/plan identity.

The native input produces the relation consumed by the surrounding CDF SQL resource. CDF logical
projection, exact filters, order, limit, and cursor bounds apply after that relation. A `find`
resource MAY combine exact predicates with its base filter through typed BSON `$and`. An aggregate
resource MAY append exact `$match`, `$sort`, and `$limit` stages. Inexact operations remain engine
residuals. CDF MUST NOT reorder an authored pipeline stage.

Cursor execution over a pipeline is allowed only when the pipeline output retains the compiled
cursor and stable-key fields with compatible types. Nondeterministic output may run as a bounded
full replacement but MUST fail incremental planning with an actionable diagnostic. Collection
dependencies visible in `$lookup` and `$unionWith`, including nested pipelines, MUST participate in
portable source-generation attestation; a dependency that cannot be enumerated makes the plan
non-portable rather than silently under-attested.

The adapter uses the official raw BSON cursor for `find` and aggregate output. Wire cursor rows and
Arrow output rows are independent bounds. Both paths retain byte admission, cancellation, bounded
queues, process-local progress, schema normalization, variant/quarantine policy, and receipt-gated
checkpoint authority.

## Safety and diagnostics

Native BSON input is authored project code and may appear redacted-by-structure in explain output,
but diagnostics MUST NOT echo literal values. Credentials and secret references never enter the
native input, manifest, compiled plan, or report. Server errors retain MongoDB provenance and
resource option names without rendering the full filter/pipeline.

Unsupported option combinations fail before contact: `allow_disk_use`/`let` with `find`, malformed
hint/collation/tag sets, write stages, invalid numeric bounds, or cursor fields absent after a
pipeline. Server permissions remain the authority for read access and source-native functions.

## Acceptance scenarios

- A filtered `find` resource discovers and executes the same subset; its filter hash and resolved
  cursor/output/discovery controls survive plan export/import without literal leakage.
- An aggregation with `$match`, `$lookup`, `$group`, and `$project` discovers the pipeline output,
  runs read-only, and supports outer CDF SQL against that output.
- Write/change-stream stages are rejected recursively before source contact.
- Source-default and resource-specific sample and cursor batch sizes change their respective server operations but do
  not change package bytes when logical output is identical.
- A pipeline cursor succeeds when cursor/key fields survive and fails before execution otherwise.
- Atlas/local live tests cover find, aggregation, discovery bounds, option validation, package,
  replay, checkpoint, cancellation, progress, and a throughput comparison to the equivalent
  official-driver operation.

## Exclusions

Finite aggregation does not replace the separately governed MongoDB change-stream CDC mode.
Server-side writes, map-reduce output collections, implicit credentials inside native JSON, and
unbounded sampling/queues remain excluded.
