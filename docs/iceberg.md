# Iceberg source

CDF reads Apache Iceberg v1 and v2 tables whose data and delete files are
Parquet. Catalog bindings are local filesystem/Hadoop layout, Iceberg REST,
and AWS Glue. All three compile to the same snapshot, task, object-access, and
Parquet execution path.

An Iceberg resource names a namespace and table, plus an optional selector:
`current`, `branch`, `tag`, `snapshot`, or `timestamp`. Discovery reads catalog
and Iceberg metadata only; it does not sample data files. The selected schema
and exact snapshot authority are recorded in the plan and canonical task set.

Two scan modes are available:

- `snapshot` (default) reads the complete selected snapshot. Use an explicit
  snapshot, tag, or timestamp when the selection must remain fixed across
  separate commands.
- `append_snapshots` resumes from the committed snapshot only after proving
  that it is an ancestor of the selected snapshot and every intervening
  operation is `append`. Only files added by those snapshots are admitted.
  Missing or divergent history and `replace`, `overwrite`, or `delete`
  operations fail before source payload or destination mutation.

```toml
[source.lake]
kind = "iceberg"

[source.lake.catalog]
kind = "glue"
region = "us-west-2"

[resource.events]
namespace = ["analytics"]
table = "events"
mode = "append_snapshots"
```

Supported table semantics include schema and partition evolution, field-ID
projection, name mappings, position deletes, equality deletes, and exact
generation checks. Unsupported uses fail during planning: Iceberg v3,
ORC/Avro data files, encrypted metadata/data without a configured KMS reader,
deletion vectors, changelog output, and resident tailing. These are capability
boundaries, not silent fallbacks.

The source uses CDF's ordinary object-access, memory/spill, retry,
reconciliation, package, receipt, and checkpoint authorities. No catalog
binding owns a private runtime, credential chain, scheduler, or data plane.
