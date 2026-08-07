# Quickstart: NYC taxi data to DuckDB

This path starts with a public Parquet file, lets CDF discover and pin its schema, loads it into DuckDB, expands to a monthly file set with manifest incrementality, and shows how drift and replay remain evidence-preserving.

## Prerequisites

- A checkout of this repository and its pinned Rust toolchain.
- Network access to the NYC Taxi & Limousine Commission public dataset.
- Commands below run from the repository root.

Build the CLI once:

```bash
cargo build -p cdf-cli --locked
export CDF="$PWD/target/debug/cdf"
```

For deterministic local source examples, see [`examples/rest-fixture`](../examples/rest-fixture/README.md) and [`examples/postgres`](../examples/postgres/README.md). Both are executed by the conformance suite.

## 1. Create a project

```bash
WORKDIR="$(mktemp -d)"
"$CDF" init "$WORKDIR" --name tlc_quickstart
cd "$WORKDIR"
```

The project defaults to local SQLite state, local packages, and DuckDB. `cdf init` does not contact a source or create destination/state files.

## 2. Add January with no typed schema

```bash
"$CDF" add tlc.yellow \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

`cdf add` writes `cdf/tlc/yellow.cdf.sql` and the shared `[sources.tlc]`
connection in `cdf.toml`. `cdf compile tlc.yellow` then performs bounded
Parquet-footer discovery, normalizes source field names, stores a hash-addressed
snapshot as a derived cache under `.cdf/schemas/`, and establishes the immutable
active schema version in the selected environment's state store. Neither
command downloads Parquet data pages or writes a package, destination, or
checkpoint.

Inspect the active schema authority:

```bash
"$CDF" schema show tlc.yellow
"$CDF" compile tlc.yellow --locked
"$CDF" sql "select resource_id, output_schema_hash from manifest_resources"
"$CDF" plan tlc.yellow
```

`cdf compile [selectors...]` prepares only the selected resources, publishing
immutable artifacts under `.cdf/compiled/`, their status index in
`.cdf/manifest.json`, and per-resource schema authority in the environment's
state store. Add `--locked` to forbid first-use establishment and require an
existing unchanged authority. Generated `.cdf/` artifacts and local state are
ignored by the project scaffold.

The plan should report one file partition. Fields such as `VendorID` are planned as normalized destination identifiers while retaining `cdf:source_name = "VendorID"` evidence.

Run it:

```bash
"$CDF" run tlc.yellow
```

The successful run panel identifies the package, verified destination receipt, and committed checkpoint. CDF advances file state only after that receipt crosses the commit gate.

If the public CDN denies a request, verify the same URL with another HTTP client. CDF reports an upstream authorization/transport failure rather than treating it as schema drift. The deterministic local add/compiler fixture is always available with:

```bash
cargo test -p cdf-cli add_local_parquet_writes_query_resource_and_shared_source_configuration --locked
```

## 3. Expand to every 2024 month

Open `cdf/tlc/yellow.cdf.sql` and change only the `glob` argument:

```sql
RESOURCE
DISPOSITION APPEND
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT *
FROM upstream(
  source => 'tlc',
  glob => 'yellow_tripdata_2024-*.parquet',
  format => 'parquet'
);
```

The generated `[sources.tlc]` block already points at the public `trip-data` prefix and contains the host egress allowlist.

Review drift, then promote intentional schema changes explicitly:

```bash
"$CDF" schema diff tlc.yellow
"$CDF" schema promote tlc.yellow
"$CDF" schema promote tlc.yellow --execute
"$CDF" compile tlc.yellow
"$CDF" plan tlc.yellow
"$CDF" run tlc.yellow
```

The plan has one logical partition per matched month. CDF keeps those identities separate even when a future executor packs small files into shared worker tasks. After the run:

```bash
"$CDF" state show tlc.yellow
```

The state view summarizes the committed `FileManifest`. Running the same command again is a fast no-op. If a new matching month appears, only that new or changed identity is planned and committed.

The deterministic multi-file/no-op/new-file proof is:

```bash
cargo test -p cdf-project file_manifest_append_run_skips_unchanged_files_and_loads_only_changes --locked
```

## 4. What happens when a later file drifts

The active schema version does not mutate silently. Every current file is reconciled against that authority and the resource contract:

- lossless width changes compile into recorded coercion verdicts;
- compatible physical types use compiler-approved lossless coercions without changing authority;
- unknown or incompatible ordinary values follow the active field disposition, while an
  incompatible partition under the governed preset produces typed quarantine evidence naming the
  file, field, physical type, expected type, rule, and remediation;
- a quarantined file identity is marked processed only after its quarantine package receives a verified destination receipt.

The run remains successful when policy admits quarantine; it does not collapse into a decoder stack trace. Review current authority with:

```bash
"$CDF" schema diff tlc.yellow
"$CDF" inspect resources
```

To exercise the incompatible-month rendering without depending on mutable public data:

```bash
cargo test -p cdf-cli governed_quarantines_incompatible_partition_with_exact_arrow_field_evidence --locked
```

Promote the active schema only after reviewing the diff:

```bash
"$CDF" schema promote tlc.yellow
"$CDF" schema promote tlc.yellow --execute
```

## 5. Run from a package without source contact

List package identities and choose the package to replay:

```bash
"$CDF" package ls
```

Deliver it into a clean local project/ledger so the original checkpoint identity does not collide:

```bash
REPLAY_WORKDIR="$(mktemp -d)"
"$CDF" init "$REPLAY_WORKDIR" --name tlc_replay
"$CDF" --project "$REPLAY_WORKDIR" run \
  --package "$WORKDIR/.cdf/packages/<package-id>" \
  --to duckdb://.cdf/replay.duckdb
```

Package execution verifies the stored package and manifest, writes through the destination protocol, records a new receipt, and commits the package's checkpoint delta without contacting the TLC source.

## 6. Run focused conformance checks

Run the query-first preview/run parity and multi-file partition laws with:

```bash
cargo test -p cdf-conformance preview_run_parity_covers_supported_archetypes --locked
cargo test -p cdf-conformance multifile_preview_traverses_the_same_planned_partitions_as_run --locked
```

Clean up when finished:

```bash
rm -rf "$WORKDIR" "$REPLAY_WORKDIR"
```
