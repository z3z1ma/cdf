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

`cdf add` performs bounded Parquet-footer discovery, normalizes source field names, writes `resources/tlc.toml`, stores a hash-addressed snapshot under `.cdf/schemas/`, and references it from `cdf.lock`. It does not download Parquet data pages or write a package, destination, or checkpoint.

Inspect what was pinned:

```bash
"$CDF" schema show tlc.yellow
"$CDF" plan tlc.yellow
```

The plan should report one file partition. Fields such as `VendorID` are planned as normalized destination identifiers while retaining `cdf:source_name = "VendorID"` evidence.

Run it:

```bash
"$CDF" run tlc.yellow
```

The successful run panel identifies the package, verified destination receipt, and committed checkpoint. CDF advances file state only after that receipt crosses the commit gate.

If the public CDN denies a request, verify the same URL with another HTTP client. CDF reports an upstream authorization/transport failure rather than treating it as schema drift. The deterministic S1 fixture is always available with:

```bash
cargo test -p cdf-cli p2_s1_add_http_parquet_pins_and_runs_with_zero_typed_fields --locked
```

## 3. Expand to every 2024 month

Open `resources/tlc.toml` and change only the resource glob:

```toml
[resource.yellow]
glob = "yellow_tripdata_2024-*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
```

The generated `[source.tlc]` block already points at the public `trip-data` prefix and contains the host egress allowlist.

Refresh the intentional schema authority and review its diff:

```bash
"$CDF" schema diff tlc.yellow
"$CDF" schema pin tlc.yellow
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
cargo test -p cdf-cli p2_s2_http_month_glob_is_incremental_and_no_change_is_a_noop --locked
```

## 4. What happens when a later file drifts

The pinned snapshot does not mutate silently. Every current file is reconciled against the baseline and the resource contract:

- lossless width changes compile into recorded coercion verdicts;
- compatible evolution produces a separately identified effective schema;
- incompatible fields/files produce typed quarantine evidence naming the file, field, physical type, expected type, rule, and remediation;
- a quarantined file identity is marked processed only after its quarantine package receives a verified destination receipt.

The run remains successful when policy admits quarantine; it does not collapse into a decoder stack trace. Review current authority with:

```bash
"$CDF" schema diff tlc.yellow
"$CDF" inspect resources
```

To exercise the incompatible-month rendering without depending on mutable public data:

```bash
cargo test -p cdf-cli governed_evolve_quarantines_incompatible_file_with_exact_arrow_field_evidence --locked
```

Refresh the baseline only after reviewing the diff:

```bash
"$CDF" schema pin tlc.yellow
```

## 5. Replay a package without source contact

List package identities and choose the package to replay:

```bash
"$CDF" package ls
```

Replay into a clean local project/ledger so the original checkpoint identity does not collide:

```bash
REPLAY_WORKDIR="$(mktemp -d)"
"$CDF" init "$REPLAY_WORKDIR" --name tlc_replay
"$CDF" --project "$REPLAY_WORKDIR" replay package \
  "$WORKDIR/.cdf/packages/<package-id>" \
  --to duckdb://.cdf/replay.duckdb
```

Replay verifies the stored package and manifest, writes through the destination protocol, records a new receipt, and commits the package's checkpoint delta without contacting the TLC source.

## 6. Verify the complete P2 contract

The P2 registry and conformance-owned laws cover all eight data-onramp golden paths. Run them with:

```bash
cargo test -p cdf-conformance p2_ --locked
```

Run every source-owned fixture named by that registry with the workspace suite:

```bash
cargo test --workspace --locked
```

Clean up when finished:

```bash
rm -rf "$WORKDIR" "$REPLAY_WORKDIR"
```
