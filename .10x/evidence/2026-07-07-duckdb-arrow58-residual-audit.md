Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Relates-To: .10x/tickets/done/2026-07-07-p0-workstream-d-dependency-tuple-residual.md, .10x/tickets/done/2026-07-07-duckdb-arrow58-transitive-residual.md, .10x/decisions/superseded/duckdb-arrow58-private-driver-residual.md, .10x/decisions/superseded/datafusion-git-pin-arrow59-tuple.md, .10x/decisions/arrow-datafusion-tuple-policy.md

# DuckDB Arrow 58 residual audit

## What was observed

The only current Arrow `58.3.0` path is the private DuckDB driver dependency:

```text
$ cargo tree --workspace --locked -i arrow-array@58.3.0
arrow-array v58.3.0
├── arrow v58.3.0
│   └── duckdb v1.10504.0
│       └── cdf-dest-duckdb v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/cdf-dest-duckdb)
│           ├── cdf-cli v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/cdf-cli)
│           ├── cdf-conformance v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/cdf-conformance)
│           └── cdf-project v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/cdf-project)
├── arrow-arith v58.3.0
├── arrow-cast v58.3.0
├── arrow-ord v58.3.0
├── arrow-row v58.3.0
├── arrow-select v58.3.0
└── arrow-string v58.3.0
```

The DataFusion path remains on the ratified git pin and Arrow `59.1.0`:

```text
$ cargo metadata --locked --format-version 1 | jq -r '.packages[] | select(.name=="datafusion" or .name=="datafusion-common" or .name=="duckdb") | [.name,.version,(.source // "path"),.manifest_path] | @tsv'
datafusion 54.0.0 git+https://github.com/apache/datafusion.git?rev=7ff7278edc1bf7446303bff51e5883a38414bbdf#7ff7278edc1bf7446303bff51e5883a38414bbdf /Users/alexanderbut/.cargo/git/checkouts/datafusion-11a8b534adb6bd68/7ff7278/datafusion/core/Cargo.toml
datafusion-common 54.0.0 git+https://github.com/apache/datafusion.git?rev=7ff7278edc1bf7446303bff51e5883a38414bbdf#7ff7278edc1bf7446303bff51e5883a38414bbdf /Users/alexanderbut/.cargo/git/checkouts/datafusion-11a8b534adb6bd68/7ff7278/datafusion/common/Cargo.toml
duckdb 1.10504.0 registry+https://github.com/rust-lang/crates.io-index /Users/alexanderbut/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/duckdb-1.10504.0/Cargo.toml
```

`Cargo.lock` also records the DataFusion git source at the full rev and shows DataFusion depending on `arrow 59.1.0` / `arrow-schema 59.1.0`.

`cargo tree --workspace --locked -i datafusion@54.0.0` shows the DataFusion dependency enters through `cdf-engine`, then `cdf-cli` and `cdf-project` consumers. It does not enter through the DuckDB destination.

## DuckDB version and feature inspection

`cargo search duckdb --limit 10` and `cargo info duckdb` reported latest/current `duckdb 1.10504.0` on 2026-07-07. No newer `duckdb-rs` release was available to test.

`cargo info duckdb` reported default features as empty and the current CDF feature as `bundled`. The normalized registry `Cargo.toml` for `duckdb 1.10504.0` contains an unconditional dependency:

```toml
[dependencies.arrow]
version = "58"
features = ["prettyprint", "ffi"]
default-features = false
```

The original registry `Cargo.toml.orig` also has `arrow = { workspace = true, features = ["prettyprint", "ffi"] }` in ordinary dependencies, not an optional feature dependency.

The optional `appender-arrow`, `vtab-arrow`, and `vscalar-arrow` features do not remove the base Arrow dependency. `src/statement.rs` unconditionally imports Arrow `StructArray` and `SchemaRef` and exposes `query_arrow` / `stream_arrow`; `src/lib.rs` unconditionally re-exports `duckdb::arrow`. `src/appender/mod.rs` gates only the extra `appender::arrow` module behind `appender-arrow`.

Conclusion: there is no low-risk current `duckdb-rs` version or feature setting that avoids Arrow 58 while preserving CDF's existing DuckDB destination contract. Avoiding the Arrow 58 dependency today would require a fork, upstream change, or replacing the wrapper with a lower-level `libduckdb-sys` integration, which is not a bounded low-risk remediation for this ticket.

## CDF DuckDB Arrow boundary audit

Owned boundary files/functions:

- `crates/cdf-dest-duckdb/Cargo.toml`: `cdf-dest-duckdb` depends directly on CDF public Arrow `59.1.0` and on `duckdb 1.10504.0` with `bundled`.
- `crates/cdf-cli/Cargo.toml`: CLI dev tests also depend on `duckdb 1.10504.0` with `bundled`; this is test/query setup, not a public Arrow boundary.
- `crates/cdf-dest-duckdb/src/lib.rs`: imports CDF Arrow 59 symbols from `arrow_array` and `arrow_schema`, and imports DuckDB row/query primitives from `duckdb::{Connection, appender_params_from_iter, params, types::Value}`. It does not import `duckdb::arrow`.
- `crates/cdf-dest-duckdb/src/api.rs::plan_package_commit`: reads package data for planning through `load_package_data`; no DuckDB Arrow API is used.
- `crates/cdf-dest-duckdb/src/api.rs::commit_package` and `commit_package_immediate`: commit/replay path opens DuckDB, loads CDF package data, applies DDL, appends row values for append/replace/merge, builds a CDF `Receipt`, stores mirror rows, and appends the receipt to the package. No `duckdb::arrow` type appears at the boundary.
- `crates/cdf-dest-duckdb/src/package.rs::load_package_data`: opens `cdf_package::PackageReader`, verifies the package, calls `read_all_segments()`, receives CDF Arrow 59 `RecordBatch` values from `cdf-package`, validates a single schema, converts fields through `field_plan`, and converts each batch through `batch_rows`.
- `crates/cdf-dest-duckdb/src/package.rs::first_schema`, `field_plan`, and `duckdb_type`: inspect Arrow 59 schema/type metadata and lower it to DuckDB SQL type strings. Unsupported or lossy Arrow types fail before reaching DuckDB.
- `crates/cdf-dest-duckdb/src/rows.rs::batch_rows` and `cell_value`: convert Arrow 59 arrays into internal `RowValues` made of `duckdb::types::Value` plus `CellKey` identity values for merge deduplication. This is the only CDF Arrow-data-to-DuckDB-value conversion point.
- `crates/cdf-dest-duckdb/src/commit.rs::append_rows`, `append_rows_to_table`, and `merge_rows`: send internal `RowValues` to DuckDB through row appender parameters (`appender_params_from_iter(values)`) and SQL queries. They do not use DuckDB Arrow appenders or virtual tables.
- `crates/cdf-dest-duckdb/src/receipts.rs::build_receipt`, `segment_acks`, `validate_requested_segments`, and `record_package_receipt_once`: build CDF kernel receipts, validate segment row counts, and append receipt JSON to the package. No Arrow structs are exposed or returned.
- `crates/cdf-dest-duckdb/src/mirrors.rs::find_duplicate_receipt`, `insert_mirrors`, `read_mirror_snapshot`, `read_load_rows`, and `read_state_rows`: receipt verification and replay mirrors move strings, counts, JSON, and typed rows via DuckDB SQL. No Arrow structs are returned.
- `crates/cdf-dest-duckdb/src/api.rs::verify_receipt`: reads `receipt_json` from `_cdf_loads`, deserializes a CDF `Receipt`, and compares JSON-derived receipt data to the supplied receipt. No Arrow structs are used.
- `crates/cdf-package/src/reader.rs::read_all_segments` and `read_segment`, plus `crates/cdf-package/src/ops.rs::read_segment_file`: package replay returns Arrow 59 `RecordBatch` values from Arrow IPC through the `cdf-package` crate. These are CDF public Arrow 59 APIs before they enter the DuckDB destination lowering step.

Boundary conclusion: Arrow 58 structs from `duckdb-rs` do not cross into CDF public Arrow 59 APIs for commit, replay, duplicate detection, or receipt verification. The DuckDB driver currently receives only `duckdb::types::Value`, SQL strings, primitive query parameters, and JSON receipt strings from CDF-owned code.

## Supply-chain posture

`deny.toml` already has explicit source posture:

```toml
[sources]
unknown-registry = "deny"
unknown-git = "deny"
allow-registry = ["https://github.com/rust-lang/crates.io-index"]
allow-git = ["https://github.com/apache/datafusion.git"]
```

No `deny.toml` update is needed for this record-only work. The DataFusion git source is already explicitly allowed, and unknown git sources remain denied.

`supply-chain/config.toml` already contains `safe-to-deploy` exemptions for Arrow `58.3.0` crates and Arrow `59.1.0` crates. No cargo-vet posture update is needed because the dependency graph did not change.

Quality gates:

- `cargo deny check`: passed. Output includes duplicate-version warnings for Arrow 58/59 and other known duplicate paths, then reports `advisories ok, bans ok, licenses ok, sources ok`.
- `cargo vet --locked`: passed with `Vetting Succeeded (393 exempted)`.

## Adversarial assessment

Potential failure: CDF might accidentally pass Arrow 59 batches into a DuckDB Arrow 58 API later.

Current evidence against it: CDF uses `PackageReader` to decode Arrow 59 batches, lowers them to internal `RowValues`, and writes through row appender values. Source inspection found no use of `duckdb::arrow`, `query_arrow`, `stream_arrow`, `appender-arrow`, or `vtab-arrow` in `cdf-dest-duckdb`.

Potential failure: receipt verification might depend on DuckDB Arrow output and reintroduce a structural mismatch.

Current evidence against it: receipt verification reads `receipt_json` via scalar `query_row`, deserializes a CDF kernel `Receipt`, and compares it to the supplied receipt. Mirror snapshots read strings and counts, not Arrow batches.

Potential failure: accepting the residual could weaken the one-tuple policy.

Historical assessment: `.10x/decisions/arrow-datafusion-tuple-policy.md` governed the engine hot path and public Arrow/DataFusion tuple. This residual was private to the DuckDB destination wrapper and did not cross the CDF public Arrow API boundary. Its former acceptance and triggers are preserved in `.10x/decisions/superseded/duckdb-arrow58-private-driver-residual.md`.

## What this supports

This supports temporarily accepting the DuckDB Arrow 58 residual without changing Rust implementation files, dependency manifests, `deny.toml`, or cargo-vet config.

It supports keeping the DataFusion TableProvider adapter unblocked: the DataFusion engine path remains on the ratified git-source Arrow 59 tuple, and the DuckDB residual is a private destination-driver dependency path.

## Limits

No remediation implementation was attempted because no current low-risk version/feature path exists. No CodeQL run was performed, per the workstream instruction to skip CodeQL. At initial evidence-recording time the residual ticket was not moved to `done/` because references outside the worker write scope still needed repair; the parent orchestrator later repaired those references and moved `.10x/tickets/done/2026-07-07-duckdb-arrow58-transitive-residual.md` to terminal state.
