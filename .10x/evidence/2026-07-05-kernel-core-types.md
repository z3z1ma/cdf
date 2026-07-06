Status: recorded
Created: 2026-07-05
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-kernel-core-types.md

# Kernel core types implementation evidence

## What was observed

`firn-kernel` now defines the kernel resource traits, Arrow stream type, resource descriptors and capabilities, batch headers/runtime payloads, source positions, scope keys, receipts, state deltas, destination planning values, contract-facing values, Arrow field metadata helpers, and the shared firn error taxonomy.

The selected dependency versions came from crates.io metadata commands on 2026-07-05:

- `cargo search arrow-schema --limit 1` reported `arrow-schema = "59.0.0"`.
- `cargo search arrow-array --limit 1` reported `arrow-array = "59.0.0"`.
- `cargo search futures-core --limit 1` reported `futures-core = "0.3.32"`.
- `cargo search serde --limit 1` reported `serde = "1.0.228"`.
- `cargo search serde_json --limit 1` reported `serde_json = "1.0.150"` for dev-only round-trip tests.
- `cargo info arrow-schema` and `cargo info arrow-array` confirmed Arrow crate version `59.0.0`.

## Procedure

The following commands were run from `/Users/alexanderbut/code_projects/personal/firn` after implementation:

```text
cargo fmt -p firn-kernel
cargo test -p firn-kernel
cargo check --workspace
cargo tree -p firn-kernel --depth 1
rg -n "DataFusion|datafusion|DuckDB|duckdb|PyO3|pyo3|Python|python|Tokio|tokio|reqwest|rusqlite|clap|object_store|firn_engine|firn_cli|firn_project" crates/firn-kernel/src crates/firn-kernel/Cargo.toml
```

`cargo test -p firn-kernel` passed: 4 unit tests passed and doc tests passed with 0 tests. The unit tests cover Arrow metadata helpers, Arrow `RecordBatch` batch wrapping/counts, serde round trips for artifact values, receipt coverage of state deltas, and the required error taxonomy categories.

`cargo check --workspace` passed.

`cargo tree -p firn-kernel --depth 1` reported this direct dependency boundary:

```text
firn-kernel v0.1.0 (/Users/alexanderbut/code_projects/personal/firn/crates/firn-kernel)
├── arrow-array v59.0.0
├── arrow-schema v59.0.0
├── futures-core v0.3.32
└── serde v1.0.228
[dev-dependencies]
└── serde_json v1.0.150
```

The forbidden-term `rg` command exited with status 1 and no output, which indicates no matches in `crates/firn-kernel/src` or `crates/firn-kernel/Cargo.toml`.

## What this supports or challenges

This supports the ticket acceptance criteria that kernel public APIs expose only Arrow, standard/runtime-neutral Rust, serde artifact values, and futures-core stream signatures. It also supports the required presence of `ResourceStream`, `QueryableResource`, `ResourceDescriptor`, `ResourceCapabilities`, `Batch`, `SourcePosition`, `Receipt`, `StateDelta`, typed positions, scope keys, metadata helpers, and error taxonomy.

No evidence challenged the ticket acceptance criteria.

## Limits

The boundary scan is textual and dependency-tree based; it does not prove future downstream crates use the kernel correctly. `Batch` carries runtime `RecordBatch` payloads and a separately serializable `BatchHeader`; canonical package Arrow IPC serialization is intentionally left to package work. SQLite, DataFusion, package file I/O, destination drivers, CLI, Python, HTTP, and project parsing were not implemented.

## Repair evidence

Parent review found two kernel issues: `PartitioningCapabilities` had a manually implemented `Default` that clippy required deriving, and the public resource trait was named `Resource` while the specs define `ResourceStream` as the trait. The repair derived `Default` for `PartitioningCapabilities`, renamed the pinned batch stream alias to `BatchStream`, made the public resource trait `ResourceStream`, and updated `QueryableResource` to extend `ResourceStream`.

The following commands were run from `/Users/alexanderbut/code_projects/personal/firn` after the repair:

```text
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo check --workspace --all-targets --no-default-features --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy --workspace --all-targets --no-default-features --locked -- -D warnings
cargo test -p firn-kernel --locked --no-fail-fast
cargo check --workspace
```

All commands passed. `cargo test -p firn-kernel --locked --no-fail-fast` passed 4 unit tests and 0 doc tests. The clippy commands completed with `-D warnings` and no diagnostics.

## Quality mutation repair evidence

Parent QUALITY mutation testing found five missed mutants in `FirnError` display formatting, `SourcePosition::version()`, and negative `Receipt::covers_state_delta` coverage. The repair added focused kernel tests for retry and non-retry display strings, embedded source-position versions across all position variants with non-1 values, and receipt rejection for package mismatch, schema mismatch, and missing segment acknowledgement.

The following commands were run from `/Users/alexanderbut/code_projects/personal/firn` after the mutation-focused test repair:

```text
cargo fmt --all -- --check
cargo test -p firn-kernel --locked --no-fail-fast
cargo clippy -p firn-kernel --all-targets --locked -- -D warnings
cargo mutants -p firn-kernel --test-tool nextest --timeout 60 --minimum-test-timeout 5 -j 4 -o reports/ai-quality/mutants-kernel --cargo-arg=--locked
```

All commands passed. `cargo test -p firn-kernel --locked --no-fail-fast` passed 7 unit tests and 0 doc tests. `cargo clippy -p firn-kernel --all-targets --locked -- -D warnings` completed with no diagnostics. The mutation run completed with `38 mutants tested in 58s: 21 caught, 17 unviable`, leaving zero missed mutants.
