Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/specs/destination-receipts-guarantees.md, .10x/specs/package-lifecycle-determinism.md

# DuckDB destination evidence

## What was observed

`firn-dest-duckdb` now implements a real DuckDB destination over `duckdb = 1.10504.0` with the bundled native DuckDB path. The crate exposes `DuckDbDestination` implementing `firn_kernel::DestinationProtocol`, a DuckDB capability report, dry-run package commit planning, package commit execution, receipt verification, single-writer lock acquisition, and an ICU runtime probe.

The implementation commits canonical `firn-package` Arrow IPC segments by reading `PackageReader` replay batches and appending supported Arrow values into DuckDB through the real DuckDB appender API. It supports append, transactional replace, and merge. Merge deduplicates exact duplicate package rows before writing and rejects conflicting duplicate merge keys because no active record ratifies a winner policy.

The destination sheet declares append/replace/merge, atomic-package transactions, package-token idempotency, namecase-v1 identifier rules, migration support, single-writer concurrency, and explicit type mappings. The extended DuckDB capability report declares `arrow_ipc_package_rows` as supported and Parquet scan replay as unsupported in this slice because `firn package archive` Parquet data is owned by `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md`.

## Procedure

Commands run from `/Users/alexanderbut/code_projects/personal/firn`:

```text
cargo check -p firn-dest-duckdb
cargo fmt -p firn-dest-duckdb
cargo test -p firn-dest-duckdb --locked --no-fail-fast
cargo clippy -p firn-dest-duckdb --all-targets --locked -- -D warnings
git diff --check
cargo deny check advisories
cargo audit
```

Results:

- `cargo check -p firn-dest-duckdb`: passed after compiling `libduckdb-sys` with the bundled native path.
- `cargo fmt -p firn-dest-duckdb`: passed.
- `cargo test -p firn-dest-duckdb --locked --no-fail-fast`: passed 8 unit tests and 0 doctests.
- `cargo clippy -p firn-dest-duckdb --all-targets --locked -- -D warnings`: passed.
- `git diff --check`: passed.
- `cargo deny check advisories`: passed with `advisories ok`; the tool warned that no config path exists and used default config, matching existing supply-chain policy context.
- `cargo audit`: exited 0 after scanning 402 crate dependencies.

The tests cover destination sheet declarations, dry-run DDL planning without creating a missing database file, append idempotency, duplicate/no-op replay, receipt verification after reopening the database, `_firn_loads` and `_firn_state` mirrors, atomic replace behavior, merge update/insert behavior with exact duplicate package-row deduplication, conflicting duplicate merge-key rejection, single-writer lock behavior, and ICU probe reporting.

## What this supports or challenges

This supports the DuckDB ticket acceptance criteria for sheet declarations, append/replace/merge, package-token idempotency, crash-window receipt verification via destination mirror after process restart, mirror tables, DDL planning, and ICU support detection.

No evidence challenged local DuckDB driver viability. The first bundled build completed and subsequent locked checks used the committed lockfile.

## Limits

The direct `duckdb-rs` Arrow appender is not used because `duckdb 1.10504.0` exposes Arrow appender APIs over Arrow 58 while Firn package/kernel APIs use Arrow 59. The destination therefore uses the real DuckDB row appender over decoded Firn Arrow IPC package batches to avoid crossing incompatible public Arrow types.

Parquet package replay is declared unsupported in this crate until `firn package archive` produces Parquet package data with a fidelity report. That package/archive work is already owned by `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md`.

Timezone-aware timestamp commits remain unsupported; the crate exposes `probe_icu` so the project/CLI doctor layer can report ICU availability. Naive second/millisecond/microsecond timestamps are supported; nanosecond timestamp/time values are rejected when they would lose precision.
