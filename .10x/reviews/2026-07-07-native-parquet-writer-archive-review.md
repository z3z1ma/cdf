Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-06-native-parquet-writer-archive.md
Verdict: pass

# Native Parquet writer and archive review

## Target

Review of the native Parquet writer/archive replacement implemented for `.10x/tickets/done/2026-07-06-native-parquet-writer-archive.md`.

## Findings

No blocking findings.

Checked risks:

- Writer path regression: `cdf-package` now writes with `parquet::arrow::ArrowWriter`, and `cdf-dest-parquet` delegates to that shared path. The removed `duckdb_writer` module is no longer referenced.
- Package identity drift: package tests still assert archive transcode preserves canonical Arrow IPC package identity and keeps Parquet sidecars/metadata outside identity.
- Receipt/idempotency drift: Parquet destination tests still cover append materialization, in-memory duplicate replay, replace pointer identity, tampered/missing objects, and receipt verification.
- Unsupported type and duplicate-name behavior: tests cover unsupported Arrow types and duplicate column names before object writes/native writer use.
- Supply-chain regression: `cargo deny`, `cargo audit`, OSV, and cargo-vet distinguish the already-ratified `RUSTSEC-2024-0436` `paste` path from other findings; no unratified advisory appeared.
- Unsafe regression: source scans found no unsafe, FFI, raw pointer, or transmute patterns in the changed crates.

## Verdict

Pass. The change satisfies the ticket scope and preserves the active package/destination contracts.

## Residual risk

`cargo geiger` did not complete successfully and is recorded as a tool limitation, not pass evidence. CodeQL was intentionally skipped under the active goal/user instruction not to recreate the CodeQL database. The remaining DuckDB dependency in the workspace belongs to the explicit DuckDB destination and unrelated existing edges, not this writer/archive path.
