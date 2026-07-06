Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/done/2026-07-05-contract-compiler-normalization.md, .10x/tickets/done/2026-07-05-formats-and-subprocess.md, .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md

# Implement Parquet file source without violating supply-chain gates

## Scope

Implement MVP Parquet file-source reads into `firn-kernel::Batch` values with observed schema and deterministic schema hash population, without introducing a supply-chain scanner failure.

## Acceptance criteria

- Parquet file sources produce resource descriptors and batches for MVP file sources.
- Arrow field metadata and batch shape are preserved to the extent supported by the chosen reader.
- `cargo deny check advisories`, `cargo audit`, and OSV scanning pass or the project has an active ratified policy exception for the exact dependency/advisory.
- The implementation has parser tests, malformed input tests, and package write/replay compatibility tests.

## Evidence expectations

Record the dependency path selected, advisory/scanner results, parser tests, malformed input tests, and package integration tests.

## Explicit exclusions

No destination writer or object-store destination behavior. Those remain owned by destination tickets.

## Progress and notes

- 2026-07-06: Split from `.10x/tickets/done/2026-07-05-formats-and-subprocess.md`. A direct `parquet = "59.0.0"` implementation worked locally, but `cargo deny check advisories` and OSV reported `RUSTSEC-2024-0436` because arrow-rs `parquet` depends unconditionally on `paste 1.0.15`, which RustSec marks unmaintained. Feature trimming cannot remove `paste`. The direct dependency was removed before committing the formats/subprocess core.
- 2026-07-06: Supply-chain policy is now ratified by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`. The policy keeps advisory checks enforced with no ignores, so this ticket remains blocked for the direct arrow-rs `parquet -> paste` path unless a later specific advisory exception is ratified or an alternative Parquet reader path avoids the advisory.
- 2026-07-06: Parent inspection found an alternative reader path through the existing `duckdb = 1.10504.0` crate with the bundled `parquet` feature already present in the locked graph. `duckdb::Statement::query_arrow` can query `read_parquet(...)`, but it yields DuckDB's Arrow 58 `RecordBatch` type while Firn formats use Arrow 59. The executable implementation should bridge through Arrow IPC bytes or another deterministic conversion that preserves supported schema/batch shape without adding a direct arrow-rs `parquet` dependency or `paste`.
- 2026-07-06: Implemented `FileFormat::Parquet` in `firn-formats` using `duckdb = 1.10504.0` with `bundled` and `parquet`, plus a renamed `duckdb-arrow` Arrow 58 `ipc` bridge into existing Arrow 59 IPC reader code. Added Parquet source descriptor/batch/file-manifest coverage, malformed Parquet data-error coverage, and package write/replay compatibility coverage. Verification passed: `cargo fmt --all -- --check`; `cargo test -p firn-formats --locked --no-fail-fast` (6 tests); `cargo clippy -p firn-formats --all-targets --locked -- -D warnings`; `cargo deny check advisories`; `cargo audit`; `osv-scanner --lockfile Cargo.lock`; and forbidden dependency scan found no direct `parquet` or `paste` entries.
- 2026-07-06: Parent review added mutation-test coverage for the existing top-level JSON object reader branch after `cargo mutants` found a missed mutant in `json_document_to_ndjson`. Final focused mutation rerun passed with 35 mutants tested, 15 caught, 20 unviable, and 0 missed. Full quality evidence is recorded in `.10x/evidence/2026-07-06-parquet-file-source-quality.md`; closure review is recorded in `.10x/reviews/2026-07-06-parquet-file-source-review.md`.

## Blockers

None. Do not add a direct `parquet` crate dependency or advisory ignore.
