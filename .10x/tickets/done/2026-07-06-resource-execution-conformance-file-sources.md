Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md, .10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md

# Add resource execution conformance for file sources

## Scope

Implement the first execution-level resource conformance harness for openable `ResourceStream` implementations, then consume it with real file-backed resources for CSV, JSON, NDJSON, and Parquet.

Owns `crates/firn-conformance/**` for the reusable harness and `crates/firn-formats/**` for the smallest production file-resource wrapper over the existing `read_file_source` implementation. The wrapper must reuse the current file readers and DuckDB-backed Parquet path; this ticket does not change the native Arrow/DataFusion Parquet policy.

Keep crate roots thin and use focused modules rather than expanding `lib.rs`.

## Acceptance criteria

- `firn-conformance` exposes a reusable async execution conformance helper for `ResourceStream` candidates with representative `ScanRequest` cases.
- The helper plans partitions through the public trait, opens each partition, drains returned batches, and verifies every batch has:
  - the candidate resource id;
  - the opened partition id;
  - a unique non-empty batch id;
  - `row_count` matching the in-memory `RecordBatch`;
  - `byte_count` matching the in-memory `RecordBatch` memory size;
  - `observed_schema_hash` matching the expected resource schema hash for the case;
  - a `RecordBatch` payload at MVP.
- The helper verifies partition-union completeness through caller-provided expected partition ids, total row count, and optional per-partition row counts. The helper must fail when a resource drops, duplicates, or mislabels a partition's rows.
- The helper verifies source-position honesty for resources that declare position replay or emit file-scoped data: file resources must attach a `FileManifest` source position to every emitted batch, and that manifest must include non-empty path, size, and SHA-256 evidence.
- The helper includes negative self-tests with deliberately faulty resources for wrong resource id, wrong partition id, duplicate batch id, bad row count, bad byte count, bad schema hash, missing expected partition, duplicate partition data, missing file position, and non-`RecordBatch` payload.
- `firn-formats` exposes a file-backed `ResourceStream` wrapper over `FileSource` without changing existing reader semantics, package semantics, or the DuckDB-backed Parquet supply-chain workaround.
- CSV, JSON, NDJSON, and Parquet file-source fixtures consume both the existing planning-level resource harness and the new execution harness.
- The implementation keeps `Cargo.lock` locked except for dependency edges directly required by this ticket.

## Evidence expectations

Record focused checks:

- `cargo test -p firn-conformance --locked resource -- --nocapture`
- `cargo test -p firn-formats --locked --no-fail-fast`
- `cargo clippy -p firn-conformance -p firn-formats --all-targets --locked -- -D warnings`
- `cargo nextest run -p firn-conformance -p firn-formats --locked`
- `cargo fmt --all -- --check`
- `git diff --check -- . ':(exclude).gitignore'`

Before closure, run the relevant `QUALITY.md` security/supply-chain gates for the touched crates, including `cargo deny check`, `cargo audit`, `cargo vet --locked`, OSV, Semgrep, source-only gitleaks, direct unsafe/FFI scan, machete, udeps, and CodeQL through `tools/codeql-rust-quality.sh` with the reusable database path. Use bounded mutation testing over the new conformance execution helper and the file-resource wrapper; if mutation tooling cannot cover both within a reasonable bound, record the exact limit and harden with negative self-tests.

## Explicit exclusions

No native arrow-rs `parquet` dependency, no `paste` advisory exception, no Cargo advisory-policy change, no DataFusion file scan provider, no live HTTP/API execution, no SQL database snapshot resource execution, no position-replay suffix API beyond the current `PartitionPlan::start_position` and emitted batch positions, no chaos process-kill layer, no CLI changes, no package lifecycle changes, and no MVP killer-demo orchestration.

Boundedness honesty remains parent scope until a public boundedness signal exists.

## References

- `firn-the-book-of-the-system.md` Chapters 7, 8, 9, 19, and 22.
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md`
- `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md`

## Progress and notes

- 2026-07-06: Split from the conformance parent after inspection showed the existing resource conformance harness is planning-only and `firn-formats` has real file readers but no openable `ResourceStream` wrapper. The next complete step is an execution-level conformance oracle plus a real file-source consumer for CSV, JSON, NDJSON, and Parquet.
- 2026-07-06: Implemented `firn-conformance::resource::assert_resource_stream_execution_conformance` with negative self-tests and added `firn-formats::FileResource` over the existing file readers. CSV, JSON, NDJSON, and Parquet tests now consume both planning and execution conformance harnesses.
- 2026-07-06: Parent review found and fixed a semver break where public fields had been added to `FormatRead`; the final implementation derives `FileResource` schema/position from existing batch data and leaves `FormatRead`'s public field set unchanged.
- 2026-07-06: Evidence is recorded in `.10x/evidence/2026-07-06-resource-execution-conformance-file-sources.md`; closure review is recorded in `.10x/reviews/2026-07-06-resource-execution-conformance-file-sources-review.md`.
- 2026-07-06: Closed after focused tests, nextest, clippy, formatting, diff check, docs, cargo metadata, cargo deny/audit/vet, OSV, Semgrep, CodeQL with reusable DB refresh, gitleaks, direct unsafe scan, isolated Geiger, machete, udeps, semver-checks, rust-code-analysis, jscpd, llvm-cov, and bounded mutation testing all produced acceptable evidence.

## Blockers

None.
