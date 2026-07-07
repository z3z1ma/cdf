Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-formats-and-subprocess.md, .10x/specs/resource-authoring-planning-batches.md, .10x/specs/types-contracts-normalization.md, .10x/specs/package-lifecycle-determinism.md, .10x/specs/project-cli-observability-security.md

# Formats and subprocess evidence

## What was observed

`cdf-formats` now reads Arrow IPC streams, NDJSON, CSV, and JSON object/array files into existing `cdf-kernel::Batch` values with `ResourceDescriptor`, observed schema, and deterministic local schema hash population. NDJSON inference is routed through `cdf-contract::ObservedSchema` and `compile_validation_program`, so row-shaped JSON feeds the same contract-observed schema path as other Arrow-backed inputs.

Parquet file-source support was attempted with `parquet = "59.0.0"` and then removed before closure because `cargo deny check advisories` and OSV reported `RUSTSEC-2024-0436` through its unconditional `paste` dependency. The unresolved Parquet reader requirement is owned by `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md`; `FileFormat::Parquet` currently returns a contract error that names the supply-chain blocker.

`cdf-subprocess` now supervises OS subprocess adapters for Arrow IPC and NDJSON stdout. It captures bounded stderr trace lines, maps nonzero exits and timeouts to the shared `Transient` taxonomy, and preserves parser/malformed-output failures as `Data` errors with stderr context.

No shared type additions were required. The implementation uses existing `cdf-kernel` types.

## Procedure

- Inspected the active ticket, governing specs, glossary, quality-gate knowledge, and done kernel/contract/package tickets before editing.
- Added focused dependencies to `crates/cdf-formats/Cargo.toml` and `crates/cdf-subprocess/Cargo.toml`; `Cargo.lock` was updated by Cargo resolution in the shared workspace.
- Implemented the adapter surface in `crates/cdf-formats/src/lib.rs` and `crates/cdf-subprocess/src/lib.rs`.
- Added tests covering Arrow IPC schema-preserving round trip, NDJSON-to-contract observed schema, CSV/JSON file sources, Parquet supply-chain blocker reporting, malformed inputs, subprocess stderr/exit/timeout/malformed-output handling, and package write/replay compatibility.

## Command results

- `cargo fmt --package cdf-formats --package cdf-subprocess`: passed.
- `cargo test -p cdf-formats --locked --no-fail-fast`: initially passed with 5 unit tests before the Parquet split.
- `cargo test -p cdf-formats -p cdf-subprocess --locked --no-fail-fast`: passed after the Parquet split, with 6 `cdf-formats` unit tests, 5 `cdf-subprocess` unit tests, and 0 doc tests.
- `cargo test -p cdf-subprocess --locked --no-fail-fast`: passed, 5 unit tests and 0 doc tests.
- `cargo clippy -p cdf-formats -p cdf-subprocess --locked -- -D warnings`: passed.
- `cargo clippy -p cdf-formats -p cdf-subprocess --all-targets --locked -- -D warnings`: passed after the Parquet split.
- `cargo deny check advisories`: initially failed on `RUSTSEC-2024-0436` via `parquet -> paste`; passed after the direct `parquet` dependency was removed.
- `git diff --check`: passed.

Dedicated CodeQL was not run during the targeted formats/subprocess subagent pass. Workspace CodeQL coverage for the final engine/declarative/formats batch is recorded in `.10x/evidence/2026-07-06-engine-declarative-formats-quality-gates.md`.

## What this supports or challenges

This supports the ticket acceptance criteria:

- Arrow IPC streams convert into kernel batches without schema loss, including field metadata.
- NDJSON inference feeds the contract-observed schema path.
- CSV and JSON file sources produce resource descriptors and batches.
- Subprocess exit, timeout, and malformed stdout map into the shared `CdfError` taxonomy.
- Adapter batches can be written into and replayed from `cdf-package` segments like native output.

## Limits

Parquet file sources remain blocked by supply-chain policy because the current arrow-rs `parquet` crate pulls the unmaintained `paste` crate. The split keeps the advisory scanners clean for this batch while preserving a durable owner for the MVP Parquet requirement.
