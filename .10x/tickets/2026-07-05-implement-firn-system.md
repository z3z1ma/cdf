Status: active
Created: 2026-07-05
Updated: 2026-07-06

# Implement the firn system

## Scope

Implement the entire firn system described by `firn-the-book-of-the-system.md` and preserved in active `.10x/` records. This parent ticket is a plan and orchestration record, not an executable implementation unit.

The parent agent owns sequencing, assignment to subagents, integration, evidence, review, and closure. Child tickets own implementation.

## Governing records

- `.10x/decisions/firn-system-authority.md`
- `.10x/decisions/firn-book-decision-register.md`
- `.10x/knowledge/firn-glossary.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/checkpoint-state-firn-line.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/project-cli-observability-security.md`
- `.10x/specs/conformance-governance-roadmap.md`

## Child sequence

MVP foundation:

- `.10x/tickets/done/2026-07-05-bootstrap-rust-workspace.md`
- `.10x/tickets/done/2026-07-05-kernel-core-types.md`
- `.10x/tickets/done/2026-07-05-checkpoint-store-sqlite.md`
- `.10x/tickets/done/2026-07-05-package-builder-reader.md`
- `.10x/tickets/done/2026-07-05-contract-compiler-normalization.md`
- `.10x/tickets/done/2026-07-05-datafusion-engine-planner.md`

MVP authoring, destinations, and product surface:

- `.10x/tickets/done/2026-07-05-http-toolkit.md`
- `.10x/tickets/done/2026-07-05-declarative-resources.md`
- `.10x/tickets/done/2026-07-05-formats-and-subprocess.md`
- `.10x/tickets/2026-07-06-parquet-format-source-supply-chain.md`
- `.10x/tickets/done/2026-07-05-python-sdk-bridge.md`
- `.10x/tickets/done/2026-07-05-duckdb-destination.md`
- `.10x/tickets/done/2026-07-05-parquet-object-store-destination.md`
- `.10x/tickets/2026-07-05-postgres-destination.md`
- `.10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md`
- `.10x/tickets/2026-07-05-cli-surface.md`
- `.10x/tickets/2026-07-05-observability-doctor-status-sql.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-05-dlt-shim-preview.md`

Fast-follow and full-system completion:

- `.10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md`
- `.10x/tickets/2026-07-05-wasm-components-registry-signing.md`
- `.10x/tickets/2026-07-05-cdc-and-streaming-supervisor.md`
- `.10x/tickets/2026-07-05-distributed-execution-and-remote-state.md`
- `.10x/tickets/2026-07-05-lakehouse-warehouse-and-vault.md`
- `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`
- `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`
- `.10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md`
- `.10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md`

## Acceptance criteria

- All child tickets are done or explicitly superseded/cancelled with durable rationale.
- The MVP killer demo from `.10x/specs/conformance-governance-roadmap.md` passes and has recorded evidence.
- Fast-follow and beyond-MVP surfaces described by the book are implemented or governed by active superseding records.
- The book can be removed without losing behavioral authority because active records, source, tests, and docs contain the needed contracts.

## Evidence expectations

Each child ticket must record command/test evidence before closure. Parent closure requires an adversarial review of the full graph, evidence mapping to acceptance criteria, and retrospective learning records where useful.

## Dependencies

No external product decision blockers remain for book-clear behavior. Technical dependencies are encoded in child ticket `Depends-On` headers.

## Explicit exclusions

A UI is excluded unless a later active decision supersedes the book. SCD2 and snapshot loader dispositions remain excluded. Airbyte destinations remain excluded.

## Progress and notes

- 2026-07-05: Created parent plan from the ratified book and active specs. Implementation has not begun; this record opens the Inner Loop path for child tickets.
- 2026-07-05: Bootstrap workspace child ticket closed with evidence and review; root Cargo workspace and crate skeletons now exist.
- 2026-07-06: Kernel QUALITY verification opened a separate supply-chain policy ticket because `cargo deny` license policy and cargo-vet adoption are not yet ratified.
- 2026-07-06: Checkpoint store child ticket closed with kernel `CheckpointStore` contract, in-memory store, SQLite WAL store, rewind/history behavior, mutation-clean tests, quality evidence, and review.
- 2026-07-06: Contract compiler, package builder/reader, and HTTP toolkit child tickets closed with parent review and shared QUALITY evidence. Reusable CodeQL database now lives at `target/quality/codeql-db-rust`.
- 2026-07-06: DataFusion engine, declarative resources, and formats/subprocess core child tickets closed with evidence and reviews. Parquet file-source support was split to `.10x/tickets/2026-07-06-parquet-format-source-supply-chain.md` after scanners showed the direct arrow-rs `parquet` crate would introduce `RUSTSEC-2024-0436` through `paste`.
- 2026-07-06: Project format/secrets, Python SDK/bridge, and DuckDB destination child tickets closed with evidence and reviews. Postgres destination has a deterministic planning/SQL/receipt surface but remains blocked until live Postgres execution evidence or a superseding planning-only decision exists.
- 2026-07-06: User ratified a crate-organization convention to avoid monolithic `lib.rs` files where possible. Recorded in `.10x/knowledge/rust-crate-organization.md`; split the new project, Python, DuckDB, and Postgres crate roots with verification in `.10x/evidence/2026-07-06-rust-crate-organization-refactor.md`.
- 2026-07-06: Opened `.10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md` for older large crate roots from earlier child tickets so the convention is not lost; it later closed after child split tickets completed.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`; the new project, Python, DuckDB, and Postgres crate roots now use ordinary Rust modules rather than `include!` maps. Consolidated quality evidence for the batch is `.10x/evidence/2026-07-06-project-python-destinations-quality-gates.md`. Local CodeQL extractor-quality work was later closed by `.10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md`.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md`; Firn now has a stale-aware local CodeQL wrapper that preserves `target/quality/codeql-db-rust`, avoids mtime-only rebuild churn with a content fingerprint, excludes generated artifacts during database creation, and records the current Rust extractor macro-expansion limit.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-05-dlt-shim-preview.md` with scoped preview shim evidence and review. Closed `.10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md` after splitting the remaining large Rust crate roots into ordinary modules. CLI has a practical command surface but remains blocked on lower-layer runtime APIs. Consolidated quality evidence for this commit batch is `.10x/evidence/2026-07-06-cli-dlt-crate-splits-quality-gates.md`.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-singer-airbyte-protocol-adapters.md`; `firn-subprocess` now has focused Singer/Airbyte protocol parser modules, canonical opaque `ForeignState` hashing, stream-scoped batch conversion, package replay compatibility tests, mutation-clean adapter tests, and full QUALITY evidence. The larger Singer/Airbyte/package-archive parent remains open for `firn package archive`.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-05-parquet-object-store-destination.md`; `firn-dest-parquet` now implements append/replace package-token Parquet materialization over filesystem/object_store, object manifest and replace-pointer receipts, tamper-aware receipt verification, DuckDB-backed Parquet export without the arrow-rs `parquet`/`paste` advisory path, non-monolithic crate modules, mutation-clean focused tests, and full QUALITY evidence.
- 2026-07-06: Closed observability child `.10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md`; `firn doctor` now has a real configured-Python interpreter probe with version, GIL/free-threaded, no-resource-code-execution, and secret-redaction coverage.
- 2026-07-06: Closed observability child `.10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md`; `firn doctor` now has structured project/environment health, redacted secret-reference details with env/file/declarative coverage, missing-secret failure redaction, and DuckDB ICU safe details.
- 2026-07-06: Closed supply-chain policy child `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`; `deny.toml` now makes advisory/license/source policy explicit, and `supply-chain/` now initializes cargo-vet so `cargo vet --locked` passes with a current-version exemption backlog.
- 2026-07-06: Closed package archive primitive child `.10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md`; `firn-package` now has a supply-chain-clean IPC-to-Parquet in-memory archive report primitive, `firn-dest-parquet` delegates to the shared writer, and the remaining archive CLI/file-placement/manifest metadata work stays with `.10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md`.

## Blockers

None for the parent plan. Individual child tickets may be dependency-gated.
