Status: active
Created: 2026-07-05
Updated: 2026-07-08

# Implement the cdf system

## Scope

Implement the entire cdf system described by `VISION.md` and preserved in active `.10x/` records. This parent ticket is a plan and orchestration record, not an executable implementation unit.

The parent agent owns sequencing, assignment to subagents, integration, evidence, review, and closure. Child tickets own implementation.

## Governing records

- `.10x/decisions/cdf-system-authority.md`
- `.10x/decisions/cdf-book-decision-register.md`
- `.10x/knowledge/cdf-product-objective.md`
- `.10x/knowledge/cdf-glossary.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
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
- `.10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md`

MVP authoring, destinations, and product surface:

- `.10x/tickets/done/2026-07-05-http-toolkit.md`
- `.10x/tickets/done/2026-07-05-declarative-resources.md`
- `.10x/tickets/done/2026-07-05-formats-and-subprocess.md`
- `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md`
- `.10x/tickets/done/2026-07-05-python-sdk-bridge.md`
- `.10x/tickets/done/2026-07-05-duckdb-destination.md`
- `.10x/tickets/done/2026-07-05-parquet-object-store-destination.md`
- `.10x/tickets/done/2026-07-05-postgres-destination.md`
- `.10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md`
- `.10x/tickets/done/2026-07-05-cli-surface.md`
- `.10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md`
- `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`
- `.10x/tickets/2026-07-05-observability-doctor-status-sql.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-05-dlt-shim-preview.md`

Fast-follow and full-system completion:

- `.10x/tickets/done/2026-07-07-p0-structural-debt-program.md`
- `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md`
- `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md`
- `.10x/tickets/done/2026-07-06-package-state-commit-artifact-contract.md`
- `.10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md`
- `.10x/tickets/done/2026-07-06-native-parquet-file-source.md`
- `.10x/tickets/done/2026-07-06-native-parquet-writer-archive.md`
- `.10x/tickets/done/2026-07-07-run-ledger-commit-session-spine-ratification.md`
- `.10x/tickets/done/2026-07-07-run-spine-implementation-program.md`
- `.10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md`
- `.10x/tickets/done/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`
- `.10x/tickets/done/2026-07-07-datafusion-tableprovider-adapter.md`
- `.10x/tickets/done/2026-07-07-datafusion-execution-honesty.md`
- `.10x/tickets/2026-07-05-wasm-components-registry-signing.md`
- `.10x/tickets/2026-07-05-cdc-and-streaming-supervisor.md`
- `.10x/tickets/2026-07-05-distributed-execution-and-remote-state.md`
- `.10x/tickets/2026-07-05-lakehouse-warehouse-and-vault.md`
- `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`
- `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`
- `.10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md`
- `.10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md`
- `.10x/tickets/done/2026-07-07-mechanical-cdf-identity-rename.md`
- `.10x/tickets/done/2026-07-07-semantic-commit-gate-terminology-cleanup.md`

## Acceptance criteria

- All child tickets are done or explicitly superseded/cancelled with durable rationale.
- The MVP acceptance demo from `.10x/specs/conformance-governance-roadmap.md` passes and has recorded evidence.
- Fast-follow and beyond-MVP surfaces described by the book are implemented or governed by active superseding records; the Chapter 22 MVP is treated as a milestone, not the project finish line.
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
- 2026-07-06: DataFusion engine, declarative resources, and formats/subprocess core child tickets closed with evidence and reviews. Parquet file-source support was split to `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md` after scanners showed the direct arrow-rs `parquet` crate would introduce `RUSTSEC-2024-0436` through `paste`.
- 2026-07-06: Project format/secrets, Python SDK/bridge, and DuckDB destination child tickets closed with evidence and reviews. Postgres destination has a deterministic planning/SQL/receipt surface but remains blocked until live Postgres execution evidence or a superseding planning-only decision exists.
- 2026-07-06: User ratified a crate-organization convention to avoid monolithic `lib.rs` files where possible. Recorded in `.10x/knowledge/rust-crate-organization.md`; split the new project, Python, DuckDB, and Postgres crate roots with verification in `.10x/evidence/2026-07-06-rust-crate-organization-refactor.md`.
- 2026-07-06: Opened `.10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md` for older large crate roots from earlier child tickets so the convention is not lost; it later closed after child split tickets completed.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-replace-include-crate-splits-with-modules.md`; the new project, Python, DuckDB, and Postgres crate roots now use ordinary Rust modules rather than `include!` maps. Consolidated quality evidence for the batch is `.10x/evidence/2026-07-06-project-python-destinations-quality-gates.md`. Local CodeQL extractor-quality work was later closed by `.10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md`.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-improve-codeql-rust-extractor-coverage.md`; CDF now has a stale-aware local CodeQL wrapper that preserves `target/quality/codeql-db-rust`, avoids mtime-only rebuild churn with a content fingerprint, excludes generated artifacts during database creation, and records the current Rust extractor macro-expansion limit.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-05-dlt-shim-preview.md` with scoped preview shim evidence and review. Closed `.10x/tickets/done/2026-07-06-split-existing-rust-crate-roots.md` after splitting the remaining large Rust crate roots into ordinary modules. CLI has a practical command surface but remains blocked on lower-layer runtime APIs. Consolidated quality evidence for this commit batch is `.10x/evidence/2026-07-06-cli-dlt-crate-splits-quality-gates.md`.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-singer-airbyte-protocol-adapters.md`; `cdf-subprocess` now has focused Singer/Airbyte protocol parser modules, canonical opaque `ForeignState` hashing, stream-scoped batch conversion, package replay compatibility tests, mutation-clean adapter tests, and full QUALITY evidence. The larger Singer/Airbyte/package-archive parent remains open for `cdf package archive`.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-05-parquet-object-store-destination.md`; `cdf-dest-parquet` now implements append/replace package-token Parquet materialization over filesystem/object_store, object manifest and replace-pointer receipts, tamper-aware receipt verification, DuckDB-backed Parquet export without the arrow-rs `parquet`/`paste` advisory path, non-monolithic crate modules, mutation-clean focused tests, and full QUALITY evidence.
- 2026-07-06: Closed observability child `.10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md`; `cdf doctor` now has a real configured-Python interpreter probe with version, GIL/free-threaded, no-resource-code-execution, and secret-redaction coverage.
- 2026-07-06: Closed observability child `.10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md`; `cdf doctor` now has structured project/environment health, redacted secret-reference details with env/file/declarative coverage, missing-secret failure redaction, and DuckDB ICU safe details.
- 2026-07-06: Closed supply-chain policy child `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`; `deny.toml` now makes advisory/license/source policy explicit, and `supply-chain/` now initializes cargo-vet so `cargo vet --locked` passes with a current-version exemption backlog.
- 2026-07-06: Closed package archive primitive child `.10x/tickets/done/2026-07-06-package-archive-transcode-primitive.md`; `cdf-package` now has a supply-chain-clean IPC-to-Parquet in-memory archive report primitive, `cdf-dest-parquet` delegates to the shared writer, and the remaining archive CLI/file-placement/manifest metadata work stays with `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md`.
- 2026-07-06: Closed Parquet file-source child `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md`; `cdf-formats` now reads Parquet sources through DuckDB's bundled reader and an Arrow IPC bridge without adding the blocked direct arrow-rs `parquet`/`paste` path, with parser, malformed-input, package replay, mutation, and QUALITY evidence.
- 2026-07-06: Split child now closed at `.10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md` for the missing lower-layer prepared-package DuckDB/SQLite replay/checkpoint runtime primitive. This is the smallest shared unblocking step for CLI `resume`/`replay package`, chaos recovery, golden replay gates, and the MVP crash-window demo without solving live source extraction in the same slice.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-package-replay-commit-gate-runtime.md`; `cdf-project` now exposes a receipt-verified prepared-package DuckDB/SQLite replay and recovery runtime primitive with focused tests, mutation coverage, full relevant `QUALITY.md` evidence, and closure review. CLI command wiring, broader run/resume orchestration, chaos killpoints, golden fixtures, and full MVP demo remain with their existing active tickets.
- 2026-07-06: Opened `.10x/tickets/done/2026-07-06-prepared-package-chaos-conformance.md` as the next conformance child. It will consume the prepared-package runtime to prove the first deterministic chaos/replay identity scenarios before broader process-kill chaos, golden fixtures, or MVP demo work.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-golden-package-conformance-foundation.md`; `cdf-conformance` now owns a reusable golden-package evidence harness with committed prepared-orders expected evidence, 100 local regeneration proof, verification-before-comparison, negative self-tests, mutation evidence, and full QUALITY evidence. Broader live-run golden gates and MVP demo work remain with `.10x/tickets/2026-07-05-conformance-chaos-golden.md`.
- 2026-07-06: Closed observability child `.10x/tickets/done/2026-07-06-engine-execution-tracing-spans.md`; `cdf-engine` now has an additive explicit-run-id tracing entry point with exact run/resource/package/partition span fields, preserved package identity, and mutation-clean execution tests. `inspect run` remains blocked under `.10x/tickets/2026-07-05-observability-doctor-status-sql.md` until run-ledger semantics are ratified.
- 2026-07-06: Closed package archive contract ratification at `.10x/tickets/done/2026-07-06-package-archive-contract-ratification.md`; the active package spec now defines persisted archive layout, non-identity metadata, status-preserving lifecycle behavior, rerun/crash policy, and CLI contract. Opened `.10x/tickets/done/2026-07-06-package-archive-persistence-cli.md` as the executable source slice.
- 2026-07-06: Rechecked the Postgres destination blocker and found local Postgres binaries are available while the crate remains planning-only. Opened `.10x/tickets/done/2026-07-06-postgres-live-execution.md` to implement the live driver-backed commit path and integration evidence needed by Postgres destination and downstream conformance work.
- 2026-07-06: Closed Postgres destination and its live execution child. `cdf-dest-postgres` now has driver-backed package commits, ephemeral local Postgres integration coverage for append/replace/merge/duplicate/receipt verification/mirrors/rollback/decimals, schema-scoped mirrors, and full relevant QUALITY evidence in `.10x/evidence/2026-07-06-postgres-live-execution.md`.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-package-archive-persistence-cli.md` and parent `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md`; `cdf package archive` now persists Parquet sidecars, canonical fidelity metadata, manifest archive metadata, verification coverage, and CLI output while preserving IPC package identity. Opened the now-closed `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md` as the separate cross-cutting decision for whether to replace the DuckDB-backed Parquet workaround with native Arrow/DataFusion Parquet and a time-boxed advisory exception.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-postgres-destination-conformance-consumer.md`; the destination conformance suite now covers the Postgres planning/sheet contract alongside DuckDB and Parquet, with live Postgres tests still providing runtime evidence.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-resource-execution-conformance-file-sources.md`; resource conformance now covers file-source execution/data-completeness for CSV, JSON, NDJSON, and Parquet through a reusable async `ResourceStream` oracle and `cdf-formats::FileResource`, without adding the native `parquet`/`paste` advisory path.
- 2026-07-06: Opened the CLI/product slice now closed at `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`. It connects the closed file-source runtime work to the book-required `cdf preview` behavior for single-match declarative local file resources while leaving native Parquet policy and broader run orchestration separate.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`; `cdf preview` now executes single-match declarative local file resources for CSV, JSON, NDJSON, and Parquet with no package, destination, or checkpoint writes, while native Parquet policy and broader run orchestration remain separate.
- 2026-07-06: Opened the CLI/runtime bridge now closed at `.10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md`. It targets the first live `cdf run` path from a single explicit local file resource through package creation, DuckDB receipt verification, and SQLite checkpoint commit without ratifying automatic run-ledger defaults.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-local-file-run-duckdb-checkpoint.md`; `cdf run --resource --pipeline --target --package-id --checkpoint-id` now supports the explicit single declarative local file resource to local DuckDB/SQLite slice, with receipt-gated checkpoint commit, recoverable post-receipt failure evidence, mutation-clean runtime tests, and full relevant QUALITY evidence. Broader run-ledger/default-id, REST/SQL, non-DuckDB, multi-resource, `resume`, and package replay CLI work remains with existing parent tickets.
- 2026-07-06: User clarified that CDF's endpoint is not merely the Chapter 22 MVP but the full production-grade, next-generation, enterprise-scale system optimized for AI-agent management. Recorded in `.10x/knowledge/cdf-product-objective.md`; parent acceptance now treats MVP as a milestone rather than the completion definition.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-live-local-file-run-golden-conformance.md`; the conformance suite now has a committed golden proof for the live local-file-to-DuckDB/SQLite run path, including 100-run deterministic evidence, verified receipts/checkpoints, recovery without source reread after durable receipt, duplicate/no-op replay, mutation-clean live-run harness checks, and relevant QUALITY evidence.
- 2026-07-06: Parent inspection found package evidence drift that blocks safe `cdf replay package`: current live packages do not contain the state/commit evidence required by the book/spec, while exact `StateDelta` and concrete commit-request artifacts would be package-hash circular. Recorded research in `.10x/research/2026-07-06-package-state-commit-artifact-circularity.md`, ratified `.10x/decisions/package-state-commit-preimage-artifacts.md`, updated package/destination specs, and opened executable ticket `.10x/tickets/done/2026-07-06-package-state-commit-artifact-contract.md`.
- 2026-07-06: Opened `.10x/tickets/done/2026-07-06-local-duckdb-lifecycle-chaos-failpoints.md` as the next executable conformance slice. It targets named local DuckDB/SQLite runtime failpoints for the package/checkpoint crash matrix while leaving the separate package artifact contract implementation to its own ticket.
- 2026-07-06: User explicitly ratified `.10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md`. Active decision `.10x/decisions/native-arrow-datafusion-parquet-policy.md` now supersedes the DuckDB-backed Parquet workaround as the target architecture and opens bounded follow-ups for the scoped `RUSTSEC-2024-0436` exception, native Parquet file-source reads, and native Parquet writer/archive replacement.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-06-package-state-commit-artifact-contract.md`; live and prepared packages now include identity-participating state/commit preimage artifacts, package readers reconstruct verified replay inputs, DuckDB artifact replay/recovery no longer needs source contact, conformance/golden fixtures consume the artifacts, and mutation-hardened package preimage validation is recorded in `.10x/evidence/2026-07-07-package-state-commit-artifact-contract.md`.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-mechanical-cdf-identity-rename.md`; repository identity now follows `VISION.md` D-24 mechanically across root book authority, Cargo crates/packages, Rust crate imports, CLI binary target, Python SDK path/imports, package golden identity, tooling fingerprint names, and `.10x/` records. Semantic terminology cleanup for the former line metaphor is split to `.10x/tickets/done/2026-07-07-semantic-commit-gate-terminology-cleanup.md`.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-semantic-commit-gate-terminology-cleanup.md`; `VISION.md`'s commit-gate vocabulary now governs checkpoint/state advancement specs, records, paths, CLI output, and tests. `guarantee line` remains reserved for future `cdf plan` output.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md`; `deny.toml` now has a single dormant advisory exception for `RUSTSEC-2024-0436` scoped to native Arrow/DataFusion Parquet, unblocking the native Parquet reader and writer/archive tickets without changing dependencies yet.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-06-native-parquet-file-source.md`; `cdf-formats` now reads Parquet file sources through native arrow-rs `parquet 59.0.0`, removes its DuckDB/Arrow 58 Parquet bridge, preserves package replay/source-position/schema-hash behavior, and records explicit scanner evidence for the ratified `RUSTSEC-2024-0436` path through `paste`.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-06-native-parquet-writer-archive.md`; package archive transcode and the Parquet destination writer now use native arrow-rs `ArrowWriter`, remove the scoped DuckDB writer/export path, preserve IPC package identity and receipt/idempotency behavior, and record explicit scanner evidence for the ratified `RUSTSEC-2024-0436` path through `paste`.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-06-local-duckdb-lifecycle-chaos-failpoints.md`; local DuckDB/SQLite package/checkpoint lifecycle chaos now has named runtime failpoints, helper-process conformance coverage for the four ratified windows, exact committed-head reuse after checkpoint commit, and mutation-clean guard coverage.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-declarative-rest-resource-execution.md`; Tier-0 declarative REST resources are now openable through explicit `cdf-http` runtime dependencies with deterministic conformance tests, fail-closed auth/allowlist/schema behavior, cursor pushdown URL effects, and mutation-hardened runtime/parser coverage. This advances the HTTP API and GitHub issues acceptance demo path while leaving live API credentials, CLI REST `run`, SQL source execution, and the full acceptance demo harness to later children.
- 2026-07-07: Opened `.10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md` as the next bounded source-execution child. It will make table-backed Tier-0 Postgres SQL resources openable as `ResourceStream`s while keeping arbitrary SQL `query` execution and CLI/package orchestration for later ratified slices.
- 2026-07-07: User architecture audit and parent source inspection identified the missing general run spine as the next highest-leverage architectural blocker after current source execution work. Recorded `.10x/research/2026-07-07-run-spine-gap-map.md` and opened blocked shaping ticket `.10x/tickets/done/2026-07-07-run-ledger-commit-session-spine-ratification.md` to ratify run-ledger semantics and the destination commit-session abstraction before executable implementation.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-declarative-postgres-sql-resource-execution.md`; table-backed Tier-0 Postgres SQL resources now execute through explicit runtime dependencies with typed pushdown metadata, live Postgres source coverage, cursor source positions, fail-closed unsupported query/dialect/metadata handling, and broad quality evidence. This advances the source breadth lane while leaving SQL-through-CLI/package orchestration to the run-spine work.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-cli-resume-spine.md`; `cdf resume` now recovers run-id-scoped interrupted work through package/receipt/checkpoint facts across DuckDB, filesystem Parquet, and Postgres without source contact after package finalization. The CLI spine parent remained open for direct CLI table-backed SQL `run` success evidence now closed at `.10x/tickets/done/2026-07-07-cli-sql-run-success.md`.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-cli-sql-run-success.md`; `cdf run` now has direct live CLI evidence for a table-backed Postgres SQL source with an exact ordered cursor, source DSN secret-file resolution/redaction, DuckDB destination rows, committed checkpoint cursor head, and terminal `run_succeeded` ledger event.
- 2026-07-07: Updated `.10x/knowledge/cdf-product-objective.md` with the falsifiable CDF 1.0 finish line, decision queue, and P0-P7 concurrent program map from the active goal objective.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-run-ledger-commit-session-spine-ratification.md`; run identity, append-only run ledger ownership, run-to-artifact mapping, resume/replay behavior, inspect-run ownership, and destination commit-session semantics are now ratified by `.10x/decisions/run-ledger-commit-session-spine.md` and `.10x/specs/run-orchestration-ledger.md`. Opened the now-closed `.10x/tickets/done/2026-07-07-run-spine-implementation-program.md` with parallelizable children for the kernel API, per-destination session refactors, run ledger store, general orchestrator, and CLI wiring.
- 2026-07-07: Closed DataFusion delegation triage at `.10x/tickets/done/2026-07-07-datafusion-delegation-pushdown-triage.md`; `.10x/decisions/datafusion-tier-b-delegation-boundary.md` keeps VISION D-1 deep DataFusion delegation as the target while making the current CDF-native loop a temporary fallback. Opened tuple, adapter, and execution-metadata honesty owners so the system stops silently drifting between DataFusion-shaped metadata and actual DataFusion execution.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`; `cdf-engine` now uses the ratified temporary DataFusion git pin on Arrow 59.1.0, first-party Arrow/Parquet crates are on 59.1.0, Python remains on PyO3 0.29 / pyo3-arrow 0.19, golden and supply-chain gates passed, and `.10x/tickets/done/2026-07-07-datafusion-tableprovider-adapter.md` is no longer dependency-gated by Arrow/DataFusion type incompatibility. The unrelated DuckDB Arrow 58 residual is tracked by `.10x/tickets/done/2026-07-07-duckdb-arrow58-transitive-residual.md`.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-datafusion-tableprovider-adapter.md`; `cdf-engine` now has the first generic `QueryableResource` to DataFusion `TableProvider` adapter slice with simple predicate translation, exact/inexact/unsupported pushdown classification, residual safety, projection, and inexact-limit suppression tests. The broader package-producing runtime delegation remains governed by `.10x/decisions/datafusion-tier-b-delegation-boundary.md`.
- 2026-07-07: Landed the filesystem Parquet CLI `run` and `replay package` slices under the CLI spine children, with shared evidence `.10x/evidence/2026-07-07-cli-parquet-run-replay.md` and review `.10x/reviews/2026-07-07-cli-parquet-run-replay-review.md`. CLI REST/Postgres run, Postgres replay, resume, and inspect-run remained open at that point.
- 2026-07-07: Closed CLI `replay package` at `.10x/tickets/done/2026-07-07-cli-replay-package-spine.md`; DuckDB, filesystem Parquet, and Postgres package replay now route through artifact replay APIs with fail-closed Postgres explicit target/dedup policy, source-loss live Postgres replay coverage, and full quality evidence in `.10x/evidence/2026-07-07-cli-postgres-package-replay.md`. At that point, CLI REST/Postgres `run`, `resume`, and `inspect run` remained open; the aggregate CLI spine later closed under `.10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md`.
- 2026-07-07: Closed `.10x/tickets/done/2026-07-07-run-spine-implementation-program.md`; kernel `CommitSession`, DuckDB/Parquet/Postgres session refactors, SQLite run ledger, general orchestrator, non-file cursor checkpoint semantics, and CLI run/resume/replay/inspect now have aggregate evidence and review. The full CLI surface and acceptance demo/conformance work remain active under their own parent tickets.
- 2026-07-08: Closed CLI preview breadth at `.10x/tickets/done/2026-07-07-cli-preview-resource-breadth.md`; preview now supports the broader REST, table-backed SQL, Arrow IPC, and deterministic first-match multi-file local authoring loop without package/destination/checkpoint/run-ledger writes.
- 2026-07-08: Closed CLI contract registry freeze/test at `.10x/tickets/done/2026-07-07-cli-contract-registry-freeze-test.md`; the contract command family now has deterministic `cdf.lock` snapshot freeze/test behavior, fail-closed missing registry handling, drift reporting, and broad quality evidence.
- 2026-07-07: Opened P0 structural-debt stop-line program `.10x/tickets/done/2026-07-07-p0-structural-debt-program.md`. Until Workstreams A, B, and C close, new destination lanes, new source-archetype lanes, and the streaming supervisor are paused.
- 2026-07-08: Closed P0 structural-debt stop-line program `.10x/tickets/done/2026-07-07-p0-structural-debt-program.md`. All six workstreams are done with evidence and adversarial review; the stop-line is fully lifted. The permanent runtime-conformance throughput rule remains active under `.10x/knowledge/runtime-conformance-throughput-rule.md`.
- 2026-07-08: Closed CLI state migrate/recover at `.10x/tickets/done/2026-07-07-cli-state-migrate-recover.md`; local SQLite state migrations are now reportable/idempotent, and package-receipt state recovery is wired through destination receipt verification and `CheckpointStore::commit` without destination row writes. At that point, the full CLI surface still waited on the backfill planner owner.
- 2026-07-08: Closed CLI backfill at `.10x/tickets/done/2026-07-07-cli-backfill-planner.md` and then closed the aggregate CLI surface parent `.10x/tickets/done/2026-07-05-cli-surface.md`; the required headless command surface is no longer a parent-plan blocker, while conformance/demo and post-MVP lanes remain active elsewhere in this record.
- 2026-07-08: Closed `.10x/tickets/done/2026-07-08-mvp-acceptance-demo-fixture-harness.md`; conformance now has a deterministic GitHub-Issues-shaped MVP acceptance demo foundation covering plan, contract freeze/test, DuckDB load, local SQL queryability, post-receipt crash/resume without source contact, replay to a second DuckDB database, duplicate replay no-op, state history, and drift quarantine. The full conformance parent remains open for broader closure and any live-provider operational smoke.

## Blockers

None for the parent plan. Individual child tickets may be dependency-gated.
