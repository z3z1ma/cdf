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
- `.10x/tickets/2026-07-05-package-builder-reader.md`
- `.10x/tickets/2026-07-05-contract-compiler-normalization.md`
- `.10x/tickets/2026-07-05-datafusion-engine-planner.md`

MVP authoring, destinations, and product surface:

- `.10x/tickets/2026-07-05-http-toolkit.md`
- `.10x/tickets/2026-07-05-declarative-resources.md`
- `.10x/tickets/2026-07-05-formats-and-subprocess.md`
- `.10x/tickets/2026-07-05-python-sdk-bridge.md`
- `.10x/tickets/2026-07-05-duckdb-destination.md`
- `.10x/tickets/2026-07-05-parquet-object-store-destination.md`
- `.10x/tickets/2026-07-05-postgres-destination.md`
- `.10x/tickets/2026-07-05-project-format-lockfile-secrets.md`
- `.10x/tickets/2026-07-05-cli-surface.md`
- `.10x/tickets/2026-07-05-observability-doctor-status-sql.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/2026-07-05-dlt-shim-preview.md`

Fast-follow and full-system completion:

- `.10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md`
- `.10x/tickets/2026-07-05-wasm-components-registry-signing.md`
- `.10x/tickets/2026-07-05-cdc-and-streaming-supervisor.md`
- `.10x/tickets/2026-07-05-distributed-execution-and-remote-state.md`
- `.10x/tickets/2026-07-05-lakehouse-warehouse-and-vault.md`
- `.10x/tickets/2026-07-06-ratify-supply-chain-policy.md`

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

## Blockers

None for the parent plan. Individual child tickets may be dependency-gated.
