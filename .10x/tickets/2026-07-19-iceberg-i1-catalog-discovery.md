Status: active
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/done/2026-07-19-iceberg-f1-neutral-object-access.md, .10x/tickets/done/2026-07-19-iceberg-f2-dependency-foundation.md, .10x/tickets/done/2026-07-19-iceberg-f3-table-snapshot-position.md

# Iceberg I1: catalog bindings, discovery, and compiled snapshot plan

## Scope

Implement the Iceberg source driver, config/schema, REST and Glue catalog bindings, generation-bound metadata reuse, exact schema discovery/pinning, compiled snapshot physical plan, add/deep-validation/inspect/doctor hooks, and local filesystem/REST discovery conformance.

## Non-goals

No data-file scan, deletes, incrementality, Glue conventional external tables, catalog mutation, or generic command branch.

## Acceptance Criteria

- Local/filesystem, REST, and Glue bindings produce identical table/snapshot semantics.
- Discovery reads metadata only, preserves Iceberg field/schema metadata, pins through ordinary CDF snapshots, and reuses same-command observations.
- The compiled plan binds exact catalog/table/ref/snapshot/metadata/schema/spec/predicate/capability authority with redacted options.
- Add/deep validation/inspect/doctor use registry hooks; local REST conformance and jobs-independent plan hashes pass.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/specs/source-extension-runtime-contract.md`

## Assumptions

- User-ratified 2026-07-19: REST neutral interface with Glue as binding.

## Journal

- 2026-07-19: Activated after F1-F4 closure. This lane owns only metadata/catalog discovery and compiled snapshot planning; payload reads/deletes remain I2. The implementation will compose injected object access and the source registry, and will not introduce generic Iceberg/Glue branches.
- 2026-07-19: Added strict typed source/resource configuration and a catalog-local registry with filesystem, Iceberg REST, and injected Glue bindings. All object metadata reads use `cdf-object-access`; REST uses injected `cdf-http`; Glue exposes only a CDF-owned pointer contract to its host adapter. Metadata bodies, JSON parse amplification, REST control responses, and Glue pointer state are charged to the shared discovery ledger through explicit configurable limits.
- 2026-07-19: Corrected REST routing to follow the protocol lifecycle: `GET /v1/config?warehouse=...` negotiates `uri`/`prefix`, then load-table uses the negotiated path. Warehouse is never treated as a path prefix. Both responses are retained for same-command reuse and report actual transferred bytes without counting internal copies.
- 2026-07-19: Catalog selection now freezes exact snapshot/ref/timestamp authority, accepts truly empty current tables without fabricating a snapshot, rejects invalid refs and unsupported format versions, and preserves Iceberg field/schema IDs, docs, defaults, source names, required flags, and physical types in the Arrow schema.
- 2026-07-19: Focused verification: `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib` passed 14 tests; `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --lib -- -D warnings` passed. These observations cover typed configuration, protocol routing, schema conversion, task invariants, and lint cleanliness; driver/manifest/live catalog conformance remains pending.
- 2026-07-19: Installed the Iceberg driver boundary and a canonical external-task planner. Discovery retains the exact generation-bound table metadata for same-command resolution; the physical plan contains only typed/redacted source and resource options; planning reads manifest-list and manifest bytes through the injected object-access authority and writes append/spill-backed task artifacts through `cdf-task-store`. Task and concurrency limits are explicit user knobs rather than hidden caps.
- 2026-07-19: Corrected a schema-evolution authority defect before execution: the task-set now records one fixed `output_schema_id`, while every data task records its historical `file_schema_id`. Projection and equality-delete semantics bind to the output schema; partition tuples bind to the file schema and its historical partition spec. Conflating these would have rejected valid evolved tables or decoded partitions under the wrong schema.
- 2026-07-19: Focused verification after driver/planner integration: `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib` passed 14/14; `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets -- -D warnings` passed. This proves crate compilation/lint cleanliness and existing catalog/config/task assertions; local catalog fixtures and task-planner behavioral coverage remain pending.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
