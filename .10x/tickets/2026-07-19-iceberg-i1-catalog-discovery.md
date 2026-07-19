Status: blocked
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/2026-07-19-iceberg-f1-neutral-object-access.md, .10x/tickets/2026-07-19-iceberg-f2-arrow59-dependency.md, .10x/tickets/2026-07-19-iceberg-f3-table-snapshot-position.md

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

None yet.

## Blockers

Dependencies above.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
