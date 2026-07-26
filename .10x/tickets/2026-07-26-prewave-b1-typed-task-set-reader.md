Status: open
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Extract the typed external task-set reader

## Scope

Move the duplicated Iceberg/Glue external-task authority retention, typed record decoding,
ordinal/content verification, parse-memory accounting, and retained executable-payload lifecycle
into one lower shared implementation. Migrate both sources.

## Non-goals

- No common catalog client, task schema, source position, retry policy, or partition planner.
- No merge of Iceberg and Glue source crates.
- No JSON/stringly task API.

## Acceptance criteria

- One typed shared reader owns authority/task bytes and parse leases exactly once.
- Iceberg and Glue inject their authority/task validation and partition-plan semantics through
  typed hooks or generics.
- Wrong task type/hash/ordinal/content, parse overflow, cancellation, and decode failure have
  shared fail-closed tests.
- Source-specific schema-observation, snapshot/generation, authorization, and retry semantics are
  unchanged and explicitly tested.
- Package hashes, task-set hashes, positions, jobs invariance, and measured source throughput do
  not regress.

## References

- `.10x/specs/catalog-task-source-commons.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/knowledge/source-destination-extension-invariant.md`

## Assumptions

- Source-backed: both task readers use `ExternalTaskSetReader`, accounted encoded/parse memory,
  typed JSON decode, ordinal/content validation, and retained payloads.

## Journal

- 2026-07-26: Direct diff confirmed the common skeleton and the source-specific semantics that
  must remain outside it.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
