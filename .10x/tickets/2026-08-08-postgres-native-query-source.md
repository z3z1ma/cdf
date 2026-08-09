Status: open
Created: 2026-08-08
Updated: 2026-08-08
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-postgres-source-binary-copy.md`

# PostgreSQL native query source

## Scope

Extend `cdf-source-postgres` from table-only resources to adapter-native read-query resources.
Prepare/describe the exact query, wrap its output in the existing exact-cast binary COPY path,
apply CDF relational/cursor intent outside it, expose bounded transaction-local controls, and
certify the lifecycle and direct-client roofline.

## Non-goals

Writes, multiple statements, calls, credential literals, generic SQL/dialect abstractions, CDC
logical replication, a row/portal transport fallback, or changes to destination semantics.

## Acceptance Criteria

- Resource options enforce exactly one of `table`/`query` and implement every option and failure
  rule in `.10x/specs/postgres-native-query-source.md`.
- Discovery prepares/describes without payload execution; runtime uses one server-enforced
  read-only transaction and binary `COPY (SELECT ...) TO STDOUT` with exact canonical casts.
- Joins, CTEs, values, aggregates, windows, set operations, and native read functions work; DDL,
  DML, COPY, CALL, session/transaction commands, and multiple statements fail safely.
- CDF projection, exact/inexact filters, ordering, limits, cursor/stable-key windows, plan export/
  preflight, schema drift, cancellation, progress, packages, replay, receipts, and checkpoints work
  against query output.
- Query/settings/output-descriptor changes affect identity; diagnostics/reports redact query
  literals and never expose credentials.
- Focused unit/live connector tests and a fair official-client binary-COPY roofline pass without
  weakening existing table-source coverage.

## References

- `.10x/specs/postgres-native-query-source.md`
- `.10x/specs/postgres-source-binary-copy.md`
- `.10x/decisions/connector-native-capability-before-commons.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/database-connector-roofline.md`

## Assumptions

- User-ratified: PostgreSQL owns its native query grammar and controls; no universal source-query
  grammar or pre-MySQL commons extraction is authorized.
- Record-backed: the existing binary COPY path is the production transport and remains the exact
  type/performance authority for query output.

## Journal

- 2026-08-08: Opened after explicit supersession of the table-only/arbitrary-SQL exclusion.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
