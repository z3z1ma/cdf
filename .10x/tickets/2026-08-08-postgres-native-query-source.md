Status: active
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
- 2026-08-08: Execution started after MongoDB native extraction closed. The existing binary COPY
  decoder's 65,536-row adaptive target and 32 MiB source ceiling remain the production defaults;
  `output_batch_rows` will only tighten the row target when explicitly configured. PostgreSQL
  owns local AST classification, server read-only transactions, prepare/describe discovery,
  transaction-local settings, derived-relation wrapping, and descriptor reattestation. The error
  audit remains scoped to preserving existing typed CDF and SQLSTATE ownership through the new
  prepare/settings/COPY call sites; no wrapper rewrite is authorized.
- 2026-08-08: Implemented the PostgreSQL-owned native-query boundary: exactly-one table/query
  validation, read-only AST admission, prepare-only discovery, output-generation identity,
  portable-plan re-description, transaction-local isolation/timeouts/search path, derived-relation
  wrapping, exact outer scan operations, and the existing binary COPY decoder. The default
  65,536-row decoder constructor remains the literal default path; only an explicit nondefault
  `output_batch_rows` selects the bounded constructor.
- 2026-08-08: Added native-query `cdf add` proposals, safe human evidence, exact compiled physical
  authority, operator documentation, and behavior tests for accepted/rejected SQL, literal
  redaction, option bounds, query wrapping, private DSN publication, and configurable Arrow batch
  boundaries. `cargo test -p cdf-source-postgres` passed 35 tests with the one environment-backed
  case ignored; rerunning that case against PostgreSQL 17 at the established local fixture passed.
  Strict affected-crate Clippy and `cargo machete --with-metadata` passed. The explicit cognitive-
  complexity diagnostic reported only the pre-existing `cdf-kernel::arrow_type` warning, outside
  this connector change.
- 2026-08-08: The first release-binary sandbox compile exposed the manifest's deliberate ban on
  control characters when exact multiline SQL was serialized as a plain string. Preserved that
  security fence and exact query bytes by base64-encoding only the serialized physical-plan field;
  adapter memory and PostgreSQL still receive the exact decoded SQL. Added round-trip/redaction
  coverage, and both connector and builtin-catalog suites passed after the repair.
- 2026-08-08: The first portable-plan run streamed 500,000 joined/windowed rows in 1.4 seconds
  through ten binary COPY batches, then correctly refused to checkpoint because query-generation
  authority had not been attached to full-scan batches. Repaired the authority separation by
  retaining generation as portable preflight/planned authority while emitting it as the bounded
  query's full-scan completion position. Added explicit full-query resume validation and exact
  cursor-start predicates for String, signed/unsigned integer, finite float, Date32, and timestamp
  domains. The focused suite now passes 36 tests plus one live ignored test.
- 2026-08-08: After the successful 500,000-row package/receipt/checkpoint lifecycle, planning the
  independently compiled `VALUES` resource found that its cached pre-encoding physical plan had
  not been invalidated. Bumped the current PostgreSQL driver authority to 2.1.0 so all cached plans
  recompile. Deliberately added no legacy physical-plan reader because CDF remains unreleased.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
