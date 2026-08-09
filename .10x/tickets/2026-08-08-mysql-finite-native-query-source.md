Status: open
Created: 2026-08-08
Updated: 2026-08-08
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-c1-semantic-registry-core-consumer-migration.md`

# MySQL finite table and native query source

## Scope

Implement `cdf-source-mysql` for MySQL 8.4 finite table and native read-query resources using a
production-maintained asynchronous Rust client's binary streaming path. Add configuration/secrets,
catalog and prepared-query discovery, complete live type fidelity, consistent snapshots, exact CDF
pushdown/cursors, bounded fetch/output controls, catalog enrollment, lifecycle/conformance, and a
direct-client roofline. Establish the same crate/source identity later extended by binlog CDC.

## Non-goals

Binlog decoding or snapshot-to-log handoff in this ticket, destination writes, generic SQL source
abstractions, row-at-a-time I/O, full-result materialization, stored procedures, writes, multiple
statements, or credential literals.

## Acceptance Criteria

- The crate/source/catalog/add/discovery/compile/portable-plan/runtime surface implements
  `.10x/specs/mysql-native-query-source.md` with exactly one of table/query.
- Query classification plus a server read-only consistent-snapshot transaction prevent writes;
  complex read queries work with exact prepared output discovery.
- Native binary decoding has a live type sheet covering signed/unsigned numbers, exact decimals,
  floats, text/binary/collations, temporal/zero-date domains, JSON, bit, enum/set, and spatial
  behavior without lossy coercion.
- Server fetch and Arrow output windows are independently bounded; cancellation/backpressure,
  retry, egress, memory, and typed MySQL error provenance use injected host authority.
- CDF SQL operations, cursor/stable-key windows, generation/preflight, progress, packages, replay,
  receipts, checkpoints, jobs invariance, and credential/query redaction pass live MySQL 8.4.
- Built-in catalog/conformance integrity and a fair same-client release `bench-max` roofline
  reach the governed floor across representative table/query shapes.
- The resulting source kind/crate can add CDC mode without a second driver identity or finite-path
  compatibility shim.

## References

- `.10x/specs/mysql-native-query-source.md`
- `.10x/decisions/connector-native-capability-before-commons.md`
- `.10x/research/2026-08-07-cdc-mysql-continuous-readiness.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/database-connector-roofline.md`

## Assumptions

- User-ratified: one MySQL source owns finite reads and CDC; finite adapter perfection precedes any
  cross-adapter commons extraction.
- Record-backed: C1 semantic registry and typed MySQL CDC position authority are complete; the
  finite source does not yet publish a binlog checkpoint.

## Journal

- 2026-08-08: Opened as the corrected B2 executable owner after the user rejected pre-adapter
  commons and ratified adapter-native query surfaces.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
