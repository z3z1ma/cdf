Status: active
Created: 2026-08-08
Updated: 2026-08-09
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
- 2026-08-09: The user ratified source defaults plus explicit resource overrides for operational
  controls. The MySQL implementation MUST ship that precedence from its first version; no
  resource-only intermediate surface or compatibility path is allowed.
- 2026-08-09: Execution started from the accepted native-query contract. Selected
  `mysql_async` 0.37's prepared binary result stream as the production transport: it streams rows
  without materializing the result and keeps transport backpressure in the async client. Cursor
  authoring remains conditional runtime intent; bounded/replace resources do not require one.
- 2026-08-09: Implemented the finite MySQL driver, configured-source catalog discovery, `cdf add`,
  exact prepared metadata, binary streaming decode, read-only consistent snapshots, bounded Arrow
  batches, portable attestation, and catalog enrollment. A live replace rerun exposed that prepared
  schema generation had been mistaken for cursor resume authority. Corrected the seam: generation
  attests a portable/full scan, a replace rerun clears its start position, and only a descriptor
  with an authored cursor binds a cursor predicate.
- 2026-08-09: Release E2E against MySQL 8.4.11 covered a 14-domain type sheet, a CTE/window/JSON
  native query, a 250,000-row full scan, configured-source discovery, portable plans, incremental
  cursor advancement, package replay, DuckDB receipts/checkpoints, and query/credential redaction.
  The full CDF pipeline loaded 250,000 mixed rows (11 MiB) in 1.0--1.2 seconds; the cursor rerun
  loaded exactly the one newly inserted row. No stack-size override was used.

## Blockers

None.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-mysql --lib`: 8 passed, covering strict query
  classification, option bounds/precedence, identifiers, metadata type mapping, redacted compile,
  and cursor-optional `cdf add`.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-source-mysql --all-targets -- -D warnings` and
  `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-cli --bin cdf`: passed.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-builtin-drivers
  tests::catalog_matches_the_data_driven_first_party_fixture -- --exact`: passed.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo build -p cdf-benchmarks --bin
  mysql-source-roofline --profile bench-max -j 12`, followed by five measured samples per cell on
  MySQL 8.4.11: all six comparable cells passed the 0.90 floor. The 65,536-row production default
  measured 0.956 of the direct `mysql_async` path for the mixed table and 0.939 for the native
  window query; the fastest aggregate sweep setting was 8,192 rows at a minimum 1.013 ratio. Full
  samples, dispersion, memory, host, executable, and comparison identity are recorded in
  `.10x/evidence/.storage/2026-08-09-mysql-source-roofline.json`.
- Release `cdf discover source mysql_native --progress never`: complete `mysql_relation` catalog;
  both live tables carried exact prepared schemas (6 and 14 fields).
- Release bounded runs: exact decimal, UTF-8, binary, bit, JSON, negative/max TIME, enum/set, and
  spatial values survived source-to-DuckDB; a second replace run re-read two rows without a cursor.
- Release cursor run: initial append committed 250,000 rows, then a newly inserted `id=250001`
  produced exactly one row and advanced the cursor checkpoint. Replaying that retained package was
  an existing-receipt no-op.
- Remaining tranche closure evidence: independent review after the CDC adapters land.

## Review

Pending.

## Retrospective

- Live portable execution found the boundary unit tests missed: replace resources need generation
  attestation but no resume cursor. Keeping native source position and authored cursor as separate
  types made the correction surgical and preserved exact incremental behavior.
- Prepared binary metadata and values cover MySQL's otherwise awkward decimal, temporal, JSON,
  bit, and spatial domains without guesswork. Text is the lossless CDF representation where Arrow
  lacks the source domain.
- A fair same-client roofline needs logical Arrow bytes rather than retained buffer capacity;
  otherwise different but equally valid builder allocation strategies falsely appear
  non-equivalent. The final comparator verifies ordered identity and complete content separately
  from retained-memory telemetry.
