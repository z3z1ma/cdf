Status: done
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

- 2026-08-08: Live release compilation after the Postgres driver version advanced exposed a project-layer authority defect: active schema hydration verified project/config/authored bytes but could reuse a physical source plan compiled by an older registered driver. The resulting artifact mixed the current configured-source identity with stale adapter authority and correctly failed manifest validation. The CLI now compares the hydrated artifact's compiler-owned discovery binding with the current resource binding and falls back to fresh discovery on mismatch; active logical schema remains the drift gate.

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

- Exactly-one input, SQL admission, settings bounds, query identity/redaction, canonical wrapping,
  cursor rebinding, generation checkpoints, and batch-bound behavior are covered by the 36 passing
  `cdf-source-postgres` tests. The environment-backed PostgreSQL 17 test also passed when selected;
  it preserves JSON, UUID, exact numeric domains, COPY framing, and the unchanged table path.
- `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-cli` and strict all-target Clippy for `cdf-cli`,
  `cdf-source-postgres`, and `cdf-benchmarks` passed. `cargo machete --with-metadata` passed after
  the dependency changes. The focused canonical roofline-query contract test passed.
- Release sandbox static validation reported 24/24 resources valid. Release compilation of
  `postgres_native.ledger_enriched` and `postgres_native.value_set` passed with active schema
  generation 1 after the driver advanced to 2.1.0. Repeating the same compile while the PostgreSQL
  container was stopped passed, proving current active artifacts are reused without source contact.
- A stale driver-2.0 portable plan failed preflight with exit 3 and
  `compiled source plan driver authority ... does not match`, before extraction or destination
  effects. A current ledger plan exported successfully to
  `.cdf/postgres-native-ledger-current.plan.json`.
- `cdf plan postgres_native.value_set --out ...` followed by `cdf run --plan ...` passed portable
  preflight and committed 3 exact rows, one package, a destination receipt, and its checkpoint.
  `cdf inspect run run-1df124b3eaa73b1c79625ad6f1d1a1ea` reported terminal success with every
  artifact available and the checkpoint committed.
- The release ledger run executed a multiline CTE/join/window/aggregate/native-`md5` query and
  streamed 500,000 rows / 88 MiB through ten binary COPY batches in 1.5 seconds, then packaged,
  validated, committed, verified, receipted, and checkpointed them. DuckDB independently reported
  500,000 rows, ids 1..500000, and the expected running-amount domain. Artifact-only replay returned
  `already loaded` from the receipt without source extraction.
- `.10x/evidence/.storage/2026-08-08-postgres-native-query-roofline.json` is a passing PostgreSQL
  17.10 `bench-max` certificate over 2,000,000 rows and 11 interleaved samples. Relative to the
  official `postgres` binary-COPY decoder, CDF retained 0.956 narrow, 0.992 mixed-decimal, and 0.970
  native CTE/join/window throughput. Every cell stayed below 10% median absolute deviation and the
  maximum CDF batch remained exactly 65,536 rows.

## Review

Pass. A focused final review compared the product and benchmark diffs with the active native-query
specification. It found and repaired two closure defects: active schema hydration could reuse a
source plan from an older registered driver, and the first native-query official-client benchmark
used semantically equivalent alias-qualified SQL instead of the adapter's exact canonical SQL.
The final implementation invalidates mismatched compiler-owned discovery bindings and the final
roofline executes the exact canonical COPY statement on both paths. No critical, significant, or
minor finding remains. Per the user's direction, independent red-team review remains consolidated
at the end of the connector tranche rather than repeated per ticket.

## Retrospective

The adapter-native implementation reused the right stable seam: PostgreSQL prepares and describes
its query, while the existing binary COPY decoder remains the only transport/type/performance
authority. The most costly failures were cross-layer identity omissions, not SQL mechanics:
multiline query bytes needed safe artifact encoding, full scans needed explicit generation
positions, and active schema cache reuse needed a current driver-binding check. Live lifecycle
testing found each before closure. The initial 500k roofline cells were too short and scheduler-
sensitive; raising the fixture to 2M rows and taking 11 interleaved samples produced a stable,
honest certificate without weakening the 90% floor. Future query-capable connectors should certify
an adapter-native query cell from the outset and bind cache reuse to the compiler-owned discovery
identity rather than project bytes alone.
