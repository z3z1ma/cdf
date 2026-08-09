Status: active
Created: 2026-08-07
Updated: 2026-08-09
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/2026-08-07-a1-5-package-native-keyed-effects.md`

# A6.2: routed destination target families

## Scope

Implement `ROUTE BY <field> MAX TARGETS <n>` parsing/lowering, deterministic route tokens and
physical target derivation, protected route authority, package route partitions/identity,
generic output-binding schema authority, homogeneous and heterogeneous routed schema families,
multi-target destination capabilities/plans/receipts/replay, and PostgreSQL/DuckDB atomic family
application with plan/inspect/run evidence.

## Non-goals

- generated resources/checkpoints per route;
- null/default/overflow routes;
- sensitive route keys;
- inferred old-route deletes;
- destinations unable to prove package-atomic multi-target settlement.

## Acceptance criteria

- [ ] Grammar, manifest, and plan carry the route field, fold version, mandatory ceiling, and
      logical base target.
- [ ] Schema authority supports the distinguished primary output and resource-scoped routed output
      bindings independently of destination physical names; compilation, promotion, and
      installation preserve per-output generations and hashes.
- [ ] Safe tokens remain exact; other admitted scalar values use deterministic slug-plus-hash
      tokens under destination identifier bounds with collision rejection.
- [ ] Routing is protected control authority and may be projected out only after route resolution.
- [ ] One package/receipt/checkpoint covers the canonical route map and per-target effects;
      partial/ambiguous settlement advances no checkpoint.
- [ ] Heterogeneous route schemas produce schema-homogeneous per-output segments, independently
      compiled queries and migration plans, and one atomic family settlement; unknown outputs do
      not create runtime authority.
- [ ] PostgreSQL and DuckDB apply all physical targets atomically and replay idempotently.
- [ ] Standalone/shared-extraction execution produces identical package and route identities.
- [ ] Focused parser/package/destination/CLI and release sandbox behavior pass.

## References

- `.10x/specs/routed-destination-target-families.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/mongodb-change-stream-source.md`
- `.10x/research/2026-08-07-routed-target-shared-extraction-readiness.md`

## Assumptions

All naming, privacy, cardinality, output-schema, and settlement semantics are active,
user-ratified authority.

## Journal

- 2026-08-07: Opened after ratification. One logical target remains authority; physical route
  tables are a bounded destination family and never implicit resources.
- 2026-08-09: Scope extended after explicit ratification to support generic heterogeneous output
  schema families. Schema authority is keyed by resource plus output binding, not physical target;
  one package/receipt/checkpoint still settles the complete family atomically.
- 2026-08-09: Status `open` → `active` after A1.5 and the neutral finite-drain certificate closed.
  Execution starts from the current typed query/package/destination authorities; no compatibility
  surface or parallel routed lifecycle will be introduced.
- 2026-08-09: Added the ordered `ROUTE BY <field> MAX TARGETS <n>` envelope, compiled route/fold
  authority, exact privacy classification rejection, and neutral typed-scalar route folding.
  Route values preserve exact safe project tokens; all other admitted scalar values receive a
  bounded human slug plus a typed SHA-256 suffix. Generic output-binding ids are derived from the
  typed value rather than destination names, and complete route families reject null/nested
  values, duplicate/colliding targets, ceiling overflow, and identifier-budget loss.
- 2026-08-09: Generalized durable schema authority from resource-only identity to the typed
  `(resource_id, output_binding)` key. Primary outputs use the explicit `primary` binding; routed
  bindings now receive independent heads, versions, promotion leases, histories, generations, and
  hashes without overloading resource ids or destination names. The pre-production SQLite state
  component advances to version 3 and uses one length-delimited scalar key materialization for the
  tuple across every primary/foreign-key lookup.
- 2026-08-09: Added package-native routed content authority: one hash-bound route family owns an
  exact ordered output/schema/content/segment partition, and one routed receipt owns matching
  per-target counts. Package building, manifest streaming, verified replay, checkpoint validation,
  and destination mirrors now reject missing, duplicate, unassigned, or cross-output segments.
- 2026-08-09: Added the generic finalized-package routed commit boundary and a DuckDB atomic-family
  implementation. DuckDB plans all target tables before data application, performs all DDL and
  row/keyed effects in one transaction, records one package receipt, and treats replay of the same
  package token as a verified duplicate rather than a second application. Other destinations do
  not advertise routed support until they implement the same settlement contract.
- 2026-08-09: Bound homogeneous route families into executable engine plans and partitioned every
  normalized Arrow batch against the exact compiled scalar map. Unknown/null routes fail before
  package publication; route-bound segment ids are globally ordered for portable manifest
  identity; per-output content/count authority is derived from the segments actually persisted.
  Project planning now asks a destination to plan the complete empty family, rejects unadvertised
  atomic-family support, and DuckDB previews all target DDL read-only without creating its file.
- 2026-08-09: Sources can now publish a deterministic compiled route inventory and declare a
  required protected route field. CLI plan/run bind that source authority to the authored logical
  target before extraction. Routed CDC delete batches retain the non-null route field beside the
  ordered effect key while package key authority remains key-only, allowing exact target selection
  without inventing nullable delete payload columns.
- 2026-08-09: Added current `CDC_APPLY` plus explicit delete-policy project authoring and carried it
  through manifest identity into CLI engine planning. Native source delete capture is enabled only
  for compiled CDC resources and is hash-bound to exact driver/physical-plan semantics; ordinary
  dispositions reject delete policy. Live MongoDB database-envelope planning is now exercising the
  homogeneous routed family end to end; heterogeneous per-output planning and PostgreSQL family
  settlement remain open.
- 2026-08-09: Completed the generic heterogeneous execution path for DuckDB. Routed sources may
  publish an exact per-output schema/observation inventory without a fabricated primary schema;
  compilation emits independent route queries and engine plans; runtime admission, SQL execution,
  dedup spill, package construction, verified replay, and destination planning select schema by
  output binding. Spill effect families include canonical schema identity, so unrelated routed
  schemas never share one Arrow writer. DuckDB key-index names include the physical target identity
  and therefore cannot collide when distinct resources route the same upstream values.
- 2026-08-09: Production-profile Atlas typed database CDC certified one heterogeneous family over
  independently discovered invoices and orders schemas. Four events became three homogeneous
  routed segments, one atomic receipt, and one committed event-token checkpoint; receipt counts
  prove a hard delete, an update, and two inserts across the two targets. PostgreSQL atomic-family
  application and shared-extraction identity remain open.

## Blockers

None. A1.5 is complete. Ordinary-row route identity shares the same package/destination model and
must not become a parallel implementation.

## Evidence

- Focused parser/compiler/kernel tests: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project
  resource_file_ --lib`, `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project query_compiler_ --lib`,
  and `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-kernel routing::tests --lib` passed. These prove the
  authored route contract, compiled manifest-visible authority, sensitive/non-scalar rejection,
  deterministic folds, typed output binding, ceiling enforcement, and identifier bounds; they do
  not yet prove package or destination settlement.
- Affected strict Clippy passed: `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-kernel -p cdf-project
  --all-targets -- -D warnings`. `cargo fmt --all -- --check` and `git diff --check` passed.
- Output-bound schema authority: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-state-sqlite
  sqlite_schema_authority --lib` passed 9 tests, including two distinct schemas under one resource;
  `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-kernel schema_authority --lib` passed; and
  `DUCKDB_DOWNLOAD_LIB=1 cargo check --workspace` passed. This proves independent durable keys and
  histories, not yet a family-wide settlement permit or destination application.
- Package/DuckDB settlement: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-kernel effect::tests --lib`
  passed 3 tests and `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-duckdb routed::tests --lib`
  passed. The tests prove exact output-segment partition validation and atomic two-target DuckDB
  application plus package-token replay without duplicate rows; they do not yet prove engine-side
  route partition construction or PostgreSQL settlement.
- Affected strict Clippy passed across kernel, package, runtime, project, DuckDB, PostgreSQL,
  SQLite, and ClickHouse crates with `--all-targets -- -D warnings`; `git diff --check` passed.
- Engine construction: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
  tests::package_evidence::routed_package_partitions_rows_into_exact_compiled_outputs -- --exact`
  passed and proves six normalized rows become three exact schema-homogeneous output partitions in
  one valid package. The test exposed and then guards the portable manifest ordering requirement.
- Routed planning: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project
  runtime_tests::live_adapters::destination_planning_facade_previews_every_routed_duckdb_target_without_writes
  -- --exact` passed, proving family-hash planning, complete per-target migrations, and no DuckDB
  file mutation. `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-duckdb
  routed::tests::routed_rows_commit_atomically_and_replay_once -- --exact` passed again after the
  shared planning refactor. Strict affected-package Clippy passed for engine, runtime, project,
  DuckDB, and CLI with all targets and warnings denied.
- Routed CDC compatibility: the MongoDB adapter tests prove key-plus-route delete payload shape,
  and `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
  cdc_apply_reduces_complete_upserts_and_key_only_deletes_across_effect_families --lib` preserves
  the existing non-routed key-only behavior. A live multi-target routed delete remains part of the
  MongoDB CDC acceptance certificate.
- Heterogeneous package path: focused engine spill testing proves two routed payload schemas remain
  separate through payload and effect-sort spill; package replay testing proves exact per-output
  schema/frontier validation; and DuckDB focused tests prove target-scoped routed key indexes and
  catalog `TIMESTAMPTZ` alias normalization.
- Production-profile sandbox certificate: package
  `pkg-portable-mongo-live-atlas-database-typed-cdc-e2e3-54997-1786317120895846000` contains three
  schema-homogeneous segments for two independently typed output bindings. One DuckDB transaction
  inserted one invoice and one order, updated one order, hard-deleted one invoice, wrote one routed
  receipt, and enabled one resume-token checkpoint. Direct table and receipt inspection confirmed
  every effect and zero missing delete keys.

## Review

Pending tranche-level review.

## Retrospective

Pending implementation.
