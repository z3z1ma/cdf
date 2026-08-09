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

## Review

Pending tranche-level review.

## Retrospective

Pending implementation.
