Status: done
Created: 2026-08-04
Updated: 2026-08-04
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-d1-project-compilation-manifest-core.md`, `.10x/tickets/done/2026-08-03-d1-compile-cli-and-manifest-sql.md`, `.10x/tickets/done/2026-08-04-d1-5a-project-source-resource-input-authority.md`, `.10x/tickets/done/2026-08-04-d2-datafusion-scalar-relational-ir.md`, `.10x/tickets/done/2026-08-03-c1-semantic-registry-core-consumer-migration.md`

# D3: query-first project authoring current-only cutover

## Scope

Implement the complete ratified D3 authoring/compiler surface in one current-model cutover:

1. Replace project resource discovery with path-fenced, stable enumeration of exactly
   `cdf/<namespace>/<resource>.cdf.sql`; derive canonical resource id/default logical target
   from the path; `cdf/` is an identity-excluded ownership marker and the only enumerated resource
   root.
2. Parse one bare admitted `SELECT` or optional ordered, no-identifier
   `RESOURCE ... AS SELECT` envelope with exact source spans and stable diagnostics.
3. Require one `upstream(source => '<configured_source>', ...)` relation; resolve the typed project
   source and immutable driver before validating all remaining recursive data-only relation
   arguments through the driver's closed resource schema.
4. Use the pinned DataFusion analysis stack and completed D2 seam for the admitted query body,
   lower completely to native typed CDF source/scalar/relational IR, and leave no durable or runtime
   DataFusion plan.
5. Compile exact target, disposition and intrinsic merge keys, cursor, trust, semantic annotation,
   and bounded/drain execution metadata. Resolve omissions through authored/project/built-in/
   failure precedence and retain each value's origin.
6. Preserve configured source, canonical resource id, and logical target as separate typed
   authorities through lock, manifest, lineage, plan/explain/validate/inspect, run, preview, and
   replay selection.
7. Extend the D1 manifest with authored/effective identities, default origins, configured-source/
   driver/config/structured-argument identities, semantic effects, native IR, output schema,
   lineage, pushdown/residual decisions, and rejected-construct diagnostics.
8. Update `cdf init`, `cdf add`/generation, examples, fixtures, generated CLI artifacts, and docs to
   emit only the query-first `cdf/` layout.
9. Delete the superseded wildcard/declarative resource authoring reader and path-bound-source
   prototype. Compile only the current model; add no second reader, migration path, fallback parser,
   feature flag, alias, or dual authority.
10. Keep validation economical: focused crate/test targets during implementation, formatting and
    strict Clippy for affected packages, one affected-boundary certificate after the cutover is
    stable, one thorough independent red-team review, and asynchronous GitHub CI observation.

The primary implementation boundaries are `cdf-project`, `cdf-cli`, `cdf-declarative` only where
its types remain legitimate internal compiler IR, `cdf-expression`/D2 consumption, semantic and
contract consumers, source registry/driver resource schemas, and manifest/lock/query projections.
The executor MUST inspect current ownership before moving code and MUST remove names that retain
retired public authority rather than wrapping them.

## Non-goals

- joins, static lookup joins, cross-resource references, or multiple upstream relations;
- `UNION ALL` or any other set operation;
- aggregation, grouping, windows, recursive queries, or subqueries;
- arbitrary source-native SQL, DDL/DML, stored procedures, arbitrary table functions, or UDFs;
- nondeterministic/ambient/unrepresentable scalar expressions outside D2;
- runtime DataFusion planning or a serialized DataFusion plan;
- generic top-level `WITH`/`OPTIONS`, `SINK`, `FROM SOURCE`, `CREATE RESOURCE`, or SQL resource ids;
- source connection details, secrets, environment endpoints, or source-level policy in SQL;
- resident supervision, macros/Jinja/runtime templating, or row-level Python/WASM hooks;
- changing semantic-registry, package-delete, destination, CDC, or source-driver behavior beyond
  the typed bindings required by D3;
- a whole-workspace test loop after each repair.

## Acceptance Criteria

### Project and identity

- [x] Only regular `cdf/<namespace>/<resource>.cdf.sql` files are current resources;
  namespace/stem/source names use `[a-z][a-z0-9_]{0,127}` with no normalization, path fencing and
  stable-read guarantees remain intact, and deterministic enumeration ignores no malformed
  `.cdf.sql` near-match silently.
- [x] Path derives only namespace, resource id, and default logical target. Configured source is
  explicit in the relation; logical target is explicit/defaulted independently. A namespace/source
  mismatch compiles successfully.
- [x] Every configured source is referenced by at least one accepted resource, without requiring a
  same-named directory. Unknown/unreferenced sources fail `Contract` before external I/O.

### Parser, relation, and defaults

- [x] Bare `SELECT` and optional ordered `RESOURCE [TARGET] [DISPOSITION] [CURSOR] [TRUST]
  [SEMANTICS] [EXECUTION] AS SELECT` forms parse with exact one-based spans. Unknown, repeated,
  contradictory, or out-of-order clauses fail with stable codes.
- [x] `CREATE RESOURCE`, an id after `RESOURCE`, `FROM SOURCE`, `SINK`, generic `WITH`/`OPTIONS`,
  and multiple statements fail through the ordinary current grammar.
- [x] `source` is required exactly once as a named string literal, is removed before driver
  resource-schema validation, and cannot be positional, computed, duplicated, or replaced by a
  type/driver/URI/credential/secret/source-level option.
- [x] Recursive string/number/Boolean/NULL/ARRAY/OBJECT relation values lower through the ordinary
  driver resource schema. Executable expressions and JSON/secret/environment escape hatches fail;
  canonical typed identity is order-independent while authored identity is not.
- [x] Default precedence is exactly authored, applicable typed project default, narrow built-in,
  then failure. Target defaults to resource id, trust defaults to `EXPERIMENTAL`, and `REPLACE`
  defaults only for proven bounded replayable input. Existing `[defaults]` becomes the sole typed
  trust/disposition/execution default authority; it admits no keyless merge, `cdc_apply`,
  `financial`, or `serving` D3 values. Every effective value records origin.

### Metadata and native lowering

- [x] `DISPOSITION APPEND`, `REPLACE`, and `MERGE(key, ...)` compile through native disposition
  authority. Merge keys are nonempty/unique, resolve against final output fields, use package
  duplicate/null/delete authority, and require destination capability.
- [x] `CURSOR` resolves exactly against final output schema and compatible source capability.
- [x] `TRUST EXPERIMENTAL|GOVERNED` binds the exact existing contract presets and their review,
  validation, quarantine, retention, publication, observability, and trust-ledger consequences;
  `GOVERNED` is never defaulted.
- [x] `SEMANTICS (field => 'canonical.reference', ...)` resolves exact C1 definitions/versions/
  hashes/parameters, validates Arrow compatibility and control fields, and changes no physical
  representation.
- [x] `EXECUTION BOUNDED` and the exact complete DRAIN vocabulary lower to native extent/stream
  policy; unknown, repeated, incomplete, overflowing, or inapplicable members fail. Unbounded input
  with no complete explicit/project drain policy fails; resident execution remains unavailable.
- [x] Pinned DataFusion analyzes the admitted one-relation projection/filter/scalar surface and D2
  lowers it completely. Joins, every set operation including `UNION ALL`, aggregates, windows,
  subqueries, unsupported functions, and multiple upstream relations fail before I/O/publication.
- [x] Native runtime execution has no SQL/default/source-resolution branch and no durable DataFusion
  type/plan. Differential fixtures prove DataFusion analysis and native execution agree for
  admitted values, types, nulls, casts, filters, and errors.

### Manifest, product cutover, and quality

- [x] Manifest schema/query projections expose exact authored bytes/hash/form/AST hash, effective
  definition identity, versions, resource path/id/default target, every effective metadata value
  and origin, explicit source/config/driver/args/source-node identity, native IR, schema,
  semantics/contracts, lineage, and pushdown/residual decisions. No secret or DataFusion plan leaks.
- [x] Bare and explicit forms share execution identity only when all effective values/policies/
  dependencies match; authored hashes always remain distinct.
- [x] Compile, validate, plan, explain, run, preview, inspect, `cdf sql`, lock binding, scaffold,
  add/generate, examples, and generated help/man/completions agree on one query-first current model.
- [x] Superseded wildcard/declarative/path-bound-source code and its fixtures are deleted. Tests
  exercise only the current model and its actual contract boundaries.
- [x] Focused parser/project/manifest/CLI/driver-boundary/differential tests pass; affected-package
  formatting and strict Clippy pass; generated artifacts are current; one stable affected-boundary
  certificate passes without repeatedly running the full workspace.
- [x] One independent red-team review attempts to falsify identity separation, default safety,
  parser rejection, secret redaction, native-only runtime, publication atomicity, and no-compat
  deletion. All significant findings are repaired once and the final verdict passes.
- [x] Changes are committed and pushed to `main` in bounded coherent increments; GitHub CI is
  checked asynchronously after pushes and any concrete failure receives evidence-backed repair.

## References

- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/project-source-resource-layout.md`
- `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`
- `.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/decisions/project-manifest-path-compile-and-query-policy.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.10x/skills/audit-project-file-publication/SKILL.md`
- `.10x/decisions/datafusion-deterministic-scalar-closure.md`
- `.10x/specs/datafusion-scalar-relational-ir.md`
- `.10x/specs/semantic-type-registry.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`
- `.10x/tickets/done/2026-08-04-d1-5a-project-source-resource-input-authority.md`
- `.10x/tickets/done/2026-08-04-d2-datafusion-scalar-relational-ir.md`

## Assumptions

### User-ratified

- The complete attached D3 handoff is authoritative in full, including query-first grammar,
  independent identities, explicit source binding, recursive data-only values, exact clause order,
  defaults, merge-key syntax, semantic annotations, bounded/drain execution, relational
  exclusions, identity and manifest laws, diagnostics, and implementation/non-goal lists.
- CDF is net new/customer zero. No backwards compatibility, legacy shim, old state reader, dual
  authoring surface, or transitional debt is permitted.
- DataFusion remains ephemeral query parsing/resolution/coercion/type/simplification authority;
  native CDF IR and ordinary execution remain authoritative.
- Implementation should use economical batch validation, incremental commits/pushes, asynchronous
  CI checks, and one red-team review rather than repeated whole-suite/review cycles.

### Record-backed

- D1 manifest publication and query projection, D1.5a typed config/stable input inventory, D2 scalar
  IR/lowering, C1 canonical semantic registry, crash-safe project publication, driver source/
  resource option schemas, trust presets, and package merge/delete authority exist at the referenced
  seams.
- The old D1.5a source-bound path meaning was never wired as a coequal public runtime reader; D3 is
  the sole cutover owner.

## Journal

- 2026-08-04: Ticket opened after the user ratified the complete D3 handoff and authorized all
  supersession. Governing decisions/specs were replaced before execution; no product source or
  generated artifact was changed in this shaping turn.
- 2026-08-04: Execution began on `main`. Re-read the complete ticket and the required
  `audit-project-file-publication` skill before product changes. The worktree contains only the
  user-owned untracked `.codex/config.toml`, which is outside scope and will remain untouched.
- 2026-08-04: The user reopened only the resource-root noun, considered `sources/`, `cdf/`, and
  `pipelines/`, supplied a full comparative analysis, and delegated the choice. Selected `cdf/` as
  the tool-ownership marker: it avoids the configured-source collision of `sources/` and the
  orchestration promise of `pipelines/`. The root is excluded from identity; namespace, resource,
  explicit configured source, and logical target remain independent. Superseded the never-released
  `resources/` decision directly; no compatibility root or reader was admitted.
- 2026-08-04: Replaced the D1.5a source-bound inventory with stable, path-fenced enumeration of
  `cdf/<namespace>/<resource>.cdf.sql`. The inventory now derives only namespace, canonical resource
  id, and default target; validates configured sources independently; and rejects existing
  `sources/`, `resources/`, and `pipelines/` roots rather than admitting fallback readers. Updated
  focused inventory fixtures to prove namespace/source independence and retired-root rejection.
- 2026-08-04: Added the first D3 compiler boundary in `cdf-engine`: DataFusion's pinned SQL parser
  admits one bare `SELECT`, extracts exactly one `upstream(source => 'configured_source', ...)`,
  lowers recursive data-only relation options to canonical structured values, records exact SQL
  span and authored AST identity, rewrites the relation only in transient analysis, and converts the
  resolved projection/filter graph through D2 into native `RelationalExpressionPlan`. The public
  result contains no DataFusion AST or plan. Focused negative tests cover executable arguments,
  missing/duplicate/computed source, positional/wrong-operator arguments, set operations,
  aggregation, CTEs, multiple statements, DDL/DML, and non-upstream relations.
- 2026-08-04: Added the CDF-owned resource envelope parser in `cdf-project`, separate from
  DataFusion query analysis. It admits bare SELECT as the normal form and the ordered no-identifier
  `RESOURCE [TARGET] [DISPOSITION] [CURSOR] [TRUST] [SEMANTICS] [EXECUTION] AS SELECT` form,
  retains one-based spans, parses intrinsic merge keys and semantic references, and lowers bounded
  or complete drain syntax directly into the existing typed execution declarations. It rejects
  identifiers after RESOURCE, retired CREATE RESOURCE, repeated/out-of-order clauses, empty or
  duplicate merge/semantic bindings, zero drain limits, and incomplete drain policy with stable
  `CDF-D3-RESOURCE-*` diagnostics.
- 2026-08-04: Added the typed query-first project compiler over the D1.5a inventory, D3 envelope,
  driver registry, D2 analyzer, and C1 registry. It resolves configured source independently from
  path namespace, validates the driver's closed resource-argument schema before any I/O, records
  authored/project/built-in/path default origins and canonical identities, enforces the bounded
  replayability precondition for the built-in `REPLACE`, compiles source plans with unresolved
  discovery schema authority, and finalizes native relational plans after pinned/discovered schema
  authority becomes available. Output merge keys/cursors and semantic annotations resolve against
  the final projected schema. Removed the cross-driver REST compatibility hook: resource-option
  validation is now the ordinary closed driver schema boundary. Extended relational-plan structural
  validation to admit only the `cdf:semantic` metadata overlay applied after D2 analysis while
  retaining all expression-derived physical metadata invariants.
- 2026-08-04: The user tightened the current-only boundary before CLI fixture conversion. Project
  resource discovery now enumerates only `cdf/` and ignores unrelated directories; it contains no
  sentinel for superseded config keys or alternate roots. Deleted tests whose sole purpose was to
  reject never-released project layouts, removed the special-case `CREATE RESOURCE` migration
  diagnostic, and changed product diagnostics from tranche-labelled `CDF-D3-*` codes to stable
  capability-labelled `CDF-*` codes. The earlier journal statements about rejected alternate roots
  and `CDF-D3-*` diagnostics are superseded by this ratified correction.
- 2026-08-04: Cut the CLI command surface over to the current compiler. Project context now loads
  only query-first resources, commands carry query authority through schema hydration and
  finalization, and ad-hoc runs persist a private `.cdf.sql` definition then compile its typed
  source proposal directly. Removed the hidden ad-hoc TOML compiler round-trip and both
  `parse_declarative_toml`/`parse_declarative_yaml` aliases. Ad-hoc identity is tracked explicitly
  in the invocation context; no configured-source name has reserved behavior.
- 2026-08-04: Converted the scaffold, `cdf add`, executable CLI/conformance fixtures, source
  examples, constant-memory runner, smoke matrix, quickstart, vision examples, and generated error
  reference to `cdf/<namespace>/<resource>.cdf.sql` plus shared `[sources.<name>]` configuration.
  Bulk searches over first-party code/docs/tools now find no wildcard resource mappings, resource
  TOML paths, old project-resource mapping diagnostic, tranche-labelled product codes, or retired
  public parser aliases.
- 2026-08-04: The user classified tests that inspect Rust source text, function/import names, line
  counts, or module layout as invalid quality evidence. Removed the repository's hand-written
  source-token/import/module-cycle/runtime-owner/dependency-direction pseudo-tests while retaining
  behavioral tests and real generated/serialized artifact contracts. Added this boundary and the
  periodic first-party `jscpd`/`cargo machete`/complexity procedure to `QUALITY.md`; narrowed the
  scheduled duplication scan to first-party `crates`, `examples`, and `tools`; and removed every
  dependency exposed as unused by `cargo machete`.
- 2026-08-04: Published the coherent current-only cutover as `e0cb1b48` (`Cut over project
  authoring to query-first SQL`) to `origin/main`. GitHub Actions run `30966145273` passed both the
  Core Rust smoke and tracked-source secret checks.
- 2026-08-04: The one independent red-team review falsified closure with six concrete findings:
  runtime commands reconstructed destination targets instead of consuming compiled target
  authority; non-default source configuration inherited the default environment overlay; manifest
  inputs were reread after compilation and could diverge from the compiled bytes; recursive
  resource arguments admitted secret references; malformed `upstream(...)` diagnostics lacked
  envelope-adjusted AST locations; and an orphaned test-only TOML/YAML path resolver remained.
  Limited closure repair to those findings rather than starting another open-ended review loop.
- 2026-08-04: Centralized compiled resource target access in `ProjectContext` and threaded it
  through run, plan, explain, preview, backfill, deep validation, destination resolution, and
  reports. Ad-hoc run synthesis alone retains its explicitly named last-segment target rule.
  Removed next-command rendering of a nonexistent `cdf run --target` flag. Added destination-plan
  behavior coverage for an explicitly qualified target while making the shared CLI fixture target
  explicit so unrelated behavior retains its intended table identity.
- 2026-08-04: Corrected source environment composition to base configuration plus exactly the
  selected environment's overlay. Added behavior coverage proving a production environment with no
  source overlay receives the base connection and not the development-only connection.
- 2026-08-04: Made project compilation retain the exact `cdf.toml` bytes observed alongside the
  typed config and construct resource manifest inputs from each compiled query's captured authored
  SQL. Manifest validation now requires each resource origin hash and input id to match its
  `ResourceSql` input. The first repair represented those captured inputs as unchanged writes and
  revalidated them before the durable transaction. Focused reviewer follow-up proved that was
  insufficient because unchanged writes were absent from the pending marker and recovery path;
  this statement is superseded by the durable guard repair below.
- 2026-08-04: Resource SQL recursively rejects `secret://` strings at any admitted data-value
  depth, rejects a secret reference in the configured-source position, reports only safe
  remediation, and never echoes the reference. The `cdf add` SQL renderer applies the same recursive
  boundary before writing. `upstream(...)` now derives relation, argument, name, and expression
  diagnostics from SQL-parser AST spans translated through the resource-envelope query offset;
  multiline behavior tests cover both missing-source and nested-secret locations.
- 2026-08-04: Deleted the orphaned `cdf-project/src/sources.rs` TOML/YAML resolver, its module and
  exports, and the unused declarative manifest-input variant. No compatibility reader, rejection
  sentinel, or legacy fixture replaced them.
- 2026-08-04: An initial focused project-test command omitted the repository's documented
  `DUCKDB_DOWNLOAD_LIB=1` developer linkage setting and was incorrectly described as a host
  limitation. The user caught the mistake. Re-ran the exact project test and the qualified-target
  CLI test with the documented setting; both passed. Added a concise always-on `AGENTS.md` reminder
  to rerun an exact focused `-lduckdb` failure with the developer setting before classifying it,
  while preserving the separate static/bundled release rule.
- 2026-08-04: Repaired the final publication finding by introducing distinct read-only
  `ProjectFileGuard` authority. Offline and refresh compilation pass captured `cdf.toml` and
  resource-SQL bytes as guards rather than synthetic writes. Pending transaction marker version 2
  durably journals guard path, byte length, and SHA-256 without storing the guarded content. Normal
  publication and crash recovery verify every guard around installs and immediately before marker
  commit. A fault-injection test edits `cdf.toml` after the first output installs and proves both the
  original publication and recovery fail `Contract`, preserve the edit, retain the pending marker,
  and never install the final lock commit point.
- 2026-08-04: Expanded root `AGENTS.md` with the project-wide invariants repeatedly established by
  this workstream: current-only/no-compat development; behavior/artifact-first tests and a ban on
  Rust-source-text assertions; economical targeted validation with explicit DuckDB, duplication,
  unused-dependency, and cognitive-complexity procedures; crate/SQL/DataFusion/project authority;
  Rust layout and naming; connector correctness/throughput/bounds; receipt/checkpoint/delete
  semantics; safe typed diagnostics; primary-agent implementation ownership; and the preserved
  graphify workflow.

## Blockers

None. D1, D1.5a, D2, and C1 dependencies are terminal; D3 may consume their public seams and closure
evidence without reopening or re-verifying those tickets.

## Evidence

- `cargo check -p cdf-project --lib --locked -j 12` passed after the inventory cutover. This proves
  the new public inventory types and production crate compile; it does not execute fixtures.
- `cargo test -p cdf-project --lib project_input_inventory --locked -j 12` compiled the focused
  target but the final local link failed because the environment could not resolve `-lduckdb`.
  No project-input assertion ran, so this is compile evidence only and is not represented as a test
  pass.
- `cargo check -p cdf-engine --lib --locked -j 12` passed after enabling the explicit DataFusion
  SQL feature and adding D3 analysis. This proves the native parser/analysis boundary type-checks.
- `cargo test -p cdf-engine --lib sql_analysis --locked -j 12` passed 6 focused tests. This proves
  recursive option parsing, order-independent canonical argument identity, authored AST identity,
  DataFusion-to-D2 native lowering, and the enumerated rejection cases; it does not yet prove the
  RESOURCE envelope, driver-schema resolution, defaults, manifest integration, or CLI cutover.
- `cargo clippy -p cdf-engine --lib --locked -j 12 -- -D warnings` passed. This is strict lint
  evidence for the new SQL-analysis production boundary only.
- `cargo check -p cdf-project --tests --locked -j 12` passed. This proves all project-inventory
  fixtures and their affected dependency boundary compile, while deliberately avoiding another
  local DuckDB-linked test execution.
- With the existing local DuckDB dylib made available only to the linker,
  `cargo test -p cdf-project --lib resource_file --locked -j 12` passed 6 focused tests. This proves
  bare/envelope parsing, canonical clause ordering, typed bounded/drain lowering, and the enumerated
  envelope rejection cases without running unrelated project tests.
- With the existing local DuckDB dylib made available only to the linker,
  `cargo test -p cdf-project --lib query_compiler --locked -j 12` passed 5 focused tests. This proves
  explicit source/default resolution, namespace/source independence, driver-owned resource-option
  rejection without secret-value echo, bounded-replay safety for the built-in disposition, native
  SQL lowering, output semantic binding, and exact full-file query-span translation.
- `cargo clippy -p cdf-contract -p cdf-project -p cdf-runtime -p cdf-source-rest --lib --locked -j
  12 -- -D warnings` passed. This is strict lint evidence for the compiler/default/semantic-metadata
  boundary and the removal of the obsolete source-option compatibility hook; it is not a whole-
  workspace certificate.
- `cargo check -p cdf-cli -p cdf-project -p cdf-engine -p cdf-runtime -p cdf-kernel -p cdf-memory
  -p cdf-package -p cdf-conformance -p cdf-foreign-stream -p cdf-source-sqlite -p cdf-dest-sqlite
  --tests --locked -j 12` passed after the query-first fixture conversion and removal of structural
  pseudo-tests. This is compile evidence for the affected test surfaces, not behavioral execution.
- `cargo machete --with-metadata` initially identified six first-party unused dependencies exposed
  by the current-only/test cleanup plus one unused vendored dev dependency. After deletion, the
  repeated command reported no unused dependencies.
- First-party `jscpd` over `crates examples tools` at the scheduled 12-line/80-token thresholds
  passed with 2.4469% duplicated lines and 2.6253% duplicated tokens. A whole-tree invocation was
  rejected as a useful gate because immutable evidence archives and vendored/generated upstream
  code raised the aggregate to 10.11%; CI and `QUALITY.md` now encode the meaningful first-party
  scope.
- Explicit `clippy::cognitive_complexity` diagnostics confirmed the lint is allow-by-default and
  therefore not covered by ordinary `-D warnings`. The diagnostic surfaced six first-party
  functions: one existing engine preview function touched by the native relational integration and
  five existing kernel/state/contract/subprocess functions. No newly introduced query/compiler
  function crossed the default threshold. This heuristic inventory is recorded honestly and is
  not represented as a strict gate.
- `cargo clippy -p cdf-source-rest -p cdf-engine -p cdf-project -p cdf-cli -p cdf-conformance -p
  cdf-runtime -p cdf-kernel -p cdf-memory -p cdf-package -p cdf-foreign-stream -p
  cdf-source-sqlite -p cdf-dest-sqlite --tests --locked -j 12 -- -D warnings` passed.
- `cargo test -p cdf-engine --lib project_query --locked -j 12` passed six focused parser/lowering
  tests, and `cargo test -p cdf-engine --lib
  query_first_relational_plan_executes_before_cli_residual_projection_and_limit --locked -j 12`
  passed its native execution-order law.
- `cargo run -p cdf-cli-core --locked --features cli-artifacts --bin
  cdf-generate-cli-artifacts -- --docs-dir docs --docs-only --check` passed after regenerating the
  current error reference.
- `cargo test -p cdf-engine --lib sql_analysis --locked -j12` passed all eight focused tests after
  closure repair. This proves the admitted parser/lowering behavior plus envelope-adjusted malformed
  argument locations and recursive secret-reference rejection without secret echo; it does not
  exercise project publication or a destination.
- `cargo check -p cdf-engine -p cdf-project -p cdf-cli --tests --locked -j12` passed after all six
  closure repairs. This proves the affected production and test graph compiles, including target
  authority, manifest snapshots, source overlays, and deletion of the legacy module.
- `cargo clippy -p cdf-engine -p cdf-project -p cdf-cli --tests --locked -j12 -- -D warnings`
  passed after all closure repairs.
- The first `cargo test -p cdf-project --lib
  selected_environment_does_not_inherit_the_default_source_overlay --locked -j12` invocation
  omitted `DUCKDB_DOWNLOAD_LIB=1` and failed to link. That result is invalid as environment evidence.
  `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib
  selected_environment_does_not_inherit_the_default_source_overlay --locked -j12` then passed the
  exact focused behavior test: 1 passed, 294 filtered out.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-cli --lib
  tests::planning::plan_uses_the_compiled_resource_target --locked -j12 -- --exact` passed: 1
  passed, 272 filtered out. This observes the explicitly qualified compiled target at the real CLI
  destination-planning report boundary.
- `cargo machete --with-metadata` again reported no unused dependencies after deleting the legacy
  resolver and manifest variant.
- `cargo clippy -p cdf-engine -p cdf-project -p cdf-cli --lib --locked -j12 -- -W
  clippy::cognitive_complexity` completed successfully. It reported the same six pre-existing
  threshold crossings in transitive first-party packages and no changed D3 function. Ordinary
  strict Clippy remains separate because this restriction lint is allow-by-default.
- Focused `rg` sweeps found no Rust test reading a `.rs` source file or asserting Rust function,
  import, token, line-count, or module-layout text; remaining `include_str!` uses consume benchmark,
  conformance, protocol, generated, or documented artifact contracts rather than Rust source.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-project --lib
  project_files::tests:: --locked -j12` passed all 20 project-file transaction tests. The batch
  includes fault injection proving a captured authored input changed after one output install blocks
  both commit and forward recovery without overwriting the input or installing the final lock.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo check -p cdf-engine -p cdf-project -p cdf-cli
  --tests --locked -j12` passed after the durable-guard change.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo clippy -p cdf-engine -p cdf-project -p cdf-cli
  --all-targets --locked -j12 -- -D warnings` passed after folding test-only failure injection into
  the transaction hook context.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo clippy -p cdf-engine -p cdf-project -p cdf-cli
  --lib --locked -j12 -- -W clippy::cognitive_complexity` completed successfully. It reported the
  same six pre-existing threshold crossings in transitive first-party packages and none in the D3
  compiler/publication repair.
- GitHub Actions `Fast Quality` run `30968316660` passed for closure-repair commit `f24eee00` after
  both the Core Rust smoke and tracked-source secret checks completed successfully. Together with
  the earlier passing incremental runs, this closes the asynchronous CI acceptance criterion; it
  does not substitute for the focused behavioral evidence above.

## Review

The independent red-team review initially returned `fail` with six concrete findings: compiled
target authority was not threaded to runtime commands; non-default source overlays inherited the
default environment; manifest inputs could be reread from a different filesystem snapshot;
recursive resource values could carry secret references; upstream diagnostics lacked exact
envelope-adjusted locations; and the orphaned TOML/YAML source resolver remained. All six received
bounded repairs and focused behavior/compile/lint evidence.

The first follow-up passed five findings but retained `fail` for manifest publication because the
initial authored-input checks were unchanged writes omitted from the durable pending marker and
recovery. The final focused, read-only reviewer pass examined only that defect and returned `pass`:
authored inputs are distinct durable marker guards; path/length/hash authority is revalidated before
publication, around every install, immediately before commit, and during recovery; and fault
injection proves a post-install edit is preserved while publication and recovery fail closed.

Residual risk: the review and local validation are intentionally affected-surface scoped. GitHub CI
remains the asynchronous repository smoke boundary after this repair is pushed.

## Retrospective

The hard failure was treating an unchanged write as equivalent to durable read authority. That was
true before the pending marker but false after a crash: only installed writes survived in the
journal. Separating `ProjectFileGuard` from `ProjectFileWrite` made the invariant explicit and
allowed recovery to enforce the same snapshot contract as the original publisher without storing
authored contents in private transaction state.

The other recurring friction was process, not Rust: broad quality machinery had obscured focused
behavioral evidence, a documented DuckDB build variable was initially missed, and source-text tests
encoded repository shape instead of product behavior. Root `AGENTS.md` now makes these boundaries
always-on. The effective loop was: bulk-search the obsolete surface, repair one owned seam, run the
smallest behavior batch plus affected check/strict Clippy, then ask the original reviewer to
re-evaluate only its surviving falsification.

D3 is complete. Its implementation landed in bounded commits through `f24eee00`, every acceptance
criterion maps to the evidence above, the independent review verdict is pass, the current-only
query-first authoring surface has no compatibility reader, and the final pushed implementation
commit passed GitHub Actions.
