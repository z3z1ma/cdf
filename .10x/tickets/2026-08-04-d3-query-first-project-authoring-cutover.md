Status: active
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

- [ ] Only regular `cdf/<namespace>/<resource>.cdf.sql` files are current resources;
  namespace/stem/source names use `[a-z][a-z0-9_]{0,127}` with no normalization, path fencing and
  stable-read guarantees remain intact, and deterministic enumeration ignores no malformed
  `.cdf.sql` near-match silently.
- [ ] Path derives only namespace, resource id, and default logical target. Configured source is
  explicit in the relation; logical target is explicit/defaulted independently. A namespace/source
  mismatch compiles successfully.
- [ ] Every configured source is referenced by at least one accepted resource, without requiring a
  same-named directory. Unknown/unreferenced sources fail `Contract` before external I/O.

### Parser, relation, and defaults

- [ ] Bare `SELECT` and optional ordered `RESOURCE [TARGET] [DISPOSITION] [CURSOR] [TRUST]
  [SEMANTICS] [EXECUTION] AS SELECT` forms parse with exact one-based spans. Unknown, repeated,
  contradictory, or out-of-order clauses fail with stable codes.
- [ ] `CREATE RESOURCE`, an id after `RESOURCE`, `FROM SOURCE`, `SINK`, generic `WITH`/`OPTIONS`,
  and multiple statements fail through the ordinary current grammar.
- [ ] `source` is required exactly once as a named string literal, is removed before driver
  resource-schema validation, and cannot be positional, computed, duplicated, or replaced by a
  type/driver/URI/credential/secret/source-level option.
- [ ] Recursive string/number/Boolean/NULL/ARRAY/OBJECT relation values lower through the ordinary
  driver resource schema. Executable expressions and JSON/secret/environment escape hatches fail;
  canonical typed identity is order-independent while authored identity is not.
- [ ] Default precedence is exactly authored, applicable typed project default, narrow built-in,
  then failure. Target defaults to resource id, trust defaults to `EXPERIMENTAL`, and `REPLACE`
  defaults only for proven bounded replayable input. Existing `[defaults]` becomes the sole typed
  trust/disposition/execution default authority; it admits no keyless merge, `cdc_apply`,
  `financial`, or `serving` D3 values. Every effective value records origin.

### Metadata and native lowering

- [ ] `DISPOSITION APPEND`, `REPLACE`, and `MERGE(key, ...)` compile through native disposition
  authority. Merge keys are nonempty/unique, resolve against final output fields, use package
  duplicate/null/delete authority, and require destination capability.
- [ ] `CURSOR` resolves exactly against final output schema and compatible source capability.
- [ ] `TRUST EXPERIMENTAL|GOVERNED` binds the exact existing contract presets and their review,
  validation, quarantine, retention, publication, observability, and trust-ledger consequences;
  `GOVERNED` is never defaulted.
- [ ] `SEMANTICS (field => 'canonical.reference', ...)` resolves exact C1 definitions/versions/
  hashes/parameters, validates Arrow compatibility and control fields, and changes no physical
  representation.
- [ ] `EXECUTION BOUNDED` and the exact complete DRAIN vocabulary lower to native extent/stream
  policy; unknown, repeated, incomplete, overflowing, or inapplicable members fail. Unbounded input
  with no complete explicit/project drain policy fails; resident execution remains unavailable.
- [ ] Pinned DataFusion analyzes the admitted one-relation projection/filter/scalar surface and D2
  lowers it completely. Joins, every set operation including `UNION ALL`, aggregates, windows,
  subqueries, unsupported functions, and multiple upstream relations fail before I/O/publication.
- [ ] Native runtime execution has no SQL/default/source-resolution branch and no durable DataFusion
  type/plan. Differential fixtures prove DataFusion analysis and native execution agree for
  admitted values, types, nulls, casts, filters, and errors.

### Manifest, product cutover, and quality

- [ ] Manifest schema/query projections expose exact authored bytes/hash/form/AST hash, effective
  definition identity, versions, resource path/id/default target, every effective metadata value
  and origin, explicit source/config/driver/args/source-node identity, native IR, schema,
  semantics/contracts, lineage, and pushdown/residual decisions. No secret or DataFusion plan leaks.
- [ ] Bare and explicit forms share execution identity only when all effective values/policies/
  dependencies match; authored hashes always remain distinct.
- [ ] Compile, validate, plan, explain, run, preview, inspect, `cdf sql`, lock binding, scaffold,
  add/generate, examples, and generated help/man/completions agree on one query-first current model.
- [ ] Superseded wildcard/declarative/path-bound-source code and its fixtures are deleted. Tests
  exercise only the current model and its actual contract boundaries.
- [ ] Focused parser/project/manifest/CLI/driver-boundary/differential tests pass; affected-package
  formatting and strict Clippy pass; generated artifacts are current; one stable affected-boundary
  certificate passes without repeatedly running the full workspace.
- [ ] One independent red-team review attempts to falsify identity separation, default safety,
  parser rejection, secret redaction, native-only runtime, publication atomicity, and no-compat
  deletion. All significant findings are repaired once and the final verdict passes.
- [ ] Changes are committed and pushed to `main` in bounded coherent increments; GitHub CI is
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

## Review

Pending one independent red-team review after the implementation is coherently testable. Review
must record severity, verdict, residual risk, and exact evidence; only concrete correctness,
throughput, security, publication, or contract failures trigger one bounded repair pass.

## Retrospective

Pending execution.
