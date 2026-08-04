Status: done
Created: 2026-08-04
Updated: 2026-08-04
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-d1-project-compilation-manifest-core.md`, `.10x/tickets/done/2026-08-04-d1-5a-project-source-resource-input-authority.md`

# D2 DataFusion scalar closure and native relational IR

## Scope

Implement `.10x/specs/datafusion-scalar-relational-ir.md` as one coherent current-schema
replacement:

- replace the current untyped/Boolean-limited expression IR and compiled-expression versions with
  a closed typed scalar IR;
- generically lower fully resolved/coerced pinned-DataFusion scalar expressions under the ratified
  built-in/`Immutable`/known-canonical-Arrow admission predicate;
- represent canonical built-in calls, operators, typed literals/columns, implicit casts, `CAST`,
  and `TRY_CAST` with exact types, nullability, dependency identity, and input dependencies;
- add the initial one-input filter-then-explicit-projection relational IR with output schema,
  aliases/ordinals, and field lineage;
- execute the recorded graph batch-vectorially under CDF schema, memory, cancellation, error, and
  control-field authority while reusing the pinned DataFusion/Arrow scalar implementation where
  admitted;
- preserve ordinary projection/filter pushdown fidelity and CDF residual execution without
  generating source-native SQL;
- update every current producer, consumer, fixture, test, golden, and plan/artifact hash directly;
  and
- prove deterministic reload/binding, DataFusion differential equivalence, and the focused
  performance envelope.

This ticket creates reusable compiler/runtime IR only. D3 later parses project SQL/envelopes,
supplies source locations, consumes the D1.5a inventory, lowers `upstream(...)`, publishes the
manifest, and performs the current-authoring cutover.

## Non-goals

- parsing `.cdf.sql`, the CDF resource envelope, or `upstream(...)` arguments;
- selecting project sources, drivers, resource options, destinations, contracts, or environment
  configuration;
- joins, multiple inputs, aggregates/grouping, windows, sets, recursion, subqueries, sorting,
  limiting, DML/DDL, or arbitrary source-native SQL;
- DataFusion aggregate/window/table functions, UDFs, extensions, `Stable`/`Volatile` or
  ambient-session functions;
- SQL semantic-annotation syntax or project-defined-semantic loading;
- manifest/lock publication, scaffold/add/example changes, or CLI/runtime project-mode cutover;
- source-specific scalar-function or SQL branches;
- per-row interpretation/materialization or runtime semantic reanalysis;
- Python/WASM hooks; or
- any prior-IR reader, migration, fallback executor, deprecated namespace, feature flag, dual
  artifact, or compatibility shim.

## Acceptance criteria

1. One new current scalar IR version represents exact typed columns/literals, canonical operators,
   canonical built-in scalar calls, and implicit/explicit/try casts. Every node records resolved
   Arrow input/output type and nullability plus deterministic column/function dependencies; invalid
   or unresolved nodes fail `Contract`.
2. One new current relational IR version represents exactly one input, optional Boolean filter, and
   ordered explicit projection. It records collision-free output names, ordinals, exact Arrow
   schema/nullability, transitive field lineage, and filter-before-project semantics; no excluded
   relational node has an opaque representation.
3. Production scalar admission is one generic predicate over the compiler's pinned DataFusion
   built-in scalar registry. It admits every fully resolved/coerced `Immutable` built-in whose
   inputs/output are in CDF's canonical Arrow closure and whose behavior has no uncaptured ambient
   authority. Repository/static inspection finds no generic growing match/list of function names.
4. Aggregate, window, table, UDF/extension, `Stable`, `Volatile`, session-dependent, unknown, and
   unrepresentable expressions fail compile-time admission even when their output type is known.
   Diagnostics identify the exact failed gate and retain supplied source location.
5. Function aliases and equivalent coerced graphs lower to canonical function/signature/type
   identity and equal execution hashes while authored provenance remains separate. Plan identity
   includes exact DataFusion 54.0.0/dependency feature tuple plus CDF IR/executor versions; it
   contains no DataFusion plan/debug serialization.
6. DataFusion-resolved implicit casts, explicit `CAST`, and `TRY_CAST` lower to distinct durable
   nodes. Focused matrices prove exact output types/nullability and match DataFusion values,
   overflows, parse errors, and null-on-failure behavior across CDF's admitted Arrow closure.
7. Runtime consumes only typed CDF IR. It performs no SQL parsing, optimizer/simplifier pass,
   overload selection, type inference, cast insertion, or output-schema derivation. Binding the
   recorded canonical function to the pinned batch-vectorized implementation verifies exact
   function/signature/input/output/nullability identity and fails stale mismatches before output.
8. Execution remains batch-vectorized and CDF-owned: input/output schema assertions, memory,
   bounded temporaries, cancellation, scalar broadcasting, deterministic error provenance,
   lineage, control fields, and resulting batches stay under ordinary CDF authority. No per-row
   dynamic dispatch or scalar cell materialization is introduced.
9. Differential/property tests compare representative immutable numeric, string, binary, temporal,
   nested, and null-handling built-in families plus operators/casts against direct pinned
   DataFusion analysis/execution over nulls and edge values. They compare schema, values, nulls,
   and errors, not merely row counts.
10. DataFusion-expanded star projections and aliases lower to explicit output fields. Pass-through
    projection reuses arrays without unnecessary materialization; aliases/collisions/empty
    projection behave exactly as the spec requires.
11. Projection/filter pushdown remains ordinary typed capability negotiation. Unsupported/inexact
    scalar predicates remain native residuals and differential tests prove pushed/residual output
    equivalence; generic code emits no source-native SQL.
12. Control-critical CDF operation/key fields cannot be removed, renamed, reinterpreted, or derived
    through the new graph without separately ratified authority.
13. The old expression and compiled-expression versions are replaced across all current producers,
    consumers, fixtures, tests, and goldens. Stale old artifacts fail with regenerate/recompile
    remediation; searches find no compatibility reader, dual executor, fallback, or legacy shim.
14. Representative projection/filter/function-family execution is within 15% of direct pinned
    DataFusion/Arrow physical-expression execution on the same host/batches, matching the existing
    native-filter roofline convention. Any wider gap is profiled and repaired or explicitly
    returned as a closure blocker.
15. Focused affected-crate tests/checks, strict affected-crate Clippy, formatting, and
    `git diff --check` pass. No whole-workspace suite is run. One independent final adversarial
    review attempts to falsify the admission predicate, durable identity, runtime no-reanalysis
    boundary, casts/nulls/errors, version replacement, and performance evidence.

## References

- `.10x/decisions/datafusion-deterministic-scalar-closure.md`
- `.10x/specs/datafusion-scalar-relational-ir.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/project-source-resource-layout.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/decisions/compiled-fused-streaming-operator-graph.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`

## Assumptions

- User-ratified: DataFusion remains the SQL parser/resolver/type/coercion/analyzer; D2 uses a
  rule-based closure over every qualifying deterministic built-in scalar rather than a manually
  curated function-name list; known output type is necessary but immutability, built-in provenance,
  captured semantics, canonical identity, and reproducible batch execution are also required.
- Record-backed: DataFusion 54.0.0 is already pinned; existing CDF expression planning already
  round-trips a Boolean subset through DataFusion analysis into durable CDF authority; current
  package/identity operators may reuse Arrow/DataFusion kernels only beneath CDF contracts and
  differential/performance gates.
- Record-backed: Foundation D1/D1.5a, canonical Arrow closure, source pushdown fidelity, memory,
  control-field, artifact replacement, and no-compatibility policies are active authority.
- Mechanical: exact Rust type/module names and whether pinned scalar binding lives in
  `cdf-expression` or `cdf-engine` are executor choices, provided DataFusion types do not leak into
  neutral kernel/runtime/extension/artifact APIs and one obvious D3 consumption seam results.

## Journal

- 2026-08-04: Opened after the user confirmed the recommended rule-based DataFusion scalar closure.
  Source inspection found the current architecture already converts CDF expressions to DataFusion
  54.0.0 for coercion/simplification and lowers back to CDF identity, while `cdf-expression`
  executes only Boolean/logical/null/comparison kernels and Boolean derives. D2 therefore replaces
  the limited IR/executor rather than adding a SQL parser or duplicating DataFusion's function
  catalog. The ticket deliberately keeps D3 SQL/project parsing and current-authoring cutover out of
  scope.
- 2026-08-04: Execution began after reading the ticket and every referenced active authority in
  full. Repository search confirms `cdf-expression` is consumed only by `cdf-engine`,
  `cdf-format-parquet`, and `cdf-source-files`; the implementation will replace the versioned
  artifact at those current consumers without a compatibility branch. DataFusion remains pinned
  at 54.0.0 and contained beneath engine/expression implementation boundaries.
- 2026-08-04: Replaced the ambiguous executable `Expression`/`ExpressionNode` vocabulary with an
  explicitly non-executable `DeclarativeExpression` AST for scan predicates and native contract
  declarations. Added scalar IR v2 in `cdf-kernel`: every node records canonical Arrow type and
  nullability, recursively derived column/function dependencies, exact column ordinal/name/type,
  canonical operators, explicit cast mode, and the DataFusion 54.0.0/feature/config plus CDF
  executor tuple. Scalar payloads have their own canonical SHA-256 identity. The old scalar and
  compiled-expression artifact shapes have no reader, alias, fallback, feature flag, or dual
  executor.
- 2026-08-04: Added relational IR v1 in `cdf-contract` with one canonical input schema, optional
  Boolean filter, ordered explicit projection, exact output schema, ordinals, lineage, canonically
  ordered control-critical fields, and content identity. Its constructor rejects empty/colliding
  projections and changed/removed control fields. The compiled-expression plan is now v2 and
  carries the same exact dependency tuple even when it contains no expressions; construction now
  validates before returning authority.
- 2026-08-04: Added the D3-facing engine lowering seam over fully resolved, coerced, and simplified
  DataFusion `Expr`. Admission structurally matches the resolved `ScalarUDF` to the pinned built-in
  registry, requires `Immutable`, accepts no name allowlist, and rejects every unrepresentable
  `Expr` variant. It records exact function arguments/result/nullability and lowers implicit,
  explicit, and try casts distinctly. Arrow IPC provides deterministic typed literal bytes without
  leaking DataFusion types into kernel/contract artifacts.
- 2026-08-04: Added the CDF-owned batch executor. It reconstructs only the recorded closed graph,
  resolves exact pinned built-ins, binds directly through DataFusion physical-expression kernels
  without parsing/coercion/simplification/optimization, checks schema/type/nullability before and
  after kernels, broadcasts scalars vectorially, applies SQL true-only filtering, checks
  cancellation between kernels, and reuses pass-through arrays. Engine residuals and derive/filter
  transforms bind once outside batch loops. `cdf-expression` is now only the narrowly documented
  Arrow-native adapter-predicate helper; it is not an identity/replay executor.
- 2026-08-04: Focused regression validation exposed two real integration boundaries and both were
  repaired without weakening contracts. Internal `_cdf_source_row` tracking now receives a
  separately bound exact residual schema instead of allowing arbitrary extra input columns.
  Statistics pruning now consumes the typed graph (preserving analyzed numeric coercions) while an
  explicit proof-safe node/type predicate keeps decimal/timezone and broader scalar support
  conservative; broad D2 execution admission therefore cannot silently broaden payload-skipping
  authority. Projection-only execution fixtures that do not exercise merge semantics now declare
  append/no-control-key resources instead of creating invalid hidden merge-key expectations.
- 2026-08-04: Added focused differential/property coverage for numeric, string, binary, temporal,
  nested, and null-handling families; immutable registry and stable/volatile/UDF gates; aliases and
  hashes; cast modes/parse errors/try-null behavior; serialization/stale identity; filter-before-
  project SQL null behavior; aliases/collisions/empty projection; control fields; zero-copy
  pass-through; generated arithmetic and relational inputs; and scalar plus fused
  filter/projection rooflines. No whole-workspace test suite was run. The repository's `graphify`
  executable was unavailable in this environment, so no graph update could be produced; direct
  source/import/search inspection was used and this limitation does not affect compiled/tested
  artifacts.
- 2026-08-04: The independent final review found one critical and six significant closure defects:
  pre-admission `Stable` function/cast provenance could be erased by simplification; nested source
  locations were not represented; public and production expression evaluation began before CDF
  memory reservation; pass-through Arrow metadata was discarded; typed DataFusion error ownership
  was flattened; the renamed declaration AST still accepted its old v1 authority; and alias/cast/
  error tests did not fully falsify their claims. The user had already authorized all closure
  repairs, so these findings were repaired as one bounded tranche rather than opened into another
  speculative review cycle.
- 2026-08-04: Admission now receives both the resolved pre-simplification graph and optimized
  execution graph when they differ. It structurally admits the former before lowering the latter,
  so constant folding cannot launder `Stable`, UDF, unsupported-node, or cast provenance. Every
  claimed explicit-cast path is consumed exactly once by an actual resolved `Cast` node or
  compilation fails. `plan_expression` supplies its coerced graph as admission authority before
  DataFusion simplification. Focused regressions prove `current_date()` still fails the volatility
  gate after its execution graph is replaced by a literal, and a false explicit-cast claim fails.
- 2026-08-04: Added D3-facing one-based file/line/column locations keyed by exact expression path.
  Lowering records the deepest failed path and preserves the matching authored location in the
  typed `Contract` diagnostic; a nested UDF regression proves the right child location survives.
  Pass-through/alias projections now clone complete Arrow field metadata and schema metadata;
  computed fields start without inherited semantic metadata, and relational validation rejects
  stripped or forged pass-through/control metadata.
- 2026-08-04: Expression execution now acquires the ordinary CDF transform lease before residuals
  or derives in package and preview paths, carries that lease through contract execution, and checks
  each kernel/output working set against it. Public scalar/relational execution requires a caller-
  held `MemoryLease` and rejects undersized authority before kernel evaluation. The synchronous
  normalization helper acquires the same bounded lease from standalone services. This closes the
  prior interval in which an expanding scalar kernel could run before CDF accounting existed.
- 2026-08-04: DataFusion errors now cross one typed phase-aware classifier. Plan/config/schema/
  unsupported failures are `Contract`; value execution/Arrow failures are `Data`; host I/O and
  resource exhaustion are `Environment`; dependency invariants remain `Internal`; and embedded
  `CdfError`, including nested I/O wrappers, retains kind/retry/message. The frozen two-file, 14-site
  inventory and classification ledger are in
  `.10x/evidence/.storage/d2-datafusion-error-{files.txt,ledger.md}`. Declaration/function authority
  moved directly to v2; the adapter executor now accepts only a validated whole declaration, so an
  extracted v1 node cannot bypass current validation.
- 2026-08-04: The same reviewer rechecked exactly the seven original closure findings. Six passed;
  memory remained open because a variable-width DataFusion kernel such as `repeat` could allocate
  beyond the fixed input multiple before the post-kernel observation. CDF now derives a
  function-name-independent allocation bound from the complete typed expression tree and canonical
  Arrow output shapes, includes a two-buffer construction-overlap allowance, and adds that authority
  to the ordinary transform reservation before any kernel runs. Bound relational plans retain the
  original typed root specifically so callers cannot bypass this preflight after binding. The same
  bound is used by public scalar/relational execution and standalone, preview, and package paths.
  Variable-width maxima deliberately fail closed when the managed budget cannot prove capacity.
- 2026-08-04: Focused closure validation passed: the expression module ran 26 tests, the production
  inexact/unsupported residual-package test passed, strict engine Clippy passed for all targets with
  warnings denied, formatting and `git diff --check` passed, and scalar/filter-projection rooflines
  remained at `0.9433`/`0.9301` relative to direct DataFusion. GitHub Fast Quality run `30950283835`
  passed commit `9e2b543c`. The repository-required `graphify update .` was attempted and could not
  run because the executable is absent from this environment. The same reviewer then rechecked only
  the memory finding and returned `PASS`; D2 closed without another review scope.

## Blockers

None. Scope, semantics, constraints, and acceptance are complete from the user-ratified decision and
referenced active records.

## Evidence

1. **Typed scalar authority / invalid-node closure (AC 1, 5, 7, 13).**
   `crates/cdf-kernel/src/expression.rs`, `crates/cdf-contract/src/expression.rs`, and
   `crates/cdf-engine/src/expression_execution.rs` contain scalar IR v2, executor v1, DataFusion
   54.0.0 plus exact feature/config identities, recursive dependency validation, canonical
   SHA-256, and current-only validation. Repository search
   `rg 'EXPRESSION_IR_VERSION|COMPILED_EXPRESSION_PLAN_VERSION: u16 = 1|substrait_version|\.optimized\\b|ExpressionUse::Contract' crates --glob '*.rs'`
   found no old reader/field/contract-use path; the only matching IR symbols were current scalar
   and relational constants. The separately named declaration AST remains source/configuration
   input and cannot enter scalar execution without compilation.
2. **Relational shape, schema, lineage, projection, and controls (AC 2, 10, 12).**
   `cargo test -p cdf-engine expression -- --nocapture` passed 20 focused tests, including
   `relational_plan_filters_before_projection_preserves_control_and_reloads_exactly`,
   `relational_plan_rejects_collisions_empty_projection_and_control_rewrites`,
   `pass_through_projection_is_zero_copy_and_stale_identity_fails_closed`, and
   `filter_then_projection_matches_generated_sql_null_semantics`. This proves the named assertions
   over exact schemas/values/nulls, canonical reload, rejection surfaces, and `Arc::ptr_eq`
   pass-through; it does not claim excluded relational nodes are SQL-parsed, which belongs to D3.
3. **Generic scalar admission and rejection gates (AC 3, 4, 5).** Production admission performs
   structural membership against `SessionStateDefaults::default_scalar_functions()` and checks
   `Volatility::Immutable`; repository inspection found no production function-name allowlist.
   The same 20-test run passed registry-based immutable admission, canonical alias/dependency/hash
   assertions, `random`/`current_date` rejection by volatility, custom immutable UDF rejection by
   built-in provenance, and unsupported `Expr` rejection. Aggregate/window/table constructs have
   no representable scalar-IR variant and reach the closed unmatched-`Expr` rejection; D2 exposes
   no table-function input surface.
4. **Casts, values, nulls, and deterministic errors (AC 6, 9).** The focused expression run passed
   explicit/implicit/try node assertions and direct DataFusion differential execution for success,
   parse failure, and try-null behavior. It also passed 32-case generated arithmetic batches and
   32-case generated filter/projection batches. Representative direct differential cells covered
   `abs` (numeric), `lower` (string), `encode` (binary), `date_part` (temporal),
   `string_to_array`/`array_length` (nested), and `nullif` (null handling), comparing exact Arrow
   `ArrayData` rather than row counts.
5. **CDF runtime envelope and residual truth (AC 7, 8, 11).** The runtime implementation contains
   no SQL parser, session context, optimizer, simplifier, type coercer, or output-schema derivation.
   `cargo test -p cdf-engine inexact_and_unsupported_predicates_are_reapplied_during_execution`
   passed and proved native residual execution on actual package output. `cargo test -p
   cdf-source-files predicate -- --nocapture` passed all 3 matching physical projection/predicate
   tests. `cargo test -p cdf-engine statistics_pruning::tests -- --nocapture` passed all 7 proof
   tests, including generated integer soundness and conservative decimal/timezone behavior.
   `residual_limit_is_consumed_across_partitions` and
   `tier_a_resource_runs_engine_projection_filter_limit_into_package` also passed after exact
   tracking-schema binding. The executor calls vectorized DataFusion/Arrow kernels and checks
   cancellation between calls; no row interpreter or scalar-cell loop was introduced.
   `crates/cdf-engine/src/expression_memory.rs` additionally derives conservative allocation
   authority for the complete typed graph before execution. The focused
   `expanding_scalar_and_prebound_plan_fail_before_an_undersized_lease` regression proves both
   public durable scalar and pre-bound relational entry points reject `repeat(text, 1000000)` under
   a 1 MiB lease before a kernel can run. Production standalone, preview, and package reservations
   consume the same bound and pass the acquired lease through residual and transform evaluation.
6. **Performance (AC 14).** On this host with the same one-million-row batches and already-bound
   physical expressions, `cargo test -p cdf-engine roofline -- --nocapture` passed both guards and
   recorded in the final run:
   - scalar: ratio `0.9433`;
   - filter plus projection: ratio `0.9301`.
   Both are below the allowed `1.15` ratio (with the test's fixed 50-microsecond timer allowance).
   These are debug-profile same-process comparative observations, not production capacity claims.
7. **Focused build quality only (AC 15).** `cargo clippy` with `--all-targets -- -D warnings` passed
   for all 11 directly affected crates: kernel, contract, expression, engine, conformance,
   ClickHouse/files/Glue/Postgres/REST/SQLite sources. The final core rerun passed the same strict
   Clippy wall for kernel/contract/expression/engine. `cargo fmt --all -- --check` and
   `git diff --check` passed. No whole-workspace test suite was run. GitHub Actions `Fast Quality`
   run `30950283835` completed successfully on final implementation commit `9e2b543c`.
8. **Closure-repair falsification (AC 3-8, 10, 12, 13, 15).** The final focused expression run
   passed 26 tests, including stable-function simplification laundering, unmatched explicit-cast
   provenance, exact nested source location, canonical alias resolution, UTF8 parse plus signed
   narrowing overflow/try-null casts, semantic/control metadata preservation and rejection,
   typed/nested error ownership, and pre-acquired-memory enforcement. The same run retained the
   roofline at scalar ratio `0.9558` and filter/projection ratio `0.9486`. Separately,
   `cargo test -p cdf-kernel stale_declarative_expression_v1_deserializes_only_to_fail_current_validation`
   passed; production residual/package and pruning checks passed (`1`, `1`, and `7` tests); contract,
   Parquet predicate, and file-source predicate checks passed (`1`, `1`, and `3` tests). One batched
   strict Clippy command passed for the 12 named affected crates with `--all-targets -- -D warnings`;
   formatting and `git diff --check` passed. No whole-workspace test suite ran. The same independent
   reviewer rechecked only the previously unresolved memory finding after `9e2b543c` and returned
   `PASS`, citing the typed-tree bound, retained pre-bound allocation root, production reservation
   threading, and focused expanding-kernel regression.

## Review

The independent final adversarial review initially returned `fail`: one critical provenance-
laundering defect and six significant gaps in location, memory, metadata, error, version, and test
authority. All seven findings are closed. The same reviewer passed six repairs on the first narrow
recheck, isolated only pre-kernel allocation authority, and returned `PASS` after commit `9e2b543c`
added the canonical type-derived bound across every execution path. Residual risk is conservative
availability: variable-width expressions reserve theoretical Arrow maxima, so a constrained
deployment can fail before execution even when a particular batch would have been small. That is a
deliberate fail-closed resource boundary, not an allocation escape or correctness exception.

## Retrospective

The durable model split was the decisive simplification: the small declaration AST still serves
source capabilities and native contract rules, while only the new typed graph can execute under
identity. That removed the old type's overloaded role instead of stretching it into a SQL IR.

The first surprise was that the prior untyped plan's `optimized` AST had also been the statistics-
pruning coercion carrier. Replacing it with the original declaration initially widened timestamp
pruning unsafely; reverting to declaration-only lowering then lost valid float coercion. The final
repair consumes the typed graph but independently gates the subset allowed to prove a payload skip.
The general lesson is that execution closure and evidence-proof closure are different authorities;
the latter must not expand merely because the former does.

The second surprise was the internal source-row tracking column. Exact input-schema checks correctly
rejected a residual expression bound before that column was appended. The wrong fix would have been
prefix/extra-column tolerance. Binding a second exact graph for the known tracked schema preserves
the invariant and keeps the hot loop unchanged.

The most effective technique was bind-time falsification: every lowered graph immediately binds to
the pinned physical kernel, so unsupported signatures/types fail during compilation and execution
does no analysis. Differential tests compare the same analyzed `Expr` through direct DataFusion and
recorded CDF paths, and the roofline runs those already-bound paths on the same batch. No recurring
toil or unowned follow-up emerged; D3 can consume the public analyzed-scalar/relational seam without
reopening D2 semantics.

The critical review finding exposed a more general compiler rule: destructive optimization may
produce execution authority only after admission has inspected the resolved pre-optimization
graph. Carrying both transient graphs is smaller and safer than attempting to reconstruct
provenance from optimized nodes. Exact-path consumption applies the same rule to parser-supplied
cast claims. The memory repair likewise clarified that accounting must begin before the first
kernel, not merely before downstream validation retains its output. DataFusion 54 provides no
generic scalar allocation-bound contract, so a typical-size multiplier would only disguise an
unproved premise. Keeping the bound in a dedicated `expression_memory` module preserves generic
function admission while making conservative availability visible and keeping execution/binding
code single-purpose.
