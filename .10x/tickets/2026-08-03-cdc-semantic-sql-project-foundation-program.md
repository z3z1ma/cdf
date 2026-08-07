Status: open
Created: 2026-08-03
Updated: 2026-08-07

# CDC, semantic, and SQL-project foundation program

This is a parent planning/shaping ticket. It is not executable implementation scope. Child
execution tickets are opened only after their focused draft specs are ratified and contain no
behavior-changing assumptions.

## Objective

Prepare CDF's next architectural wave without destabilizing the active finite connector program:

- make checkpoint/source-position authority safe for transaction logs and MongoDB resume tokens;
- extract shared relational-source mechanics before MySQL;
- make semantic definitions first-class versioned compiler authority;
- publish and query a complete project compilation manifest;
- add a SQL-like project front-end that lowers into native CDF plans;
- add plan-declared batch hooks only after runtime/sandbox authority is explicit;
- preserve explicit resource authoring and defer general templating.

## Trigger and priority

The user activated shaping on 2026-08-03 after requesting a deep architecture assessment focused on
CDC, MySQL, MongoDB, configuration/SQL authoring, semantic types, hooks, manifests, and optional
templating. The user supplied an external review and asked CDF to move forward from it with maximum
fidelity in project records.

The external review is reconciled—not copied—by
`.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`.

On 2026-08-07 the user explicitly activated the remaining CDC/MySQL tranche: first-class MongoDB,
Postgres, and MySQL CDC, ordinary MySQL source reads, reuse of the existing Postgres/MongoDB source
crates instead of parallel CDC source kinds, and package cleanup suitable for indefinitely
repeated execution. Current readiness is recorded in
`.10x/research/2026-08-07-cdc-mysql-continuous-readiness.md`.

## Relationship to the active connector program

`.10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md` remains the executable
finite-connector owner. This program MUST NOT silently alter its ratified source/destination
semantics, benchmark gates, review evidence, or strict child ownership.

Recommended integration rule:

- finish the active finite SQLite/ClickHouse/MongoDB program;
- run this program's source-position/CDC and compiler shaping in parallel where files do not
  overlap;
- do not begin overlapping `cdf-source-sql` extraction until the current database source children
  have stable closure evidence;
- make the semantic registry a stop-line for the next connector program after the active wave,
  unless the user explicitly reorders the remaining MongoDB destination;
- keep MongoDB change streams out of the finite MongoDB source ticket.

## Workstream graph

### Foundation lane A — CDC artifact authority

**A0. Protocol research and ratification**

- fresh official-source research for PostgreSQL logical replication positions/transactions,
  MySQL binlog/GTID positions/transactions, and MongoDB change-stream resume/invalidate semantics;
- ratify ordered-log coordinate shape, typed resume-token scope, complete-after-image/key-only-delete
  row model, package-native keyed delete effects, maximum transaction policy, and pre-production
  artifact replacement.

**A1. Source-position algebra and artifact version transition**

- executable child to implement `.10x/specs/cdc-log-source-foundation.md` position/version subset;
- centralize validate/scope/equivalence/reachability/join semantics;
- update source position, checkpoint state/store, declarative, portable task, package, system-SQL,
  fixture, and conformance artifacts coherently;
- no CDC adapter ships before A1 closes.

Status: complete. Owner:
`.10x/tickets/done/2026-08-03-cdc-source-position-artifact-transition.md`, governed by
`.10x/specs/cdc-source-position-artifacts.md`.

Depends-On: A0.

**A1.5. Package-native keyed effects and delete application**

- replace the homogeneous package segment model with closed rows/keyed-changes content;
- derive complete upsert and key-only delete schemas under one exact-key reduction authority;
- separate source deletion capture from explicit `ignore`/`hard`/Boolean-`soft` destination
  application;
- replace manifest/state/staging/commit/receipt/replay identities coherently with no compatibility
  shape;
- provide merge/CDC conformance before either ingress mode adds streaming/partial availability.

Governing authority: `.10x/decisions/package-native-keyed-delete-effects.md` and
`.10x/specs/package-keyed-delete-effects.md`. Executable child intentionally pending a later turn.

Depends-On: A0. It may execute independently of A1 but must close before A2 keyed-effect package
integration and A3 destination proof.

**A2. Log-source runtime archetype and CDC batch contract**

- implement transaction-aligned safe-frontier publication, typed transient operation validation,
  and lowering into the A1.5 package-native keyed-effect contract;
- synthetic deterministic log source and model/crash conformance;
- bounded large-transaction/overshoot behavior;
- finite drain commands only.

Depends-On: A1 and A1.5.

**A3. First end-to-end CDC source/destination proof**

- choose PostgreSQL logical replication or MySQL binlog plus one `cdc_apply` destination after
  protocol research and user priority;
- prove ordered inserts/updates/deletes, replay, crash recovery, receipts, and checkpoint advance;
- collect direct-library/protocol roofline and memory/overshoot evidence.

Depends-On: A2 and the relevant connector/destination capability spec.

**A4. Additional first-party CDC sources**

- PostgreSQL logical replication, MySQL binlog, and MongoDB change streams remain separate source
  crates/tickets using A2;
- MongoDB uses typed resume-token positions, not `ForeignState`;
- each adapter gets focused protocol research, live topology fixtures, performance evidence, and
  independent red-team review.

Depends-On: A2; each child may depend on A3 if shared destination proof is required.

Current implementation placement is user-ratified: extend `cdf-source-postgres` and
`cdf-source-mongodb`; create one `cdf-source-mysql` owning both finite table reads and binlog CDC.
Do not register separate `postgres_cdc`, `mongodb_cdc`, or `mysql_cdc` source kinds.

**A5. Resident supervision**

- pause/resume/daemon/operator lifecycle over proven finite drain epochs;
- separate later program, not a prerequisite for A1-A4.

Depends-On: at least one terminal CDC adapter proof. Parked until separately activated.

### Foundation lane B — Relational sources and MySQL

**B0. Stable duplication map**

- inspect final Postgres/SQLite/ClickHouse source implementations after the active connector wave;
- classify exact duplicate mechanics versus backend-specific SQL/catalog/protocol/decode behavior;
- bound the first extraction and compiled-plan version effect of removing Postgres
  `dialect=postgres`.

**B1. `cdf-source-sql` extraction**

- activate `.10x/specs/sql-source-commons.md` after ratification;
- extract typed relational scan/catalog validation and shared conformance;
- preserve concrete driver ids and adapter-owned dialect/protocol/error/performance;
- remove the redundant Postgres dialect option intentionally.

Depends-On: B0 and stable closure of overlapping active connector source work.

**B2. MySQL finite source**

- new focused source spec/ticket based on current official protocol research;
- native binary path, consistent snapshot, complete type sheet, catalog/cursor semantics, roofline;
- validate B1 without broadening it speculatively.

Depends-On: B1 and semantic-registry stop-line C1 unless explicitly waived.

**B3. MySQL destination**

- separate destination spec/ticket with native bulk protocol, append/replace/merge truth,
  idempotency, receipts, schema/type mapping, crash/live/roofline evidence;
- no dependency on source implementation beyond narrowly shared lower protocol facts.

Depends-On: semantic-registry stop-line C1 and its own ratified behavior; sequencing against B2 is
chosen by actual shared dependency evidence.

### Foundation lane G — continuous package retention

**G1. Retention-aware committed-package collection**

- consume the already-parsed environment/trust retention rules instead of permanently protecting
  every committed package;
- keep `cdf package gc` no-write by default and add an explicit execution intent using the same
  plan;
- tombstone heavy canonical package data only after verified destination receipt and committed
  checkpoint authority, retaining the minimal manifest/hash/receipt evidence required by active
  package and promotion specifications;
- invoke the same bounded collector after successful drain-epoch checkpoint settlement so repeated
  or resident execution cannot accumulate heavy package buffers forever;
- report when collection removes the last local residual bytes available for schema promotion;
- fail closed on corrupt, ambiguous, in-flight, recovery-required, or inside-retention artifacts.

Depends-On: state-backed schema authority and promotion settlement (closed) but may execute before
A1.5/A2. Exact execution flag and retention-boundary behavior await the compact ratification
checkpoint below.

### Foundation lane C — Semantic types

**C0. Existing semantic inventory and ratification**

- enumerate every `cdf:semantic` producer/consumer and exact value;
- ratify canonical namespace/version/parameter grammar, unknown-tag policy, project-defined type
  staging, and destination mapping selector shape.

Closed by `.10x/research/2026-08-03-semantic-authority-inventory.md`,
`.10x/decisions/semantic-reference-registry-and-unknown-policy.md`, and the active semantic spec.

**C1. Built-in semantic registry and consumer migration**

- activate `.10x/specs/semantic-type-registry.md`;
- implement data-only definitions, exact resolution, direct producer migration, Arrow compatibility,
  compiled validation/redaction, and destination mapping refinement;
- migrate PII, variant, and PostgreSQL exact-value semantics without behavioral drift;
- bind reachable definitions into lock and manifest authorities.

Depends-On: C0 and D1 manifest data-model coordination for final publication; implementation may
stage internal registry work before D1.

Core/consumer owner:
`.10x/tickets/done/2026-08-03-c1-semantic-registry-core-consumer-migration.md`.

**C2. Project-defined semantic definitions**

- optional later child only if explicitly included in C0 ratification;
- same closed data schema and validation vocabulary, no executable predicates.

Depends-On: C1.

### Foundation lane D — Project compiler and manifest

**D0. Configuration authority map and grammar ratification**

- inventory current `cdf.toml`, declarative, driver schema, environment, destination-policy, lock,
  and publication ownership;
- ratify SQL/resource path authority, shared source configuration, environment overlay, relational
  envelope boundary, offline/refresh compile behavior, and manifest path/commit policy;
- remove the Postgres-special-cased destination policy only through a separately bounded generic
  policy-model ticket.

The initial explicit-id/profile decision and the later path-bound-source revision were superseded
on 2026-08-04 after the user ratified the complete query-first D3 handoff. Current authority is
`.10x/decisions/filesystem-source-resource-and-configuration-authority.md` and
`.10x/specs/project-source-resource-layout.md`. The original inventory remains historical evidence.
Manifest policy remains closed by
`.10x/research/2026-08-03-project-compiler-authority-inventory.md`,
`.10x/decisions/superseded/project-manifest-path-compile-and-query-policy.md`, and
the active manifest spec. Postgres policy cleanup owner:
`.10x/tickets/done/2026-08-03-d0-remove-postgres-merge-dedup-policy.md`.

**D1. Project compilation manifest**

- activate `.10x/specs/project-compilation-manifest.md`;
- publish one canonical hashed secret-redacted graph using existing forward-only multi-file
  publication with `cdf.lock` last;
- add focused `cdf sql` read-only manifest tables without replacing the current SQLite engine;
- no read-only command recovers pending publication.

Depends-On: D0. Must use `.10x/skills/audit-project-file-publication/SKILL.md` during execution.

Owners:

- `.10x/tickets/done/2026-08-03-d1-project-compilation-manifest-core.md`;
- `.10x/tickets/done/2026-08-03-d1-compile-cli-and-manifest-sql.md`.

**D1.5. Typed project input authority**

- D1.5a established reusable typed configured-source definitions, immutable driver type, selected-
  environment source-option overlays, source/resource schema separation, path-fenced stable SQL
  input inventory, and no coequal runtime reader;
- its then-ratified `sources/<source>/<resource>.cdf.sql` path-bound-source interpretation is
  superseded shaping authority, not a compatibility requirement or D3 grammar;
- D3 reuses the typed configuration, stable-read, path-fence, and source-schema machinery while
  changing the only current resource root to `cdf/<namespace>/<resource>.cdf.sql`, deriving
  logical resource identity/default target from the path, and resolving the explicit relation
  argument `source => '<configured_source>'` before driver-owned resource arguments;
- D3 updates manifest/lock bindings, scaffold, add/generate, examples, validation, inspection, and
  execution selection atomically while deleting both the spike-era declarative surface and the
  never-exposed path-bound-source prototype;
- no legacy reader, migration, dual authoring mode, alias, or compatibility shim is admitted.

Depends-On: D1. Governed by `.10x/specs/project-source-resource-layout.md` and
`.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`. D1.5a is complete:
`.10x/tickets/done/2026-08-04-d1-5a-project-source-resource-input-authority.md`.

**D2. Native scalar/relational IR expansion**

- replace the Boolean-limited IR with the ratified rule-based closure over every qualifying
  deterministic built-in scalar in the pinned DataFusion registry;
- record exact coerced types/nullability, canonical function/signature identities, casts,
  projection/filter graph, output schema, and lineage in durable CDF authority;
- prove DataFusion analysis-to-native lowering and vectorized execution equivalence;
- no joins/aggregations/windows or runtime DataFusion plans in v1.

Depends-On: D0 and D1; coordinates with C1 for semantic annotations. It may land after D1.5a but
does not depend on the final project-authoring cutover.

Status: complete. Closed at:
`.10x/tickets/done/2026-08-04-d2-datafusion-scalar-relational-ir.md`, governed by
`.10x/decisions/datafusion-deterministic-scalar-closure.md` and
`.10x/specs/datafusion-scalar-relational-ir.md`.

**D3. SQL project front-end**

- use `.10x/specs/sql-project-authoring.md` as the complete ratified D3 contract;
- admit a bare `SELECT` as the normal resource form and an optional no-identifier
  `RESOURCE ... AS SELECT` metadata envelope;
- derive canonical resource id/default logical target from
  `cdf/<namespace>/<resource>.cdf.sql` while keeping configured source and target independent;
- require `upstream(source => '<configured_source>', ...)`, resolve typed source/driver authority
  first, and validate recursive data-only remaining arguments through the driver resource schema;
- implement typed defaults with recorded origin, `DISPOSITION MERGE(key, ...)`, exact semantic
  annotations, and bounded/purpose-built drain execution clauses;
- use pinned DataFusion only for query-body parse/analysis and D2 lowering, then publish only native
  source/operator/contract/semantic/destination artifacts and the complete D1 manifest;
- continue rejecting joins, every set operation including `UNION ALL`, aggregation, windows,
  subqueries, multiple upstream relations, and runtime DataFusion planning;
- delete/reject `CREATE RESOURCE`, path-bound source identity, every non-`cdf/` resource root, retired
  project declarations/maps, and every compatibility/dual-reader shape.

Depends-On: D1, D1.5a, D2, and C1 for first-class semantic syntax. D3 is also the current-only
project-authoring cutover: it deletes the retired resource map/declarative reader in the same
tranche that makes the SQL resources executable.

Status: complete. Closed at:
`.10x/tickets/done/2026-08-04-d3-query-first-project-authoring-cutover.md`.

**D4. Explicit generation; templating remains parked**

- adapt `cdf add`/discovery to generate explicit SQL resources after D3;
- no general macro/Jinja engine in initial SQL authoring;
- a future render-and-pin macro ticket requires measured duplication and a separately ratified
  deterministic expansion contract.

Depends-On: D3.

### Foundation lane E — Hooks

**E0. Authority and runtime ratification**

- explicitly retain or supersede VISION D-23 for Python execution;
- select first runtime/sandbox, attach point, schema effect, row-count/watermark behavior, and
  performance/admission floor;
- refresh Wasmtime/WASI research and resolve or safely exclude recursive control values from WIT.

**E1. Batch transform hook runtime**

- activate `.10x/specs/batch-transform-hooks.md`;
- one pre-contract batch attach point, content-hashed code/environment, exact schema effect,
  side-effect-free capabilities, deterministic replay rules, and manifest identity;
- focused native/pass-through roofline and failure/cancellation/memory conformance.

Depends-On: E0, D1, C1, and the plan-declaration surface from D3 or a separately ratified
declarative declaration.

**E2. Lifecycle observer/action contract**

- separate future spec/program for package/receipt/checkpoint side effects;
- never fold into E1.

Depends-On: separate high-impact side-effect inventory and user ratification. Parked.

### Immediate correctness lane F

**F0. REST inert transform repair**

- `.10x/tickets/2026-08-03-rest-records-transform-contract-repair.md`;
- recommended rejection/removal until E1 exists;
- may execute independently after its compatibility behavior is ratified.

## Critical path and parallelism

The program has two useful independent trunks after ratification:

```text
CDC:       A0 → A1 → A2 → A3/A4
Compiler:  C0 → C1 ─┐
                     ├→ D3 → D4
             D0 → D1┤
             D0 → D2┘
Hooks:      E0 + C1 + D1/D3 → E1
MySQL:      B0 → B1 + C1 → B2/B3
Retention: G1 (independent) ───────────────┐
                                           └→ final CDC certificate
```

This graph is explanatory, not authorization for parallel agents. The user has requested that the
primary agent own implementation and use separate agents only for red-team review.

## Program-wide invariants

- Arrow remains the canonical type system; semantics annotate rather than replace it.
- DataFusion analyzes/lowers at compile time and never becomes runtime identity authority.
- no checkpoint commits mid-transaction;
- no connector-specific branch enters generic project/CLI/engine/runtime/package/state code;
- no first-party database owns the broad source kind `sql`;
- secrets remain references and never enter SQL/manifest/hook identity;
- packages, receipts, and checkpoints remain the only execution commit gate;
- read-only commands do not recover or mutate project publication;
- hooks are batch-level and vectorized; lifecycle side effects are separate;
- runtime templating is forbidden; any future macro expansion is rendered and pinned;
- performance claims require direct-library/protocol rooflines on the same host and semantics;
- validation cadence is focused during repair and one aggregate boundary certificate runs after a
  tranche stabilizes, not after every edit.

## Program acceptance criteria

The parent closes only when the user-selected active subset has terminal child evidence and all
deferred lanes are explicitly parked with owners. For a full close:

- A1/A2 position/log foundation is active, conformance-proven, and used by at least one end-to-end
  CDC adapter/destination proof;
- B1 is used by the existing relational sources and MySQL without dialect leakage;
- C1 resolves all current behavior-bearing semantic tags and binds lock/manifest/contract/
  destination authority;
- D1/D1.5/D2/D3 compile one path-derived SQL resource with one explicit configured-source binding
  through typed shared source configuration to the ordinary native execution path with a
  deterministic queryable manifest; resource id, source, and target remain independent;
- E1 executes one ratified batch hook runtime under exact schema/memory/determinism rules, or hooks
  are explicitly parked if the user retains D-23;
- F0 removes the current accepted-but-inert transform behavior;
- focused child evidence, one final affected-boundary certificate, direct rooflines, documentation,
  generated artifacts, and independent red-team reviews pass;
- every discovered risk has a durable owner or recorded no-action rationale.

## Non-goals

- implementing this parent as one tranche;
- resident CDC supervision before finite drain CDC works;
- a universal database/log/hook abstraction;
- full SQL warehouse modeling or scheduling;
- general Jinja/runtime templates;
- arbitrary Python/WASM side effects;
- silently reopening canceled monolithic tickets;
- rerunning the entire workspace suite after every repair.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/cdc-log-source-foundation.md`
- `.10x/specs/sql-source-commons.md`
- `.10x/specs/semantic-type-registry.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/specs/project-source-resource-layout.md`
- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/datafusion-scalar-relational-ir.md`
- `.10x/decisions/datafusion-deterministic-scalar-closure.md`
- `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`
- `.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`
- `.10x/tickets/done/2026-08-04-d1-5a-project-source-resource-input-authority.md`
- `.10x/specs/batch-transform-hooks.md`
- `.10x/tickets/2026-08-03-rest-records-transform-contract-repair.md`
- `.10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md`
- `.10x/knowledge/active-backlog-and-future-roadmap.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`

## Assumptions and provenance

### User-ratified direction

- CDC is a near-term priority.
- MySQL and MongoDB are priority first-class connectors.
- SQL-like explicit project definitions, semantic types, manifests, and inline hook capability are
  desired directions.
- DataFusion parses/resolves/types/coerces the SQL query body, and the pinned deterministic
  built-in scalar surface is admitted by a generic immutable/known-canonical-type/reproducible-
  execution predicate rather than a manually curated name list; CDF retains durable plan and
  runtime-envelope authority.
- explicit path-derived resources are preferred over implicit templating;
- semantic references use the exact active canonical grammar, unknown semantics fail closed, and
  project definitions use the same data-only registry after built-in migration;
- the generated manifest lives at `.cdf/manifest.json`; offline and explicit refresh compilation
  use existing crash-safe project publication, and `cdf sql` mounts rather than recompiles it;
- SQL resources live at exactly `cdf/<namespace>/<resource>.cdf.sql`; the path supplies only
  canonical resource id and default logical target, while each query explicitly names one
  configured source through `upstream(source => '<name>', ...)`;
- canonical resource id, configured source, and logical target are independent; resource namespace
  is not required to equal source name;
- `SourceRegistry` is an internal implementation catalog, never a project namespace: the relation
  resolves a typed configured source, immutable source type resolves the driver, and only then does
  that driver validate the remaining structured relation arguments;
- root wildcard resource maps, declarative `resources/<source>.toml`, explicit SQL resource ids,
  every non-`cdf/` resource root, `CREATE RESOURCE`, source sidecars, and arbitrary `${...}`
  interpolation are retired current-schema shapes with no compatibility path;
- resource namespace/stem and configured-source tokens match `[a-z][a-z0-9_]{0,127}` exactly with
  no normalization;
- every configured source is referenced by at least one valid explicit resource and no inactive
  source state exists; it need not own a same-named resource directory;
- one resource file may be a bare admitted `SELECT` or optional no-id `RESOURCE ... AS SELECT`;
  every query contains exactly one explicitly source-bound, driver-typed `upstream(...)` relation;
- omitted target/disposition/trust/execution values resolve before publication through explicit,
  typed-project, narrow-built-in, or failure precedence and the manifest records each origin;
- target defaults to the path-derived resource id, trust defaults to `EXPERIMENTAL`, and
  disposition defaults to `REPLACE` only for proven bounded replayable input;
- merge keys are intrinsic to `DISPOSITION MERGE(key, ...)`; semantic annotations use exact
  canonical registry references; execution is bounded or a complete typed drain policy;
- joins, all set operations including `UNION ALL`, aggregates, windows, subqueries, multiple
  upstream relations, and runtime DataFusion planning remain rejected;
- correctness and throughput are non-negotiable; validation must also be economical.
- implementation is owned by the primary agent; separate agents are reserved for red-team review.
- Postgres and MongoDB CDC extend their existing source crates; one MySQL source crate owns both
  ordinary reads and CDC, and no mode receives a separate source kind.
- Packages are a durable pre-commit/recovery buffer, not an indefinitely retained primary store;
  after receipt-gated checkpoint commit, heavy package bytes become eligible for the explicit
  environment/trust retention policy and safe tombstoning.
- source positions use protocol-specific PostgreSQL/MySQL committed variants and a distinct opaque
  MongoDB resume-token variant.
- CDC emits complete after-images for insert/update and destination keys for delete; MySQL
  ROW/FULL/GTID is the first proof, with the documented PostgreSQL/MongoDB prerequisites.
- Packages represent deletes as first-class exact-key effects shared by `merge` and `cdc_apply`;
  package construction selects at most one final effect per key, while event-history consumers use
  append resources.
- Source deletion capture is distinct from destination application. Captured deletes remain in the
  package, and every delete-capable merge/CDC binding explicitly selects `ignore`, `hard`, or
  Boolean-marker `soft` with no default. Soft delete preserves existing values, inserts no missing
  tombstone, and later complete upsert clears the marker.
- CDF is net-new and customer zero: artifact schemas are replaced outright with no compatibility
  readers, migrations, or transitional debt.
- MongoDB accumulates ordered change events into segments/packages and advances the terminal resume
  token only after the exact destination receipt; it does not group events by source transaction.

### Record-backed constraints

- current finite MongoDB excludes change streams/resume tokens/CDC;
- active connector child semantics and sequence remain separately owned;
- finite drain epochs, package/receipt/checkpoint gates, DataFusion identity separation, Arrow
  closure, project publication, and Python/WASM boundaries are established by referenced records;
- current CDF artifacts are pre-production/current-schema-only.

### Unratified blockers

- whether the resolved host spill budget is the maximum PostgreSQL/MySQL single-transaction byte
  authority, with a resource allowed only to lower it;
- the first `cdc_apply` destination set and exact resource syntax for explicit hard/soft/ignore
  delete application;
- whether first-use CDC requires an integrated consistent snapshot, requires an explicit native
  start position, or explicitly starts from the current source frontier;
- Python execution-substrate supersession and first hook runtime;
- whether to reorder the remaining MongoDB destination around C1.

D3 has no remaining semantic blocker. The user ratified its query-first form, independent identity
model, explicit source binding, structured values, clause grammar/order, defaults, trust,
disposition/merge keys, semantics, execution policy, relational exclusions, identity law,
diagnostics, manifest obligations, and current-only cutover in full on 2026-08-04.

## Journal

- 2026-08-07: The user activated full CDC/MySQL delivery and emphasized indefinite execution plus
  collection of already-settled package buffers. Current-source inspection confirmed A1 and the
  compiler/state foundations are complete, while keyed effects, `cdc_apply`, log-source runtime,
  MySQL, and retention-aware collection remain. Existing Postgres/MongoDB crates will be extended
  and one MySQL crate will own both finite and CDC modes. Three behavior choices still change
  source/destination/data-loss semantics and are held at a compact ratification checkpoint before
  executable adapter tickets are opened.

- 2026-08-04: The user challenged the undefined `source-driver catalog` phrase and the repeated
  source/resource identities visible in the sandbox project, then ratified the recommended
  replacement in full. `SourceRegistry` is now recorded as internal implementation authority only.
  The canonical project layout is `sources/<source>/<resource>.cdf.sql`; path supplies
  `<source>.<resource>`; root `cdf.toml` supplies one typed shared source configuration plus sparse
  selected-environment source option overlays; SQL supplies only the upstream relation and
  per-resource behavior. The prior explicit-id/profile decision was moved to `superseded/`, and the
  new focused decision/spec explicitly reject wildcard resource maps, source sidecars, arbitrary
  environment interpolation, retired declarative project resources, and compatibility machinery.
  D1.5 now owns the current-only project-model replacement before D2/D3 execute.

- 2026-08-03: The user accepted all C0/D0 recommendations. Semantic grammar/built-ins/unknown
  policy/project-definition scope, manifest path/compile/query/publication policy, and the typed SQL
  envelope/profile boundary are now active decisions/specs. Four executable children own C1 core,
  Postgres policy drift removal, D1 manifest core, and D1 CLI/query integration. Product code remains
  untouched in this required ratification/publication turn.
- 2026-08-03: The user reordered execution to complete foundation lane C (semantic types) and lane
  D (project compiler/manifest) before returning to lane A, requested bounded fast-feedback
  validation, lane-boundary review, incremental commit/push, and asynchronous CI observation.
  C0/D0 source inventories are recorded in
  `.10x/research/2026-08-03-semantic-authority-inventory.md` and
  `.10x/research/2026-08-03-project-compiler-authority-inventory.md`. They confirm six current
  behavior-bearing semantic families, one meaningless descriptive fixture, a configurable variant
  invalid state, reusable typed plan/lock identities, the existing crash-safe publication seam, a
  `cdf sql` recompilation leak, and still-live Postgres-owned merge-dedup policy drift. The only
  remaining C0/D0 blockers are the compact user-visible ratification checkpoint derived from those
  inventories.
- 2026-08-03: Program opened as a non-executable owner after live-source reconciliation of the
  supplied external review. Six focused draft specs and one immediate correctness ticket were
  created. No product code, external state, build, or test was changed/run.
- 2026-08-03: Publication portions were reconciled through
  `.10x/skills/audit-project-file-publication/SKILL.md` and
  `.10x/knowledge/project-file-publication-recovery.md`: staged targets first, durable pending
  marker, forward-only recovery, `cdf.lock` last, stable generation reads, and no read-only
  recovery.
- 2026-08-03: A0 official protocol research completed in one batch and is recorded in
  `.10x/research/2026-08-03-cdc-protocol-position-contract.md`. It resolves the position wire-shape
  recommendations, selects MySQL ROW/FULL/GTID as the shortest complete-image first proof, records
  PostgreSQL unchanged-TOAST reconstruction and MongoDB exact-post-image prerequisites, and names
  the public Mongo change-stream transaction-boundary limitation. The CDC spec now contains an
  exact ratification checkpoint rather than delegating field design to A1.
- 2026-08-03: The user ratified the recommended typed positions and row-image model, reiterated the
  no-backward-compatibility/no-tech-debt policy for this net-new customer-zero codebase, and
  clarified MongoDB as receipt-gated event-prefix segmentation rather than transaction grouping.
  A1 position/artifact work is unblocked. Only the PostgreSQL/MySQL single-large-transaction resource
  limit remains open for A2.
- 2026-08-03: The user ratified package-native keyed delete effects as a general continuous-data
  handoff beyond CDC, including future SaaS deletion feeds. Equality-by-declared-key is the only
  delete shape; merge and `cdc_apply` are the only admitted dispositions; packages retain captured
  deletes even when application is `ignore`; ordinary unordered merge duplicates remain fail-fast;
  CDC uses protocol-ordered last-change-wins; missing target deletes are idempotent no-ops; and
  explicit hard/Boolean-soft/ignore application remains destination policy. The governing decision
  and active spec are `.10x/decisions/package-native-keyed-delete-effects.md` and
  `.10x/specs/package-keyed-delete-effects.md`.
- 2026-08-04: The user ratified all remaining D1.5 path/config/relation recommendations. Project
  source and resource tokens now match `[a-z][a-z0-9_]{0,127}` exactly with no normalization;
  every configured source must own at least one valid resource and has no inactive state; and one
  query-local `upstream(...)` table function carries closed named data-only resource arguments
  validated by the already selected driver's resource schema. The core envelope spelling/order is
  also recorded. Implementation is sequenced as additive internal D1.5a input authority, then D2,
  then one D3/current-authoring cutover that deletes the old resource map/declarative reader. This
  avoids pushing either a half-runnable replacement or a compatibility/dual-reader surface.
- 2026-08-04: After D1.5a closed, the user confirmed that DataFusion remains the SQL parser,
  resolver, coercion/type analyzer, and scalar implementation authority beneath CDF's durable plan
  envelope. D2 is no longer a hand-maintained function-name allowlist: it admits every pinned
  DataFusion built-in scalar that is fully typed, `Immutable`, inside CDF's canonical Arrow closure,
  free of uncaptured ambient semantics, canonically representable, and reproducibly executable in
  vectorized batches. Known output type is required but does not admit aggregate/window/table/UDF,
  `Stable`/`Volatile`, session-dependent, or opaque expressions. The accepted decision/spec and
  completed child are `.10x/decisions/datafusion-deterministic-scalar-closure.md`,
  `.10x/specs/datafusion-scalar-relational-ir.md`, and
  `.10x/tickets/done/2026-08-04-d2-datafusion-scalar-relational-ir.md`. D2 closed after focused
  differential/performance validation, one independent adversarial review, bounded closure
  repairs, and a same-reviewer final pass; D3 may now consume the current typed IR.
- 2026-08-04: The user ratified the complete D3 handoff in full and explicitly authorized
  supersession. The prior mandatory `CREATE RESOURCE`, path-bound configured source, and
  `sources/<source>/<resource>.cdf.sql` authority is preserved under `superseded/` only. Current D3
  uses `cdf/<namespace>/<resource>.cdf.sql`, derives only resource id/default target from the
  path, requires `upstream(source => '<configured_source>', ...)`, and treats resource id, source,
  and target as independent. Bare `SELECT` is the normal form; optional ordered `RESOURCE ... AS`
  clauses own target, disposition with intrinsic merge keys, cursor, trust, semantic bindings, and
  bounded/drain execution. Structured relation values are recursive and data-only. Defaults are
  fully resolved with origin before manifest publication; `EXPERIMENTAL` is the conservative trust
  default and `REPLACE` is available only for proven bounded replayable input. The complete
  relational exclusion, identity, diagnostic, manifest, no-DataFusion-runtime, and no-compatibility
  laws are now active. D3 shaping is closed and the executable cutover ticket is
  `.10x/tickets/done/2026-08-04-d3-query-first-project-authoring-cutover.md`.
- 2026-08-04: During D3 execution, the user reopened only the resource-root noun, supplied a full
  comparison of `sources/`, `cdf/`, and `pipelines/`, and delegated the final choice. `cdf/` is now
  the sole root and an identity-excluded tool-ownership marker. It preserves the existing
  `<namespace>.<resource>` identity while avoiding both `sources/` configured-source ambiguity and
  `pipelines/` orchestration implications. The never-released `resources/` proposal is superseded
  without a compatibility reader.
- 2026-08-04: D3 closed after bounded query-first compiler/cutover commits, one independent
  red-team review, repair of all six concrete findings, a focused reviewer pass over the surviving
  publication-guard defect, affected-boundary validation, and successful GitHub Actions run
  `30968316660` for final implementation commit `f24eee00`. The terminal owner is
  `.10x/tickets/done/2026-08-04-d3-query-first-project-authoring-cutover.md`.

## Blockers

The compiler/state lanes are closed. CDC execution is blocked only on the three exact choices in
`Unratified blockers`: one-transaction byte authority, initial `cdc_apply` destination/delete
surface, and first-use CDC bootstrap. G1's collection mechanism also needs confirmation that
retention expiry tombstones canonical package bytes automatically after checkpoint settlement.
Hooks remain independently parked pending runtime ratification.

## Evidence

- Audit evidence is recorded in
  `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md` with source paths, observed
  structures, corrections, and limits.
- Protocol A0 evidence is recorded in
  `.10x/research/2026-08-03-cdc-protocol-position-contract.md` with official PostgreSQL, MySQL, and
  MongoDB sources, exact proposed artifact fields/algebra, row-image consequences, and limits.
- Current implementation/readiness evidence is recorded in
  `.10x/research/2026-08-07-cdc-mysql-continuous-readiness.md`.
- Implementation evidence pending ratification and child tickets.

## Review

Pending independent red-team review when the first executable child hands back. The user requested
no ongoing multi-agent review loop; each bounded child receives one thorough adversarial review and
only findings with concrete correctness/throughput impact become repairs.

## Retrospective

The external review was valuable as a hypothesis generator but contained six source-relevant
overstatements. Revalidating authority before ticketing prevented a one-field CDC patch, a false
“manifest service without artifact,” a second semantic type lattice, an unrestricted SQL promise,
and an unratified Python runtime from becoming executable assumptions. The durable lesson is to
turn ambitious architecture advice into focused contracts only after tracing current artifact and
execution identities end to end.
