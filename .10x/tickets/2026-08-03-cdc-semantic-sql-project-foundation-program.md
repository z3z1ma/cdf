Status: open
Created: 2026-08-03
Updated: 2026-08-04

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
`.10x/tickets/2026-08-03-c1-semantic-registry-core-consumer-migration.md`.

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

The initial explicit-id/profile decision was superseded on 2026-08-04 after the user rejected the
spike-era taxonomy. Current authority is
`.10x/decisions/filesystem-source-resource-and-configuration-authority.md` and
`.10x/specs/project-source-resource-layout.md`. The original inventory remains historical evidence.
Manifest policy remains closed by
`.10x/research/2026-08-03-project-compiler-authority-inventory.md`,
`.10x/decisions/project-manifest-path-compile-and-query-policy.md`, and
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

**D1.5. Project source/resource model replacement**

- replace root wildcard resource maps and declarative resource file locators with deterministic
  `sources/<source>/<resource>.cdf.sql` enumeration;
- derive canonical id `<source>.<resource>` from the validated path and prohibit SQL id/source
  repetition;
- add typed `[sources.<source>]` base configurations plus selected-environment source option
  overlays, with immutable source type and schema-validated source/resource option separation;
- D1.5a establishes the typed filesystem/configuration compiler input inventory without exposing a
  second project reader or changing runtime behavior;
- the D3 current-authoring cutover consumes that inventory and updates manifest/lock bindings,
  scaffold, add/generate, examples, validation, and inspection atomically while deleting the old
  authoring surface;
- reject the spike-era shape with no legacy reader, migration, dual authoring mode, or compatibility
  shim.

Depends-On: D1. Governed by `.10x/specs/project-source-resource-layout.md` and
`.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`. D1.5a is complete:
`.10x/tickets/done/2026-08-04-d1-5a-project-source-resource-input-authority.md`.

**D2. Native scalar/relational IR expansion**

- extend only the expression/cast/projection subset ratified for SQL v1;
- prove DataFusion analysis-to-native lowering and vectorized execution equivalence;
- no joins/aggregations/windows or runtime DataFusion plans in v1.

Depends-On: D0 and D1; coordinates with C1 for semantic annotations. It may land after D1.5a but
does not depend on the final project-authoring cutover.

**D3. SQL project front-end**

- activate `.10x/specs/sql-project-authoring.md`;
- parse exact CDF envelope/metadata plus DataFusion-compatible query body;
- lower to native source/operator/contract/semantic/destination artifacts;
- replace the retired project declarative front-end and publish the D1 manifest.

Depends-On: D1, D1.5a, D2, and C1 for first-class semantic syntax. D3 is also the current-only
project-authoring cutover: it deletes the retired resource map/declarative reader in the same
tranche that makes the SQL resources executable.

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
- D1/D1.5/D2/D3 compile one path-derived SQL resource through one typed shared source configuration
  to the ordinary native execution path with a deterministic queryable manifest;
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
- explicit path-derived resources are preferred over implicit templating;
- semantic references use the exact active canonical grammar, unknown semantics fail closed, and
  project definitions use the same data-only registry after built-in migration;
- the generated manifest lives at `.cdf/manifest.json`; offline and explicit refresh compilation
  use existing crash-safe project publication, and `cdf sql` mounts rather than recompiles it;
- SQL resources live at exactly `sources/<source>/<resource>.cdf.sql`; the path is the sole
  canonical source/resource identity, SQL repeats neither, shared typed source configuration lives
  once in `cdf.toml`, and environment overlays may change admitted source option values but not
  source name/type;
- `SourceRegistry` is an internal implementation catalog, never a project namespace: path resolves
  configured source, source type resolves driver, and the resource relation then resolves through
  that driver;
- root wildcard resource maps, declarative `resources/<source>.toml`, explicit SQL resource ids,
  source sidecars, and arbitrary `${...}` interpolation are retired current-schema shapes with no
  compatibility path;
- source/resource path tokens match `[a-z][a-z0-9_]{0,127}` exactly with no normalization;
- every configured source owns at least one valid explicit resource and no inactive source state
  exists;
- the query contains exactly one path-bound, driver-typed `upstream(...)` base relation whose
  named data-only arguments validate through the selected driver's closed resource schema; there
  is no separate relation clause or compiler-provided input/source alias;
- correctness and throughput are non-negotiable; validation must also be economical.
- implementation is owned by the primary agent; separate agents are reserved for red-team review.
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

- PostgreSQL/MySQL maximum single-transaction resource behavior;
- exact D2 native scalar/cast allowlist and IR version;
- exact D3 semantic-annotation tokens, which must reuse the canonical semantic reference grammar;
- exact D3 data-only structured-value grammar for complex `upstream(...)` resource arguments;
- detailed D3 focused-policy value grammar, including drain execution, without changing the
  ratified core envelope order;
- Python execution-substrate supersession and first hook runtime;
- whether to reorder the remaining MongoDB destination around C1.

## Journal

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

## Blockers

The exact unratified choices listed above still govern D2/D3/E1. D1.5a is executable; D2 requires
its scalar/cast checkpoint; D3 then performs the single current-authoring cutover after D1.5a/D2.
CDC A1 is closed; the large-transaction policy must be settled before A2 closes. F0 is closed at
`3487de68`.

## Evidence

- Audit evidence is recorded in
  `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md` with source paths, observed
  structures, corrections, and limits.
- Protocol A0 evidence is recorded in
  `.10x/research/2026-08-03-cdc-protocol-position-contract.md` with official PostgreSQL, MySQL, and
  MongoDB sources, exact proposed artifact fields/algebra, row-image consequences, and limits.
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
