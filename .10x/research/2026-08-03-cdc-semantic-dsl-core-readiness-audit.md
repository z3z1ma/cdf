Status: done
Created: 2026-08-03
Updated: 2026-08-03

# CDC, semantic-type, and SQL-project core-readiness audit

## Question

Does the current CDF implementation support the proposed next wave—first-class finite database
sources and destinations, CDC, MySQL and MongoDB, a SQL-like project language, first-class
semantic types, a compilation manifest, and Python/WASM hooks—without building on an unstable or
misunderstood kernel? Which parts of the supplied external review are supported by the live source,
which require correction, and which design choices remain unratified?

## Trigger and provenance

On 2026-08-03 the user supplied a separate AI's deep architectural review and explicitly asked CDF
to move forward from that Q&A with maximum fidelity in the resulting 10x records. The supplied
review is treated here as a set of hypotheses and recommendations, not as source authority. The
user's product direction is authoritative: CDC is wanted soon; MySQL and MongoDB are priorities;
the project configuration deserves an overhaul toward SQL-shaped authoring; semantic types,
compilation metadata, hooks, and explicit-over-implicit generation are desired. Exact artifact
shapes and behavioral semantics remain proposals until ratified or established by existing active
records.

## Sources and methods

The audit inspected live source and active/terminal project records at revision
`b7b3eb72db88c19fcc65ca456c8e517201e794ae` on 2026-08-03. No build or test suite was run because
this was a read-only architecture investigation and the user had explicitly requested economical,
focused validation.

### Source inspected

- workspace membership and Rust source inventory in `Cargo.toml` and `crates/**`;
- source-position types and aggregation in `crates/cdf-kernel/src/position.rs` and
  `crates/cdf-kernel/src/position_aggregation.rs`;
- checkpoint artifacts and the SQLite checkpoint-store schema in
  `crates/cdf-kernel/src/checkpoint.rs` and `crates/cdf-state-sqlite/src/sqlite.rs`;
- batch CDC metadata, drain epochs, source-stream capabilities, and source-frontier comparison in
  `crates/cdf-kernel/src/batch.rs`, `crates/cdf-runtime/src/drain_epoch.rs`, and
  `crates/cdf-runtime/src/source.rs`;
- destination CDC disposition declarations and rejections across first-party destination crates;
- Postgres, SQLite, and ClickHouse source modules, with emphasis on discovery, identifier policy,
  query rendering, cursor planning, decoding, and transport;
- project/declarative models, lockfile coverage, source compiler artifacts, and the local system-SQL
  implementation in `crates/cdf-project`, `crates/cdf-declarative`, `crates/cdf-runtime`, and
  `crates/cdf-cli/src/system_sql.rs`;
- semantic metadata helpers, contract redaction, destination mapping sheets, and Postgres exact
  value semantics;
- the native expression IR in `crates/cdf-expression` and DataFusion adapters in `cdf-engine`;
- REST `records_transform`, project runtime hooks, Python/foreign-stream boundaries, and the
  `cdf-wasm` placeholder crate.

### Records inspected

- `VISION.md`, especially §§6.5, 13.3, 25.3 and decisions D-1, D-2, D-8, D-15, D-16, D-19,
  D-23, and D-26;
- `.10x/specs/checkpoint-state-commit-gate.md`;
- `.10x/specs/stream-epochs-watermarks.md`;
- `.10x/decisions/kernel-owned-stream-epoch-policy.md`;
- `.10x/decisions/non-file-window-close-checkpoint-semantics.md`;
- `.10x/specs/source-extension-runtime-contract.md`;
- `.10x/specs/catalog-task-source-commons.md`;
- `.10x/specs/types-contracts-normalization.md`;
- `.10x/knowledge/type-policy-authority.md`;
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`;
- `.10x/decisions/compiled-fused-streaming-operator-graph.md`;
- `.10x/specs/project-cli-observability-security.md`;
- `.10x/specs/datafusion-currency-bridges.md`;
- `.10x/specs/foreign-stream-interop.md`;
- `.10x/decisions/neutral-foreign-stream-boundary.md`;
- `.10x/tickets/cancelled/2026-07-05-cdc-and-streaming-supervisor.md`;
- `.10x/tickets/cancelled/2026-07-08-wasm-wit-interface-foundation.md`;
- `.10x/knowledge/active-backlog-and-future-roadmap.md`;
- the active SQLite/ClickHouse/MongoDB connector program, source/destination specs, and child
  tickets.

## Repository facts revalidated

- The workspace has **56** member crates, not 55. The separate review's 55-crate count is stale or
  omitted one member.
- `crates/**.rs` contains **391,304** lines at the audited revision, closely matching the supplied
  391k claim.
- `.10x/tickets/done/` contains **452** completed tickets.
- No `TODO` or `FIXME` marker occurs in Rust source under `crates/`. This is a cleanliness signal,
  not evidence that all contracts are complete.
- Six executable/open connector records remain at the project root. The connector wave is not
  merely planned: SQLite source/destination and ClickHouse source have extensive implementation,
  performance, and review journals; ClickHouse destination is terminal; MongoDB source and
  destination remain open. MongoDB change streams and `cdc_apply` are explicitly excluded from the
  active finite connector program.

## Findings

### 1. Core assessment

The kernel's extension, package, receipt, checkpoint, memory, and finite drain-epoch boundaries are
strong enough to support the next wave. The most important positive finding is that the required
work can extend existing authorities rather than create a parallel runtime:

- `CompiledSourcePlan` already records source-neutral driver identity, descriptor, capabilities,
  exact Arrow schema, type-policy allowances, schema evidence, redacted options plus hash, and a
  physical source plan plus hash (`cdf-runtime/src/source.rs:1489-1506`).
- compiler-owned source identities separately bind discovery, compiled plan, physical plan, and
  source semantics (`cdf-runtime/src/source.rs:1508-1556`);
- drain execution already closes finite epochs at source-declared safe frontiers and advances
  committed authority only after package, receipt, and checkpoint settlement;
- Arrow schemas, packages, receipts, and checkpoints already carry the identity and evidence
  surfaces required by a compiler front-end and future log sources.

The core is therefore not a rewrite candidate. Its material gaps are localized but foundational:
log-position algebra, a reusable log-source lifecycle, repeated relational-source mechanics,
semantic-definition authority, and a complete project compilation artifact.

### 2. CDC position authority is incomplete in more ways than one field

The supplied review correctly identifies a CDC stop-line, but understates it.

Current `SourcePosition::Log` stores `LogPosition { version, log, offset: i64,
sequence: Option<String> }` (`cdf-kernel/src/position.rs:38-46, 247-253`). It does not identify a
protocol, encode whether a coordinate is a committed transaction boundary, bind a transaction, or
state what `sequence` means. `SourcePosition::ForeignState` provides protocol-labelled opaque
bytes plus a content hash, but has only equality semantics and is intentionally a foreign adapter
escape hatch. A first-party MongoDB resume token should not be demoted to that shape.

The missing work is a position **algebra**, not just serialization:

1. Structural validation must prove protocol/scope identity and committed-boundary legality.
2. Equivalence must distinguish the same restart authority from merely equal-looking offsets.
3. Monotone reachability must be protocol-correct and shared by authored source-frontier
   termination and recovery validation.
4. Aggregation/join must advance multiple emitted log positions plus an existing checkpoint without
   permitting regression or cross-stream mixing.
5. Batch slicing and transaction alignment must establish which position remains exact when one
   source transaction spans several Arrow batches or package segments.

Today the gaps are observable:

- generic position aggregation handles file manifests, table snapshots, and cursor maxima, then
  requires every other emitted position to be exactly equal
  (`cdf-kernel/src/position_aggregation.rs:73-123`). A sequence of advancing log positions therefore
  fails as “divergent segment source positions.”
- drain source-frontier comparison treats equal log names plus `observed.offset >= target.offset`
  as sufficient and treats `sequence` as optional equality
  (`cdf-runtime/src/drain_epoch.rs:736-775`). It has no transaction-boundary rule.
- `SourceStreamCapabilities` can advertise named log frontiers, but capability declaration does not
  supply the missing comparison or aggregation semantics (`cdf-runtime/src/source.rs:660-774`).

This is the first CDC implementation stop-line. A source must never emit a receipt-gated
checkpoint that resumes inside a transaction, even if package rotation, checkpoint cadence, timer,
row, or byte thresholds fire while that transaction is being decoded.

### 3. The artifact compatibility surface is wider than `state_version`

Changing source positions before the first CDC source is still the correct timing, but the change
is not a single migration:

- `SOURCE_POSITION_VERSION` is 1 and every position variant validates against it;
- `CHECKPOINT_STATE_VERSION` is independently 1 and positions appear in input, output,
  continuation, state segments, and late-data carryover;
- `CHECKPOINT_STORE_SCHEMA_VERSION` is independently 1 and the SQLite schema constrains and
  duplicates typed position JSON inside both columns and `delta_json`;
- declarative position declarations, portable worker/task position encodings, package state
  preimages, fixtures, and any canonical hashes that contain positions must change coherently.

Active state authority is deliberately current-schema-only while CDF remains pre-production. The
recommended implementation is therefore a **coherent current-schema replacement** with explicit
version bumps and fail-closed rejection of old artifacts, not speculative compatibility readers or
a broad migration framework. The complete touched-artifact inventory must be established in the
execution ticket before code changes.

### 4. CDC vocabulary exists, but no first-party CDC flow exists

The separate review says `_cdf_op`/`cdc_apply` is untested by a source. The live source shows a
slightly more developed but still incomplete seam:

- `BatchHeader` already has `cdc: Option<CdcMetadata>`;
- `CdcMetadata` names an operation field and carries a `SourcePosition`
  (`cdf-kernel/src/batch.rs:124-181, 417-421`);
- engine residual handling treats the operation field as control-critical so it cannot be silently
  captured or discarded;
- `WriteDisposition::CdcApply` exists in kernel, project, declarative, CLI guarantee reporting, and
  conformance vocabulary.

However:

- no first-party source constructs `CdcMetadata`;
- no code defines, validates, or emits the canonical `_cdf_op` value vocabulary;
- `CdcMetadata.position` is not reconciled against `BatchHeader.source_position` in a complete CDC
  protocol;
- DuckDB, Postgres, SQLite, ClickHouse, and Parquet destinations reject or reserve
  `cdc_apply`; no first-party destination applies insert/update/delete operations;
- operation-image, key, schema-evolution, transaction-order, delete, retry, and receipt semantics
  remain unspecified.

The existing seam should be completed, not duplicated. It must not be mistaken for a working CDC
archetype.

### 5. There is no log-source archetype

CDF has one implemented file source, one explicitly extracted catalog-task commons used by
Iceberg/Glue, and several query-shaped database source implementations. Calling these “three
extracted source archetypes” is inaccurate: query-shaped sources are a repeated family, not a
common implementation authority. No crate or runtime module owns a log-source lifecycle.

A reusable log-source archetype should own only source-neutral mechanics:

- checkpoint-to-protocol start selection;
- ordered transaction admission and bounded batch emission;
- transaction-aligned safe-frontier publication;
- canonical `_cdf_op` validation and CDC batch metadata;
- pause/cancel/retry rules that cannot reorder or duplicate a transaction invisibly;
- epoch overshoot accounting when a transaction crosses a row/byte/time threshold;
- exact completion/continuation handoff and checkpoint binding;
- synthetic conformance for crash and retry at each boundary.

Postgres logical replication, MySQL binlog, and MongoDB change streams must retain their own wire
protocols, slot/GTID/resume-token rules, topology checks, schema-event handling, and error
classification. A universal `LogSource` object model that erases those facts would be a leaky
abstraction.

### 6. Relational source duplication is now real and should be extracted before MySQL

No `cdf-source-sql` commons exists. The current production module inventory is approximately:

- Postgres source: 4,628 lines across binary COPY, catalog, driver, error, and source modules;
- SQLite source: 1,814 lines in its current split;
- ClickHouse source: 5,547 lines including source-owned tests and type machinery.

Line counts do not prove duplication, but source inspection does: all three independently model
table/catalog observations, identifiers, projections, filters, cursor/tie-break planning,
partition metadata, query rendering, and plan validation. They also have crucial differences in
snapshot consistency, parameter/literal encoding, quoting, catalogs, types, transports, and decode
paths.

The correct seam is not a universal SQL dialect or a source driver that multiplexes backends.
`cdf-source-sql` should own a neutral relational scan vocabulary, structural validation, stable
projection/cursor/tie-break planning, catalog-observation normalization, and reusable tests.
Adapters must continue to own server SQL rendering, quoting, catalog queries, physical type
sheets, binary decoding, consistency/snapshot protocols, transport, retries, and error provenance.

Postgres currently exposes a source option schema field `dialect` whose only legal and default
value is `postgres` (`cdf-source-postgres/src/driver.rs:37, 135-149`). The driver id already proves
that identity. The same constant is copied into partition metadata and validated at execution.
This is redundant configuration, not a useful abstraction seam. It should be removed as part of
the commons extraction or a focused compatibility change; no first-party driver should own the
broad “SQL” category or ask users to restate its concrete dialect.

With Postgres, SQLite, and ClickHouse as three real implementations, extraction is no longer a
single-implementation speculative interface. It should precede MySQL so MySQL validates the seam
instead of defining it while copying Postgres.

### 7. Semantic metadata is behavior-bearing but lacks definition authority

`cdf:semantic` is not merely five files of unused plumbing:

- kernel helpers store and retrieve a free-form string on Arrow `Field` metadata;
- declarative field schemas accept an arbitrary semantic string;
- contract redaction interprets `pii:*` as PII;
- normalization/variant code uses exact semantic tags;
- Postgres source emits exact-value semantic tags for JSON, JSONB, and NUMERIC text encodings;
- Postgres destination uses those tags plus `cdf:physical_type` to reconstruct exact native values
  and reject semantic reinterpretation.

The missing authority is a registry that defines what a tag means, which Arrow physical types it
may annotate, how it validates, what equivalence/cast rules are permitted, how it affects
redaction/display, and which destination mappings may specialize it. Destination sheets currently
select only by Arrow type (`TypeMapping { arrow_type, destination_type, fidelity }`), so semantic
mapping is adapter-local.

An active invariant constrains the design: Arrow is CDF's closed canonical type system. A semantic
registry must be versioned annotation/profile authority **over Arrow physical types**, not a second
logical type lattice or custom execution-kernel type system. SQL syntax such as `::money('USD')`
may eventually author a semantic annotation, but it must lower to an ordinary Arrow type plus a
locked semantic definition and validation program.

The registry should unify existing consumers only after an exact direct replacement map for current
tags is specified. Producers, consumers, fixtures, and artifact versions must change coherently;
no alias or compatibility resolution layer is warranted for this net-new customer-zero system.

### 8. The SQL project language is a new compiler front-end, not a config-file rewrite

The supplied review is directionally correct that CDF is pre-adapted for a SQL-like front-end:

- the source compiler already produces a typed, hashed `CompiledSourcePlan`;
- the engine already records a compiled operator graph and keeps DataFusion on the analysis side
  of the identity boundary;
- project resources already resolve through named source drivers and option schemas;
- `cdf.lock` already pins dependency, resource, schema, contract, and destination facts.

It overstates the completeness of the target IR. `cdf-expression` currently binds only Boolean
columns/literals, `NOT`/`AND`/`OR`, null checks, and column-to-literal comparisons; derived columns
are Boolean. It does not represent general scalar expressions, casts, aliases, arithmetic,
struct/list construction, joins, aggregations, windows, or full SQL relational plans. The current
DataFusion adapter is a resource query bridge, not a SQL project compiler.

Therefore the first SQL authoring slice should be intentionally narrow: one explicit resource,
one named source relation, projection/filter/alias/cast and a bounded set of deterministic scalar
expressions, lowered at compile time into native CDF IR. Joins, aggregations, windows, unions,
runtime SQL execution, and cross-resource post-load modeling remain outside the initial contract.
This matches the active boundary that CDF performs bounded in-flight extraction transforms rather
than becoming a warehouse modeling scheduler.

Connections, secrets, egress, retry/rate, state, package roots, and destination policy should
remain typed named profiles/policies rather than opaque SQL strings. The SQL resource definition
may reference them but must not embed credentials. Exact file grammar—custom `CREATE RESOURCE`, a
standard SQL statement plus companion metadata, or a CDF wrapper that delegates only its query
body to DataFusion—remains a user-visible choice requiring ratification. DataFusion compatibility
must be proven against the chosen grammar rather than assumed.

Both the existing declarative front-end and the SQL front-end should lower to the same compiler
artifacts during migration. Runtime behavior must never branch on which authoring syntax produced
the plan.

### 9. A queryable manifest requires a stable artifact first

The external review's “serve, do not write” inversion is a false dichotomy. A reproducible query
surface needs an immutable thing to query.

Current `cdf.lock` is not a full compile manifest. It stores project/dependency pins, resource
descriptors/capabilities, execution extent and stream policy, schema references, contract
snapshots, and destination sheets. It does not store complete compiled source plans, redacted
option/physical-plan hashes, output schemas, compiled operator graphs, semantic registry snapshots,
hook identities, template expansions, or complete lineage.

Current `cdf sql` is not DataFusion-backed. It creates an in-memory SQLite database and mounts
checkpoint/package/segment/receipt artifacts into six read-only tables
(`cdf-cli/src/system_sql.rs:16-23, 68-163`). This is already a useful query service and should be
extended rather than replaced merely to satisfy the DSL narrative.

The correct architecture is:

1. compilation produces a canonical, versioned, content-hashed, secret-redacted project manifest;
2. atomic publication uses the existing project multi-file publication/crash-recovery authority;
3. `cdf sql` mounts stable relational views over that artifact;
4. future DataFusion/ADBC catalog exposure may serve the same artifact when a concrete consumer
   justifies it.

The lockfile remains the pin/expectation authority; the manifest is the complete compilation
result. Conflating them would make routine compile output mutate dependency-lock semantics.

### 10. Hooks need two separate contracts and are not automatically deterministic

There is no user-facing hook system today.

- REST accepts and propagates `records_transform`, including the VISION example
  `python://...#flatten_reactions`, but no runtime code executes it. This is an inert accepted
  option and a real correctness smell: configuration can claim a transform that never occurs.
- `RuntimeStageHook` and `ReceiptVerifiedHook` are borrowed in-process callbacks used by project
  orchestration/replay tests and failure injection. They are not serializable, declarative, hashed,
  sandboxed, or suitable as a product hook API.
- the Python/foreign-stream path is a producer/interchange boundary, not a mid-pipeline hook host;
- `cdf-wasm` exists only as a one-line placeholder crate. The canceled WIT foundation found that
  recursive `ScopeKey::Composite` and `SourcePosition::Composite` require a separately ratified
  acyclic wire projection before a stable WIT interface can exist.

Two capabilities must not be collapsed:

1. **Data transform hooks** consume and produce accounted Arrow batches at a declared compile-time
   attach point, declare input/output schema and watermark behavior, are content-addressed, and are
   side-effect-free/capability-limited.
2. **Lifecycle observers/actions** react to package/receipt/checkpoint events and require explicit
   retry, idempotency, failure, authorization, secret, and side-effect semantics. They do not
   mutate identity-bearing data.

Row-level Python hooks should not be offered; they would defeat vectorization and make memory/error
accounting ambiguous. Batch-level hooks preserve the high-throughput boundary.

Recording post-transform batches means package replay need not re-execute a transform hook. It does
**not** make nondeterministic hooks free: original package identity, retries before package
finalization, watermark claims, schema evidence, quarantine behavior, cacheability, and external
side effects still require deterministic and explicit rules.

There is also an authority conflict: VISION D-23 and active records say Python is
authoring/interchange only, never the execution substrate. A Python runtime hook would supersede
that rule. The user must explicitly ratify the narrower replacement and its trust/sandbox model
before an executable Python-hook ticket can exist.

### 11. Templating should remain deferred and compile-only

The user's preference for explicitness aligns with current architecture:

- project wildcard mappings and source discovery already generate sets of resources from catalog
  authority;
- `cdf add` can materialize explicit source/resource files;
- no runtime Jinja engine or implicit resource loop exists.

Initial SQL authoring should therefore have no general templating language. If measured duplication
later justifies macros, expansion must occur before semantic compilation, produce canonical
rendered resources, be content-hashed into the manifest, and be inspectable/diffable. Runtime string
interpolation, secret expansion, filesystem/network access, and environment-dependent generation
must remain prohibited. “Render and pin” is retained as a future rule, not an initial feature.

### 12. Sequencing must respect the already-active connector wave

The supplied review recommends semantic types before more destinations, but the finite connector
program is already ratified and partly implemented. Reordering it silently would conflict with
active ticket authority and discard sunk review/performance evidence.

The reconciled sequence is:

1. finish the current finite SQLite/ClickHouse/MongoDB connector program under its existing specs;
2. change the source-position/checkpoint artifact family and establish position algebra before any
   CDC source ships;
3. extract `cdf-source-sql` before MySQL finite extraction or Postgres/MySQL log work copies the
   current database source family;
4. establish the semantic registry before the **next connector program after the active wave** adds
   more mapping sheets, unless the user explicitly reorders the remaining MongoDB destination;
5. formalize the project compilation manifest;
6. build the SQL authoring front-end over the stable manifest/IR contract;
7. build transform hooks only after their plan/manifest declaration and runtime-authority conflict
   are resolved.

CDC source work can proceed after steps 2 and the reusable log-source archetype; it does not need to
wait for the complete SQL-project/hook lane. Resident supervision is a later operational layer over
finite drain epochs, not a prerequisite for proving bounded CDC drain commands.

No duration estimate from the supplied review is retained. Each item crosses artifact, compiler,
conformance, and performance boundaries; estimates require bounded tickets and measured dependency
graphs.

## Smells and architectural risks requiring durable ownership

| Finding | Severity | Why it matters | Owner created by this audit |
|---|---|---|---|
| REST `records_transform` is accepted but inert | significant | Configuration claims a transform that never runs | `.10x/tickets/2026-08-03-rest-records-transform-contract-repair.md` |
| Log positions have no committed-transaction authority or usable aggregation | critical before CDC | A checkpoint could be incomparable or land mid-transaction | `.10x/specs/cdc-log-source-foundation.md` and the new parent program |
| `CdcMetadata`/`cdc_apply` vocabulary has no complete source/destination protocol | critical before CDC | Existence of enums can be mistaken for delivery support | `.10x/specs/cdc-log-source-foundation.md` |
| Query-shaped sources duplicate relational planning mechanics | significant before MySQL | MySQL would either copy behavior or force a rushed universal dialect | `.10x/specs/sql-source-commons.md` |
| Postgres exposes a constant-only `dialect=postgres` option | minor alone, significant as a boundary signal | Users restate driver identity and Postgres appears to own generic SQL | `.10x/specs/sql-source-commons.md` |
| Semantic tags are free-form while affecting redaction/exact-value behavior | significant | Meaning can drift without version/lock authority | `.10x/specs/semantic-type-registry.md` |
| Lockfile is incomplete as compile output | significant before DSL | SQL files cannot produce a complete inspectable/reproducible project graph | `.10x/specs/project-compilation-manifest.md` |
| SQL target IR is narrower than the proposed language | significant | An unrestricted parser would lower into DataFusion-only runtime semantics or fake support | `.10x/specs/sql-project-authoring.md` |
| Python hook direction conflicts with D-23; WASM wire shape unresolved | critical before hooks | Execution/sandbox/state semantics cannot be inferred from an attach-point enum | `.10x/specs/batch-transform-hooks.md` |

## Conclusions

1. The core is structurally sound. No drastic kernel rewrite or broad crate consolidation is
   justified.
2. CDC must begin with a versioned position algebra and committed-transaction safe-frontier
   contract, followed by a source-neutral log lifecycle. Adding only `tx_boundary` to the current
   struct would leave aggregation, resume, and protocol scope unsound.
3. MongoDB resume tokens deserve a typed first-party position variant with opaque content, exact
   scope, and equality/resume semantics. They must not be hidden in generic `ForeignState`.
4. `cdf-source-sql` is now justified by three real implementations. It must share relational scan
   mechanics without owning any server dialect or wire protocol.
5. First-class semantics must remain annotations over Arrow and become versioned compile/lock
   authority consumed by contracts, destinations, redaction, and artifacts.
6. The SQL DSL should be a compiler front-end into expanded native CDF IR, with DataFusion limited
   to compile-time analysis/lowering and no DataFusion identity types crossing the boundary.
7. CDF should both publish and serve a project manifest: stable artifact first, queryable tables
   second.
8. Hooks are a later plan-declared batch capability. Data transforms and lifecycle side effects
   require separate contracts. Python execution requires explicit supersession of the current
   execution-substrate rule; WASM requires the previously blocked wire projection.
9. General templating is not justified for the initial SQL language. Discovery and explicit
   generation remain preferred; any future macro system must render and pin before compilation.

## Ratification still required before execution

The draft specs created by this audit make concrete recommendations, but code execution must not
invent these user-visible choices:

1. whether the current pre-production source-position/checkpoint artifact family may be replaced
   wholesale (recommended) rather than supporting legacy readers;
2. the initial CDC row-image and `_cdf_op` contract, especially update/delete payloads and maximum
   transaction handling;
3. the exact SQL authoring split between SQL files and typed project/profile metadata;
4. the semantic-definition namespace/version grammar and whether project-defined types are in the
   first registry slice;
5. whether Python is permitted as a sandboxed execution-time transform host, superseding D-23;
6. whether to finish the existing MongoDB destination before the semantic registry (recommended to
   preserve the active connector program) or explicitly reorder that child.

## Limits

- This was a source-and-record audit. No external PostgreSQL, MySQL, MongoDB, or ClickHouse protocol
  documentation was revalidated and no server was contacted. Protocol-specific CDC specifications
  require fresh official-source research.
- No runtime was benchmarked. Performance recommendations preserve CDF's batch, memory, and direct-
  library roofline doctrine but do not establish new throughput numbers.
- The audit did not claim every line in 391k Rust lines was read. It traced the concrete authority
  and execution chains relevant to the proposed architecture and verified the external review's
  repository-wide numeric assertions separately.
- Draft specs are shaping artifacts, not authorization to change code or external state.
