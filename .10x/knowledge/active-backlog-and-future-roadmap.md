Status: active
Created: 2026-07-25
Updated: 2026-08-03

# Active backlog and future roadmap

## Purpose

Keep CDF's executable backlog bounded, sequenced, and distinguishable from the long-horizon
product vision. A ticket is active only when its outcome is currently prioritized, its
dependencies are available, and its acceptance criteria are concrete enough to execute without
inventing product semantics. Future ambitions remain authoritative in `VISION.md`, decisions,
specifications, research, and this roadmap; they do not remain open tickets merely as reminders.

Cancelled tickets preserve investigation history and may be mined when a future program is
deliberately activated. Cancellation does not reject the capability.

## Current execution

The stabilization program is terminal at
`.10x/tickets/done/2026-07-25-stabilization-steady-state-program.md`. Its source lifecycle,
statistics pruning, daily-driver CLI and hosted release, constant-memory and interop proof, and
P1/P3 aggregate lanes are all closed.

The successor CPU-saturation program is terminal at
`.10x/tickets/done/2026-07-26-stage-local-cpu-saturation.md`. Stage-local destination pressure
no longer caps run-wide jobs, Parquet object groups encode independently, and the exact prior
one-TiB acceptance improved from 44:54.863 to 8:19.07 at 2.222 GB/s with constant memory and
verified identity.

The pre-wave architecture hardening successor is terminal at
`.10x/tickets/done/2026-07-26-pre-wave-architecture-hardening-program.md`. Its product-boundary
enforcement, catalog-task source commons, destination common services, typed CLI/error authority,
holistic CLI experience, crash-publication follow-up, and final extension-authoring proof are
closed. Its closure returned the executable graph to zero before connector work was activated.

The finite connector-mode readiness successor is terminal at
`.10x/tickets/done/2026-07-31-connector-mode-readiness-program.md`. Its independently observable
deep certificate, model-based deterministic-package and settlement falsifiers, and connector
admission command with explicit core-change budget are closed. Final Slow Quality passed 21/21
jobs, and `.10x/evidence/2026-07-31-connector-mode-readiness-closure.md` records the aggregate
limits.

The user-ratified finite SQLite/ClickHouse/MongoDB connector wave is active at
`.10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md`. SQLite and ClickHouse
implementation/review evidence is substantially developed; ClickHouse destination is terminal;
MongoDB source/destination remain explicit finite-mode children. MongoDB change streams and
`cdc_apply` remain outside that program.

The CDC/semantic/SQL-project successor is active in **shaping only** at
`.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`. Its live-source audit and
six draft contracts establish the dependency graph, but no architecture child is executable until
the named artifact, CDC row-image, SQL grammar/profile, semantic-registry, and hook-runtime choices
are ratified. The accepted-but-inert REST `records_transform` surface has a focused blocked
correctness owner at `.10x/tickets/2026-08-03-rest-records-transform-contract-repair.md`.

Parked capabilities below remain demand-activated rather than silently reopened; the next feature
wave still requires the activation rule at the end of this record.

A cold-start engineer begins with `.10x/knowledge/cold-start-engineering-handoff.md`. It indexes
the terminal current cut, operational runbooks, present performance floors, known
generated-evidence lag, and the architectural invariants that are otherwise distributed across
the project memory.

## Parked future programs

### Native format breadth

The enterprise format catalog remains the product direction, but Avro, ORC, XML, spreadsheets,
MessagePack/CBOR, and archive containers are demand-activated capabilities rather than P3 closure
requirements. Governing records:

- `.10x/decisions/native-enterprise-format-catalog-v1.md`
- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
- cancelled P3 B6-B13 tickets under `.10x/tickets/cancelled/`

Avro's bounded dependency-owned expansion problem and ORC's missing resource-authority surface
must be revalidated against current upstream releases before either codec is reactivated.

### Provider-query sources

Athena is a credible managed-query source, complementary to direct Iceberg rather than a
replacement for it. The retained design is:

```text
governed query
→ provider-side execution
→ immutable result manifest
→ ordinary external Parquet tasks
→ CDF package/receipt/checkpoint pipeline
```

Direct Iceberg remains the expected path for identity scans. Athena should be reactivated for
selective filters, joins, aggregations, federated access, or governed query surfaces where
provider execution materially reduces transferred data. The first future ticket MUST compare a
direct Iceberg identity projection, a selective query, and a join/aggregation on one controlled
host before product implementation. Authority:

- `.10x/research/2026-07-19-athena-unload-source-protocol.md`
- `.10x/tickets/cancelled/2026-07-19-athena-a1-unload-source-spike.md`

The broader future family includes Trino spooling and provider-native query result planes. Each
provider chooses direct binary read, managed spooling, or explicit export from measured evidence;
CDF does not impose one export-first strategy.

### Continuous execution and CDC

CDC foundation shaping is active. Live-source audit established that current `LogPosition` lacks
transaction-boundary authority and advancing log aggregation, while the thin `CdcMetadata`/
`cdc_apply` vocabulary has no first-party end-to-end implementation. The active draft requires a
versioned position algebra, typed MongoDB resume-token position, transaction-aligned safe
frontiers, canonical operation/key behavior, and one shared log-source lifecycle before any CDC
adapter ships:

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/cdc-log-source-foundation.md`
- `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

Finite CDC drain commands MUST reuse the existing drain-epoch, watermark, rolling-spool,
checkpoint, receipt, and commit-gate semantics rather than create a parallel runtime. Resident
streaming supervision, daemon scheduling, pause/resume operations, and long-lived ownership remain
parked until at least one finite CDC source/destination proof closes. Historical monolithic owner:

- `.10x/tickets/cancelled/2026-07-05-cdc-and-streaming-supervisor.md`

### Relational source commons and MySQL

Postgres, SQLite, and ClickHouse now provide three real query-shaped source implementations, but no
source-side SQL commons exists. Before MySQL, extract source-neutral relational catalog/scan/
projection/filter/cursor validation into `cdf-source-sql` while concrete adapters retain every
dialect, catalog query, consistency protocol, client, decoder, and error. Remove the redundant
Postgres `dialect=postgres` option as a bounded versioned cleanup; no database owns source kind
`sql`.

- `.10x/specs/sql-source-commons.md`
- `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

Execution waits for stable closure of the overlapping active connector source work and fresh MySQL
protocol/snapshot/type research.

### Semantic registry and SQL project compiler

First-class semantic types and SQL-like project authoring are active shaping directions. Semantic
types remain versioned annotations over Arrow, not a second type lattice. The project compiler must
publish a canonical secret-redacted manifest before `cdf sql` exposes it; `cdf.lock` remains pin
authority. DataFusion may parse/analyze/lower SQL at compile time, but native CDF IR remains the
serialized and runtime identity. The initial language is one explicit source resource with a
bounded vectorized projection/filter/cast/scalar subset; joins, aggregation, scheduling, and
runtime templates remain excluded.

- `.10x/specs/semantic-type-registry.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/specs/sql-project-authoring.md`

SQL grammar/profile split, semantic namespace/project scope, and manifest path/publication behavior
must be ratified before implementation. Existing declarative resources remain a coequal front-end
during migration.

### Plan-declared hooks

Batch transform hooks are a desired but blocked future compiler/runtime capability. The retained
shape is Arrow-batch-level, content- and environment-hashed, schema-effect-declared,
side-effect-free, memory-accounted, and manifest-bound. Lifecycle notifications/actions are a
separate high-impact contract. Python execution currently conflicts with D-23; WASM still requires
a safe WIT/control projection or a proven narrower interface.

- `.10x/specs/batch-transform-hooks.md`
- `.10x/tickets/cancelled/2026-07-08-wasm-wit-interface-foundation.md`
- `.10x/tickets/2026-08-03-rest-records-transform-contract-repair.md`

General Jinja/runtime templating remains parked. Discovery and explicit generated SQL resources are
preferred; any future macro system must render and pin deterministic outputs before compilation.

### Extreme metadata cardinality

P3's enforced constant-memory envelope is exact through 1 TiB, 1,024 file positions, and 5,120
canonical segments. Current artifact contracts still retain one semantic file-position,
state-segment, and destination-acknowledgement record per corresponding identity. That measured
envelope is not a claim of constant metadata at million-file/package scale or over an unbounded
checkpoint horizon.

Before a future million-file, petabyte-duration, or continuously rotating package program claims
that larger envelope, it must externalize those semantic arrays behind content-addressed,
stream-verified position/state/receipt authorities while preserving the existing package,
checkpoint, and commit identities. It must not reintroduce eager compatibility readers or make a
resident cache the execution authority. Historical analysis and the current measured boundary:

- `.10x/tickets/done/2026-07-11-p3-f2-materialization-closure-audit.md`
- `.10x/tickets/done/2026-07-11-p3-f4-one-tb-memory-closeout.md`

### Distributed execution and remote state

Remote workers, fenced shared state, object-store packages, and Spark/Flink/Ballista adapters
remain future work. They MUST wrap the canonical portable partition task and isolated-worker
equivalence law; no framework may reinterpret CDF package or verdict semantics. Historical
owners:

- `.10x/tickets/cancelled/2026-07-05-distributed-execution-and-remote-state.md`
- `.10x/tickets/cancelled/2026-07-12-p3-j5-execution-plan-marshaling-metrics.md`

### WASM and additional foreign runtimes

WASM, Lua, registry admission, signing, sandbox brokers, and component SDKs remain future work.
The next WASM program starts with one ratified foreign-boundary state machine and a lossless
projection for recursive scope/source-position values. It must reconcile typed control,
terminal status, cancellation, broker quotas, and current Wasmtime/WASI behavior before
publishing versioned WIT. Historical research and owners:

- `.10x/research/2026-07-18-wasi03-stream-cost-interface-model.md`
- `.10x/research/2026-07-12-wit-recursive-value-projection.md`
- cancelled WASM/H4/WIT tickets under `.10x/tickets/cancelled/`

Current Python and subprocess admission MUST close independently of these future runtimes.

### DataFusion ecosystem expansion

CDF retains DataFusion as a standard analysis/scheduling currency under the identity boundary.
J1 statistics pruning is complete as a verified-package segment-selection authority with shared
memory admission, streamed decisions, and conservative absence. Object-store session registration, evidence
catalog/ADBC, `ExecutionPlan` marshaling, Ballista groundwork, exotic `FileFormat` hosting, and
selective kernel adoption are parked until a concrete consumer makes each bounded outcome
valuable. Authority:

- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `.10x/specs/datafusion-currency-bridges.md`
- cancelled J2/J4/J5/J6 tickets under `.10x/tickets/cancelled/`

### Destinations, warehouses, lakehouses, and secret providers

Future destination programs should be cut by one adapter or one shared provider boundary, never
by the former combined lakehouse/warehouse/vault ticket. Candidate programs include Iceberg and
Delta destinations, Snowflake, BigQuery, other warehouses, and cloud/vault secret providers.
Every destination must implement the destination ingress/sheet/receipt conformance boundary
without generic runtime branches. Historical owner:

- `.10x/tickets/cancelled/2026-07-05-lakehouse-warehouse-and-vault.md`

### Distribution hardening

The initial five-target checksummed GitHub install channel is complete. Signing/notarization,
package-manager channels, auto-update, a post-1.0 LTS selection, and upgrades away from GitHub
Actions versions carrying Node-runtime deprecation warnings remain demand-activated release work.
They MUST preserve the static-DuckDB release contract, generated-artifact freshness, reproducible
archives, checksum-before-install behavior, and the actual published-artifact smoke. Authority:

- `.10x/specs/versioning-lts-release-policy.md`
- `.10x/evidence/2026-07-26-p1-ws8-hosted-release.md`

## Activation rule

To activate a parked program:

1. Name the current product need and priority.
2. Revalidate temporal research and dependency capabilities.
3. Ratify any unresolved product semantics.
4. Open the smallest bounded ticket or a parent with bounded children.
5. Add it to the active stabilization successor or a newly authorized feature program.

Do not reopen historical monoliths merely to preserve their filenames.
