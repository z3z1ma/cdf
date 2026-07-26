Status: active
Created: 2026-07-25
Updated: 2026-07-25

# Active backlog and future roadmap

## Purpose

Keep CDF's executable backlog bounded, sequenced, and distinguishable from the long-horizon
product vision. A ticket is active only when its outcome is currently prioritized, its
dependencies are available, and its acceptance criteria are concrete enough to execute without
inventing product semantics. Future ambitions remain authoritative in `VISION.md`, decisions,
specifications, research, and this roadmap; they do not remain open tickets merely as reminders.

Cancelled tickets preserve investigation history and may be mined when a future program is
deliberately activated. Cancellation does not reject the capability.

## Current stabilization program

`.10x/tickets/2026-07-25-stabilization-steady-state-program.md` is the sole aggregate owner for
the work that must reach terminal state before the next two major feature programs begin.

Its ordered lanes are:

1. Source lifecycle and fixed-schema admission closure — done.
2. Evidence-driven statistics pruning — done.
3. Daily-driver CLI and release readiness — CLI and hosted prerelease done; P1 aggregate closeout
   remains.
4. Constant-memory and implemented-interop proof — done.
5. P3 aggregate evidence and closure — done; P1 aggregate closure is the final active lane.

Only bounded implementation tickets inside these lanes are executable. Parent and closeout
tickets coordinate or audit; they are not implementation units.

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

Resident streaming supervision, CDC source archetypes, `cdc_apply`, package rotation, pause,
drain, and resume remain future product work. They MUST reuse the existing finite drain-epoch,
watermark, rolling-spool, checkpoint, receipt, and commit-gate semantics rather than create a
parallel runtime. Historical owner:

- `.10x/tickets/cancelled/2026-07-05-cdc-and-streaming-supervisor.md`

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

## Activation rule

To activate a parked program:

1. Name the current product need and priority.
2. Revalidate temporal research and dependency capabilities.
3. Ratify any unresolved product semantics.
4. Open the smallest bounded ticket or a parent with bounded children.
5. Add it to the active stabilization successor or a newly authorized feature program.

Do not reopen historical monoliths merely to preserve their filenames.
