# CDF — Continuous Data Framework

## The Book of the System

**A Rust-native, DataFusion-powered, contract-governed data movement kernel, described completely.**

**Date:** July 5, 2026
**Scope:** This book is the system. It contains no implementation code, yet it is written so that the implementation follows from it as a consequence: every noun has a mechanism, a lifecycle, a serialized form, and a command that inspects it; every decision has a rationale and a revisit trigger; every guarantee has a derivation. Two teams reading this book independently should produce compatible systems, because the artifacts it specifies — not the prose — are the protocol.

---

## Preface: what "continuous" means

Twice in the history of software engineering, a word did the work of a revolution, and both times the word was *continuous*.

Continuous integration did not make integration faster. It made integration *small*. Before it, teams integrated in dreaded phases — weeks of divergence followed by days of merge archaeology — and the size of the phase was the size of the risk. The insight of CI was not a tool; it was a change of unit. Integrate every change, automatically, with a verdict attached, and integration stops being an event and becomes a property: the codebase is *always integrable*. Continuous delivery repeated the move one level up. Every change produces a versioned, immutable, content-identified artifact; the artifact is promoted through gates — build, test, staging, production — and each gate demands proof before promotion; deployment stops being a ceremony and becomes a property: the system is *always deployable*. In both cases, "continuous" never meant "always running." It meant **always ready, always provable, always reversible** — a property of the practice, not of the clock. A nightly build can be perfectly continuous in this sense. A 24/7 process that nobody can verify or roll back is utterly discontinuous.

Data engineering never received this revolution. Two decades after CI, most data loads are still artisanal events: a connector runs, rows disappear into a warehouse, a cursor advances somewhere, and when something is wrong three weeks later the investigation is archaeology — grep the logs, guess the state, re-run and hope. There is no artifact, so there is nothing to promote. There is no gate, so there is nothing to prove. There is no plan, so there is nothing to review. State is folklore. Failure is forensics.

CDF — the Continuous Data Framework — is the continuous practice applied to data movement, taken all the way down. Its doctrine transposes term by term:

| Continuous delivery | Continuous data |
|---|---|
| Every change builds an immutable, content-addressed artifact | Every run compacts into a hash-addressed **load package**: data plus evidence |
| Artifacts are promoted through gates, never rebuilt per environment | Packages are **replayed** into destinations, never re-extracted per target |
| A deploy is complete when health checks confirm, not when the script exits | A load is complete when the destination's **receipt** verifies, not when the writer returns |
| The release ledger records what shipped, when, and why | The **checkpoint ledger** records every state transition, append-only |
| `plan` before `apply`; the diff is the review artifact | `cdf plan` before `cdf run`; the plan — pushdown, DDL, guarantees — is reviewable in a pull request |
| Reproducible builds: same inputs, same bytes | **Golden packages**: same fixture, same manifest hash, across machines and months |
| Canary → ring → GA promotion, demotion on regression | Resource **trust promotion**: discovery → governed → fast path, demotion on drift |
| Rollback is a first-class operation | **Rewind and replay** are first-class commands |

At the center of the transposition stands one boundary, which this book calls **the commit gate**:

> **A source cursor may advance only after all data represented by that cursor has been durably committed to the destination and the destination's receipt has been recorded in the checkpoint ledger. Nothing passes the commit gate without proof.**

Every chapter that follows is machinery to make that sentence cheap, ergonomic, inspectable, and fast — and to make its consequences (replay, resume, audit, rewind, reproducibility) fall out as ordinary commands rather than heroic operations.

One clarification before anything else, because the name invites the misreading: **continuous does not mean streaming.** CDF runs batch pipelines, micro-batch pipelines, and (in a later executor mode) unbounded streams, and all three can be equally continuous, because continuity is the always-provable property, not the always-running property. A pipeline that fires once a night, produces an artifact, passes the gate, and can be replayed byte-identically is continuous. Streaming is a scheduling mode; continuity is a discipline.

## How to read this book

The book runs in six parts. Part I positions the system: the thesis, the nouns, the lessons taken from a decade of predecessors, and the Decision Register — every design decision the system has taken, numbered, answered, and armed with a revisit trigger. Part II is foundations: a small formal model, the transition calculus, from which the system's guarantees are derived rather than asserted. Part III is the system itself, thirteen chapters ordered so each depends only on chapters before it: layering, runtime, types, resources, authoring, batches, contracts, packages, checkpoints, destinations, errors, observability, security. Part IV is the surface operators and contributors touch: CLI, project format, conformance, compatibility, governance. Part V is the road: what ships first, what is deliberately cut, and how every cut item arrives later through a seam designed now. Part VI is the standing review discipline: the invariants against which any change to the system — including changes to this book — must be judged, and the questions the design holds open on purpose. Appendices carry the glossary, the internal stress harness, the July 2026 dependency survey grounding the engineering claims, and a catalogue of objections with answers.

Most substantial chapters speak in three registers, marked in the prose rather than with labels. First the **intuition**: an analogy precise enough to reason with — bank settlement for receipts, customs declarations for capability sheets, double-entry bookkeeping for the mirrored ledger. Then the **mechanism**: the traits, schemas, tables, and flows an implementer builds. Then, where the material deserves it, the **theory**: the named result or research lineage the mechanism instantiates — the end-to-end argument behind receipts, idempotence as the resolution of exactly-once, watermark semantics from the dataflow literature, content addressing from Merkle structures, design-by-contract behind the validation compiler. A reader can stop at any register and hold a correct model; the deeper registers refine, never revise, the shallower ones.

Conventions: `cdf` in code face is the CLI binary and the crate prefix; CDF in prose is the project. Interface sketches in Rust, TOML, Python, SQL, and WIT are normative for shape and semantics, not for identifier-level API stability. Where a chapter and the Decision Register appear to disagree, the register rules.

---

# Part I — Position

## Chapter 1: The thesis

### 1.1 What CDF is

CDF is a data movement kernel: a Rust library, a family of engine and extension crates, and a single CLI that together turn data ingestion from opaque connector execution into a planned, optimizable, contract-governed, replayable state transition over Arrow-native batches.

One sentence per load-bearing noun, each expanded in its own chapter:

- A **resource** is an optimizable data source: it declares its schema, keys, cursor, partitioning, and pushdown capabilities, and it produces Arrow record batches. (Chapter 8)
- A **run** compiles resources into read plans, executes them as streams, validates them against contracts, and packages the results. (Chapters 6, 12)
- A **load package** is the durable, hash-addressed evidence of a run — data, schema, contract verdicts, quality profiles, lineage, state deltas, destination receipts — the build artifact of data. (Chapter 12)
- A **checkpoint** is a typed state transition that commits only after the destination has durably acknowledged the data the checkpoint represents — the moment the commit gate opens. (Chapter 13)
- A **destination** is a transactional or idempotent commit target that answers in receipts, never a sink that swallows rows. (Chapter 14)
- A **contract** is executable policy: a compiled validation program with a total verdict for every cell of every batch. (Chapter 11)
- A **capability sheet** is a machine-readable declaration of what a resource or destination can actually do, tested by a conformance suite and snapshotted into a lockfile. (Chapters 8, 14, 19)
- A **plan** is the pre-execution artifact that states what will be read, what will be pushed down and at what fidelity, what DDL may run, which delivery guarantee applies, and which state will advance. (Chapters 8, 18)

### 1.2 What CDF is not

CDF is not a better Singer, a lighter Airbyte, a native Meltano, or dlt ported line-for-line to Rust. It is not a workflow orchestrator: it schedules nothing and embeds in any scheduler. It is not a dbt replacement: post-load model graphs belong downstream, and the project layer hands them a receipt as their trigger. It is not a BI tool, a catalog, or a hosted platform. It is not a connector zoo whose north-star metric is connector count; breadth is an outcome of a provable protocol (Chapter 20), never a roadmap phase. The kernel is headless, embeddable, and CLI-first; anything with a screen sits above it and depends on it, never the reverse.

CDF also refuses "exactly-once" as a slogan. Chapter 4 derives, and Chapter 14 tabulates, the honest position: at-least-once extraction composed with idempotent or transactional destination commits yields effectively-once observed results at specific destinations under specific, stated conditions — and `cdf plan` prints which condition holds before a byte moves. Where no condition holds, the plan says so, in advance, in writing.

### 1.3 The category shift

Every predecessor system, whatever its virtues, *runs a configured connector*. CDF *compiles a read*. The difference is the difference between a shell script and a program with a type checker.

The intuition is `terraform plan`. Infrastructure-as-code did not merely automate provisioning; it interposed a reviewable artifact between intent and effect — here is exactly what will be created, changed, destroyed — and that artifact changed the sociology of operations: risky changes became pull-request reviews. CDF interposes the same artifact into data movement. Before execution, the operator reads what will be fetched, which predicates the source will absorb and with what fidelity guarantee, how work partitions, what DDL might run at the destination, which delivery guarantee the combination of source, disposition, and destination earns, and which state will advance. After execution, the run has left a package: evidence that outlives the process, replays without touching the source, and hashes identically when nothing changed.

The mechanism is a compilation pipeline (Chapter 8): resource declarations lower to logical read plans; capability negotiation lowers those to physical scan plans with per-predicate pushdown fidelity; contract policy compiles against the observed schema into a validation program; the destination's capability sheet joins against the output schema to produce a commit plan with any required migrations. Each stage's output is serialized — into the plan artifact before the run, into the package after it.

The theory is that this restores the classic phase separation of compilers to a domain that lost it: analysis errors (type mismatches, identifier collisions, unsupported mappings, guarantee downgrades) surface at plan time, where they are cheap, instead of at load time, where they are incidents. Data movement becomes as inspectable, replayable, and boring as merging code. That is the entire ambition, and "boring" is the highest compliment this book knows.

## Chapter 2: The landscape and its lessons

CDF descends from a decade of open-source data movement, and it is explicit about its inheritance: every predecessor contributed a lesson, and every lesson lands in a named CDF mechanism. This chapter gives each predecessor a fair reading — what it proved, where it stopped, and what CDF does with both.

### 2.1 dlt: the library-first proof

dlt proved the largest single lesson: ingestion should feel like ordinary programming. Its Python decorators turn a generator into a pipeline participant; its schema inference makes first contact with a messy source frictionless; its hints (primary key, merge key, incremental cursor) attach intent without ceremony; its schema contracts (`evolve`, `freeze`, `discard_row`, `discard_value`, applied uniformly whether the author yields dicts, Arrow tables, or dataframes) made governance a resource-level knob; its declarative REST source demonstrated that the majority of API ingestion needs no code at all — a lesson its ecosystem has since amplified by letting language-model agents generate thousands of declarative sources, which works precisely because a constrained, schema-validated format is the safest thing to let an agent write. Its destination layer owns schema migration; its `_dlt_loads` table gave operators a warehouse-native audit trail.

Where dlt stops is where CDF begins. Python object iteration remains its execution substrate, so the fast path tops out at the interpreter. Its state is practical but informal relative to a ledger: there is no typed transition history, no structural commit protocol, no receipt. There is no pre-run plan artifact: dlt normalizes and loads what arrived; it cannot tell you in advance what it will fetch, because fetching is opaque user code. And its load packages are staging, not evidence — useful in flight, not designed to be replayed, diffed, and audited for years. CDF keeps dlt's ergonomics (the resource model, the hints, the contracts, the declarative tier, and a compatibility shim that runs `@dlt.resource` functions directly — §21.2) and replaces the substrate, the state model, and the evidence model underneath them.

### 2.2 Singer: the pipe-debuggable protocol

Singer's genius was smallness: three JSON message types — `SCHEMA`, `RECORD`, `STATE` — flowing over stdout, composable with a shell pipe, debuggable with `head`. Any language could produce them; any target could consume them. That composability lesson survives in CDF's subprocess tier (§9.6), which keeps stdio as a first-class boundary.

Singer's failure was underspecification, and it is worth being precise about the failure mode because it recurs across the industry: when a protocol leaves state as an arbitrary blob and capabilities as undocumented behavior, every tap-target pair becomes its own compatibility project, quality varies invisibly between forks, and the ecosystem's breadth becomes a liability rather than an asset. CDF's answers are the typed position vocabulary with an honest `ForeignState` quarantine type for genuinely opaque foreign state (§13.3), and capability sheets that are tested claims rather than folklore (§8.4).

### 2.3 Airbyte: the destination-acknowledged state lesson

Airbyte formalized what Singer left loose: sources and destinations as protocol actors, catalogs with declared streams, and — the contribution CDF elevates to its central invariant — the rule that a destination must emit state only after the preceding records are durably committed to long-term storage. Emitting state early is, in Airbyte's own terms, an error. That is the commit gate in embryo.

What CDF declines to inherit is the packaging: connector containers and inter-process JSON as the default data plane, the platform as the kernel, and state semantics that live inside platform behavior rather than in an inspectable artifact. CDF keeps the actor rigor, mechanizes the acknowledgment rule into a structural API (there is no way to commit a checkpoint except through a function that demands a receipt — §13.2), and moves the default execution back into one process over Arrow memory. An Airbyte-source adapter (§21.3) welcomes the connector ecosystem across the bridge; Airbyte destinations are deliberately not bridged, because the destination side is where CDF's receipt protocol lives, and delegating it would forfeit exactly the guarantees the system exists to provide.

### 2.4 Meltano: projects as code

Meltano's contribution was never a data plane; it was the discovery that data projects deserve the same Git-native treatment as software projects: declarative project files, environment overlays, lockfiles, and — subtly important — *variant awareness*, the recognition that two connectors with the same name routinely differ in behavior and quality. CDF's project format (Chapter 19) is Meltano's lesson with the lockfile extended from versions to semantics: CDF locks capability sheets, type-mapping tables, contract snapshots, and the identifier normalizer version, so an upgrade that changes *meaning* — not merely bytes — surfaces as a reviewable diff (§19.3).

### 2.5 Sling: operational crispness

Sling proved that a data tool can feel like a good Unix tool: one command moves data, flags or a YAML replication file configure it, and the operator is done in a minute. CDF's CLI (Chapter 18) holds itself to that standard while backing every crisp command with the ledger, packages, and plans the crispness previously traded away.

### 2.6 Bruin and Mage: the unified surface and the inner loop

Bruin demonstrated appetite for one project surface spanning ingestion, quality, and comparison — and the danger of building the monolith before the kernel; CDF answers with a project layer that is a crate depending on the kernel, never the reverse (Chapter 5). Mage demonstrated the developer inner loop that ingestion tooling forgot: preview one batch, run one partition, inspect everything, locally, now. CDF ships that loop as `cdf preview`, `cdf inspect`, and `cdf replay` — with no UI in the kernel's dependency graph, because the loop is a property of the artifacts, not of a screen.

### 2.7 The substrate: why the ecosystem argument now favors this design

A design that bets its execution on Apache DataFusion is betting on a maintained path, not an idle library, and the evidence is the family built on the same extension points CDF uses — `TableProvider`, `ExecutionPlan`, optimizer rules, filter-pushdown negotiation. Delta Lake's Rust implementation, LanceDB, InfluxDB 3, GreptimeDB, the Comet accelerator that translates Spark plans into DataFusion plans, the Ballista distributed engine, and an ADBC driver exposing DataFusion to a dozen languages all ride these seams in production. DataFusion's own recent motion is CDF's motion: LIMIT-aware Parquet pruning, dynamic filters pushed through joins and subqueries, and order-of-magnitude cheaper plan cloning for systems that plan constantly — which CDF, a system that compiles every read, emphatically is. The dependency moves fast, on a published cadence; Chapter 22 pins how CDF rides it without being dragged. But the direction of upstream motion and the direction of this design are the same direction.


## Chapter 3: The Decision Register

A design meant to be built cannot leave questions open, and a design meant to be maintained cannot hide the reasoning behind its answers. Every decision below is numbered, answered, and armed with a *revisit trigger* — the observable condition under which it should be reopened. Chapters elaborate; the register rules. When a future change is proposed, the first question is which register entries it touches; a change that touches none is a refinement, and a change that silently contradicts one is a bug in the process before it is a bug in the code.

**D-1. How deeply is DataFusion embedded?**
Two tiers. Every resource implements the minimal `ResourceStream` contract — descriptor, partitions, batch stream — built on arrow-rs types alone. Resources that can do less work when asked for less additionally implement `QueryableResource`, which the engine wraps in a DataFusion `TableProvider`. DataFusion is mandatory in the engine crate and invisible in the authoring path: a resource author never has to learn that a query engine exists. *Revisit if* a large fraction of first-party resources hand-roll pushdown the engine could have negotiated, which would argue for promoting the queryable tier to the base.

**D-2. How is user extraction logic supplied?**
A five-tier ladder, cheapest first: (0) declarative TOML/YAML compiled to native resources and validated by a published JSON Schema; (1) Rust, statically linked; (2) Python, embedded via PyO3 with zero-copy Arrow interchange over the Arrow C Data Interface and PyCapsule protocol, shipped with a fully typed `cdf-sdk`; (3) WebAssembly Components over a published WIT world on the WASI 0.3 baseline, sandboxed, for third-party distribution; (4) subprocess adapters speaking Arrow IPC, NDJSON, Singer, or Airbyte protocol over stdio. Lua, bespoke DSLs, and dynamically loaded Rust plugins are rejected with reasons (§9.7). Every tier's interchange with the kernel is identical: Arrow batches plus a descriptor. *Revisit* per D-25 and D-26, which govern the Python and WASM baselines respectively.

**D-3. How are quarantine side-outputs represented?**
As a framework construct, not a query-plan construct. `ContractExec` yields one accepted stream to the plan and routes rejected rows through a side channel into the package's `quarantine/` artifacts, each row carrying its error code, contract rule ID, and source position. Destinations may optionally materialize `_cdf_quarantine` tables. DataFusion's physical plans are single-output, and contorting them into multi-output semantics would buy nothing but coupling. *Revisit if* DataFusion grows first-class multi-output plans.

**D-4. What is the canonical package data format?**
Arrow IPC (file format, LZ4-framed) inside a package, because packages must round-trip the exact observed schema — dictionary encodings, field metadata, extension annotations — and Parquet's type system is a lossy projection of Arrow's. Parquet is the archive and interchange tier (`cdf package archive` transcodes with a fidelity report) and the analytical tier: stats, quarantine, and lineage are Parquet because they are queried across packages. Manifests and receipts are canonical JSON. Everything is hash-addressed (§12.5). *Revisit if* Arrow IPC forward-compatibility across arrow-rs majors bites in practice, at which point the canonical and archive roles swap.

**D-5. Where does state live?**
Behind a `CheckpointStore` trait from the first commit; SQLite in WAL mode is the local default, and an in-memory store serves tests. Destinations that can hold tables additionally mirror durable load facts into `_cdf_loads` and `_cdf_state` (§13.6), enabling ledger reconstruction and warehouse-native audit. Postgres- and object-store-backed stores arrive post-MVP; the trait is sized for them, including the lease semantics distribution will require. *Revisit if* multi-writer local state becomes common before distributed mode ships.

**D-6. How strict are contracts by default?**
Default `evolve` — new tables and columns admitted, type widening admitted, narrowing quarantined — because first contact with a source is always discovery, a conclusion the field's collective experience with dlt-style contracts supports. `cdf contract freeze` flips a resource; project-level trust presets set stricter defaults in one line (§11.4). *Revisit if* real-project telemetry shows `evolve` defaults causing silent-drift incidents; the fallback posture is freeze-after-first-clean-run.

**D-7. How does pushdown declare correctness?**
Per predicate, in the vocabulary DataFusion's own `TableProviderFilterPushDown` established: `Exact` (source semantics provably match engine semantics; the engine drops its filter), `Inexact` (source returns a superset — prunes pages, windows, partitions — and the engine re-applies the exact predicate), or `Unsupported`. API resources default to `Inexact`, because remote timezone, collation, and consistency semantics rarely match Arrow's bit-for-bit, and an over-claimed `Exact` is silent data loss. `cdf explain` prints the classification per predicate. *Revisit:* never; production experience across the DataFusion ecosystem settles this vocabulary.

**D-8. How do bounded and unbounded resources share the runtime?**
Both produce the kernel's `BatchStream`; the difference is a `Boundedness` marker plus mandatory policy. An unbounded plan is illegal without a checkpoint cadence, a package rotation rule, and a watermark strategy — the planner refuses otherwise. The first executor runs bounded plans to completion and unbounded plans in *drain mode* (until quiescent or `--max-duration`), which covers micro-batch CDC and queue draining; a resident streaming supervisor is a later executor mode over unchanged types (§25.3). *Revisit if* log-CDC demand outruns the streaming milestone; CDC ships first as bounded micro-batches over a log position.

**D-9. Do transformations live in CDF?**
In-flight, per-batch, schema-stable transforms yes: rename, cast, redact, derive, filter, nested-structure policy — compiled into the normalize operator and recorded in the package like every other decision. Multi-table, post-load model graphs no; the project layer triggers dbt or SQLMesh with the load receipt as the handoff artifact (§11.6). *Revisit if* users consistently smuggle post-load SQL into resources, which would argue for a minimal post-load hook before it argues for a transform layer.

**D-10. Are package manifests signed?**
Hash-addressed now — SHA-256 content addressing, with the canonicalized manifest hash serving as the package's identity — and signature-ready now, via a reserved detached-signature slot with a defined signing input. Actual signing lands post-MVP behind a feature flag, and the same slot becomes the connector-signing trust surface when third-party distribution arrives. Hashing delivers integrity; signatures deliver attribution; the design needs attribution only when strangers' code ships. *Revisit if* a compliance-driven design partner needs signatures at MVP.

**D-11. When does distributed execution arrive?**
After local correctness is proven: after both conformance suites pass on three sources and three destinations. The seams exist from the first commit — partitions are the distribution unit, packages are the shuffle-free hand-off artifact, and the `CheckpointStore` trait admits a fenced remote implementation. Before building a scheduler, Ballista — DataFusion's own distributed subproject — is evaluated as substrate rather than reinvented (§25.2). *Owner: the milestone after conformance.*

**D-12. Async runtime and concurrency model?**
Tokio, multi-threaded, because DataFusion, object_store, and every relevant driver already live there. CPU-heavy work runs on a pool separate from I/O reactors, following the two-runtime pattern DataFusion's maintainers document; blocking FFI — DuckDB's C API, the Python interpreter — is confined to bounded `spawn_blocking` pools (§6.1).

**D-13. Memory management?**
One accounting ledger: DataFusion's `MemoryPool` for engine operators, extended to every framework component that buffers — package builders, adapter queues, appender staging. Exceeding budget triggers, in order, early package-segment flush, backpressure, spill, and clean failure. Surprise OOM is a bug class, not a weather condition (§6.3).

**D-14. Identifier and naming policy?**
Source-original names preserved verbatim in schema metadata, forever. A versioned normalizer (`namecase-v1`) derives destination identifiers deterministically; post-normalization collisions are plan-time hard errors with rename hints; the normalizer version is recorded in every schema snapshot and package, because changing it re-keys everything downstream (§7.4).

**D-15. Canonical type system?**
Arrow's, closed, plus three metadata annotations: semantic tags, source-name provenance, and nullability provenance. Destination type mappings are declared tables in capability sheets, joined at plan time and snapshotted in the lockfile; lossy mappings require the explicit contract allowance `allow_lossy_mapping`, and unsupported mappings fail the plan (§7.3).

**D-16. Deletes and CDC semantics?**
Write dispositions include `cdc_apply`; CDC-shaped batches carry a `_cdf_op` column (`insert | update | delete`) plus source positions. The first release covers deletion-aware merge for cursor sources that expose deletions; log-based CDC is a later source archetype whose state shapes — `LogPosition`, transaction boundaries — the ledger already models (§13.3).

**D-17. Retry, rate limit, and error taxonomy?**
One taxonomy — `Transient`, `RateLimited`, `Auth`, `Contract`, `Data`, `Destination`, `Internal` — drives retry at the smallest safe unit (request → partition → run) under a run-level retry budget. A built-in HTTP toolkit gives every declarative and Python API resource correct pagination, rate limiting, backoff, and auth refresh by construction rather than by author diligence (Chapter 15).

**D-18. Secrets?**
References, never values, in every serialized artifact: `secret://provider/key` URIs everywhere; resolution at execution through a `SecretProvider` trait; resolved values held in zeroizing wrappers registered with a redaction layer that scrubs diagnostics by construction (§17.1).

**D-19. Observability?**
`tracing` throughout with optional OTLP export — and, more fundamentally, the system's own artifacts are the primary observability surface. The ledger is SQLite, the stats are Parquet, the engine is DataFusion, so `cdf sql` queries the system's own operational history natively (Chapter 16). Introspection is a query, not a log grep.

**D-20. Scheduling?**
Out of kernel, permanently. The CLI is scheduler-friendly — single binary, meaningful exit codes, `--json` on every command — and the library embeds in any orchestrator. `cdf run --loop <interval>` exists for local development only. *Revisit:* never for the kernel.

**D-21. Testing strategy?**
Three pillars: conformance suites that define what "supported" means for any resource or destination implementation; a chaos layer that kills the process at every lifecycle boundary in CI, on every merge, forever; and golden-package tests comparing produced evidence hash-by-hash against committed fixtures (Chapter 20).

**D-22. Licensing and governance?**
Apache-2.0, one repository, crates on crates.io under semver. The serialized artifacts — checkpoint schema, package manifest, capability-sheet format, the WIT world, the declarative JSON Schema — are independently versioned specs with committed migrations, because artifacts outlive binaries (Chapter 22).

**D-23. Python's role, precisely?**
Authoring and interchange surface, never execution substrate. Python runs to *produce* batches; from the instant a batch crosses the C Data Interface, everything downstream is Rust. PyO3 lives in the optional `cdf-python` crate; the kernel's dependency graph contains no Python (§9.4).

**D-24. Naming?**
The project is **CDF, the Continuous Data Framework**. The name states the doctrine: the continuous practice — small, automated, verified, reversible — applied to data movement, in direct lineage from continuous integration and continuous delivery, with "continuous" meaning always-provable rather than always-running (Preface). CLI binary `cdf`; crate prefix `cdf-`; PyPI package `cdf-sdk`; WIT namespace `cdf:resource`; dot-directory `.cdf/`; project file `cdf.toml`; internal destination tables `_cdf_loads`, `_cdf_state`, `_cdf_quarantine`; system columns `_cdf_op`, `_cdf_variant`, `_cdf_load`, `_cdf_segment`, `_cdf_row`, `_cdf_loaded_at_ms`. The provenance tuple `(_cdf_load, _cdf_segment, _cdf_row)` is the framework-owned address of a row CDF committed—package, segment, ordinal—not a user key; it makes exact replay, diagnosis, and governed corrections possible without making append invent business identity.

**D-25. Free-threaded Python posture?**
CPython 3.14's free-threaded build is officially supported upstream, and PyO3 0.28+ targets it — including the `Python::detach` API and the abi3t stable-ABI path arriving with 3.15. CDF's posture: **correct on GIL builds, parallel on free-threaded builds, identical semantics on both.** `cdf-sdk` and `cdf-python` are built and CI-tested against both a GIL 3.12+ interpreter and 3.14t; on free-threaded interpreters, multiple Python resources — and multiple partitions of a `parallel`-marked resource — execute concurrently on the dedicated Python pool. The design must not *depend* on free-threading, and must not waste it either. *Revisit if* the ecosystem's default interpreter flips to free-threaded, at which point the GIL path becomes the compatibility mode.

**D-26. WASM tier baseline?**
WASI 0.3 — which brought native `async func`, `stream<T>`, and `future<T>` into the Component Model's canonical ABI, with the host owning one event loop across all components — is the baseline for CDF's WIT world. A guest resource exports `open(partition)` as an async function returning a `stream<u8>` of Arrow IPC bytes; cancellation semantics arrive with the 0.3.x train; Wasmtime is the reference host. Guest SDKs: Rust first, Python via componentize-py as its 0.3 support matures. Timing: designed now, shipped post-MVP (§9.5). *Revisit at* WASI 1.0 for the registry's long-term interface freeze.

**D-27. Lakehouse table formats?**
Iceberg and Delta are destinations, not package formats. The package remains CDF's own evidence layout (D-4); post-MVP, `cdf-dest-iceberg` and `cdf-dest-delta` commit packages into lakehouse tables through iceberg-rust and delta-rs — both DataFusion-native projects — with the table format's own snapshot metadata carried inside the CDF receipt, so the commit gate and the table's transaction log corroborate each other. Two ledgers agreeing beats one ledger trusted. *Revisit if* a design partner needs Iceberg at MVP; the Parquet/object-store destination is the designed seam.

**D-28. Upstream cadence policy?**
The load-bearing dependencies move on published schedules: arrow-rs releases majors at most quarterly and minors monthly, holding deprecated APIs roughly two majors; DataFusion majors land several times a year; duckdb-rs encodes the bundled DuckDB version in its own semver and maintains an LTS branch; object_store lives in its own Apache repository on its own cadence. CDF's policy: each CDF minor release pins exactly one (arrow-rs major, DataFusion major, object_store minor, duckdb-rs DuckDB-encoded version) tuple, recorded in the lockfile spec. Upgrades are deliberate, gated on the golden-package suite — Arrow IPC byte-stability is a release gate — and never occur in CDF patch releases. *Revisit if* an upstream breaks serialized-artifact compatibility, which promotes that dependency into the artifact-spec review process.

---

# Part II — Foundations

## Chapter 4: The transition calculus

This chapter is the book's theoretical spine: a small formal model from which the system's guarantees are derived rather than asserted. Nothing in it is exotic — every result is a known pattern from databases and distributed systems, assembled — but writing the model down does three jobs. It makes the guarantees checkable: every claim in Chapter 14's guarantee table reduces to these definitions. It makes the crash matrix (§12.3) a case analysis rather than a checklist. And it gives reviewers a fixed point: a proposed change either preserves these invariants or it does not, and the review can say which.

### 4.1 The intuition: authorization is not settlement

Banking learned centuries ago to separate two moments that look like one. When a card is swiped, the network *authorizes* — funds appear reserved, the receipt prints — but the money has not moved; it moves at *settlement*, a later, verified, recorded exchange between institutions, and only settlement updates the books. Every data-loading bug of the "the cursor advanced but the rows never landed" family is a system confusing authorization with settlement: the writer returned, so the cursor moved, but the destination never durably settled the data. CDF's model is the banking model. The write returning is authorization. The receipt verifying is settlement. Only settlement — the commit gate — updates the ledger.

### 4.2 The model

Let a **scope** σ identify a unit of state (a resource, or a partition, window, file, or stream within one — §13.4). The **ledger** L(σ) is an append-only sequence of transitions; the **head** of a scope is its latest committed transition. A **transition** is a tuple:

```text
t = ⟨ s_in,  P,  V,  r ⟩

s_in  the committed head consumed (the input position)
P     the load package: batches B, compiled contract program C, plan artifacts,
      proposed output position s_out — all hash-addressed, hash(P) is t's identity
V     the contract verdicts actually rendered over B by C (serialized in P)
r     the destination receipt covering hash(P) and every segment of B
```

Three primitive operations:

```text
propose(σ, P)        append t to L(σ) with status = proposed        (before any destination write)
commit(σ, t, r)      status ⟶ committed, head(σ) ⟶ t                (the commit gate)
rewind(σ, t')        append a rewound marker; head(σ) ⟶ t'          (never deletes history)
```

### 4.3 The invariants

**I1 — Gate.** `commit(σ, t, r)` succeeds only if `verify(r, dest)` succeeds and `r` structurally covers `hash(P)` and every segment of `B`. There exists no other operation that sets `status = committed` or moves a head forward. (Mechanized in §13.2: the gate is a function signature before it is a discipline.)

**I2 — Monotone ledger.** L(σ) is append-only. Heads move forward only via `commit` and backward only via an explicit, recorded `rewind`. History is never rewritten; a rewound transition remains in the ledger with its marker.

**I3 — Replay determinism.** A package fixes its own replay: `replay(P, dest)` depends only on P's contents — recorded batches, recorded verdicts, recorded commit plan — and never re-derives batching, re-runs extraction, or re-evaluates contracts. Consequently two replays of one package are byte-equivalent inputs to the destination.

**I4 — Idempotent application.** For destinations declaring `package_token` idempotency, `apply(apply(D, P), P) = apply(D, P)`: re-driving a package is a no-op answered with a duplicate-marked receipt. For destinations declaring `merge_keys` idempotency, the same equation holds for the `merge` disposition restricted to key-observable state.

**I5 — Window soundness.** For a cursor with ordering claim `best_effort` and lag tolerance λ, the committed output position satisfies `s_out ≤ max(observed cursor) − λ`. (The mechanism is `CursorSpec` in §8.2; the consequence is that late writes inside the lag window are recaptured by the next run instead of skipped forever.)

### 4.4 Derived guarantees

**Effectively-once is a composition, not a feature.** Exactly-once *delivery* between independent parties is not achievable in the presence of failures — the sender cannot distinguish a lost acknowledgment from a lost message, so it must retry, so the receiver may see duplicates. This is the oldest lesson of reliable messaging, and every honest system resolves it the same way: at-least-once delivery composed with an idempotent receiver yields exactly-once *observed effects*. In this calculus: extraction is at-least-once by construction (I1 forbids advancing past unproven data, so crashes cause re-reads, never skips); I3 makes retried loads byte-identical; I4 makes byte-identical loads collapse to one effect. Chapter 14's guarantee table is this composition evaluated cell by cell against declared destination capabilities — which is why the table is *derived* by the planner, not curated by hand.

**The receipt is the end-to-end argument, applied.** The end-to-end argument — the design principle that correctness checks must live at the endpoints of a system, because intermediate reliability cannot substitute for endpoint verification — is why a receipt is not merely a return value. A driver's write call returning is an intermediate signal; TCP delivering bytes to a warehouse's load endpoint is an intermediate signal; only a check *against the destination's own durable state* is an endpoint check. Hence every receipt carries a `verify` clause (§14.4): a destination-declared procedure by which a third party — or the crash-recovery path — confirms the receipt against the destination itself. Recovery in the committed-but-not-checkpointed window runs `verify` before opening the gate, closing the loop between claim and reality.

**Crash consistency is a case analysis.** A crash interrupts a transition at some prefix of its lifecycle. I2 and I3 make each prefix decidable from durable evidence alone: no receipt and no finalized package → the transition never happened, re-plan; finalized package, no receipt → replay P (I3), no source contact needed; receipt durable, gate not yet passed → verify and commit, a pure ledger operation; gate passed → done. The five-row crash matrix of §12.3 is this analysis written as an operations table, and the chaos layer (§20.2) is this analysis executed as a test, at every boundary, forever.

**Replays are confluent.** From I3 and I4: for token-idempotent destinations, any interleaving of replays of any set of committed packages reaches the same destination state. Operationally this is the license behind the calmest sentence in the book — `cdf replay` is always safe when in doubt — and behind cross-destination migration by package replay rather than re-extraction.

**Why not two-phase commit.** The textbook answer to "make two parties agree" is distributed 2PC, and CDF deliberately avoids it. 2PC buys atomicity across parties at the price of blocking on coordinator failure and requiring participant votes; CDF needs neither, because its structure is asymmetric: the destination is the *sole* voter (its receipt is the only decision), and the source side needs no vote because I3+I4 make its retries harmless. The result has 2PC's outcome — no state advances without the durable party's confirmation — with none of its blocking, which is precisely the trade the presumed-abort literature recommends when one side can be made idempotent. When CDF later coordinates *many* workers (§25.2), the same asymmetry holds per partition, and coordination reduces to lease fencing on the shared ledger rather than to consensus on data.

### 4.5 What the calculus does not claim

It does not claim the source is consistent: a source that lies about its cursor ordering beyond its declared lag violates I5's premise, which is why ordering claims are conformance-tested, not trusted (§20.1). It does not claim byte-level exactly-once into destinations that declare no idempotency — the guarantee table says at-least-once there, and the planner warns. And it does not claim atomicity across *multiple* destinations in one run: each destination settles independently, and cross-destination agreement is by replayed packages, not by transaction. Each non-claim is a boundary the operator can read in the plan, which is the calculus doing its final job: making the limits as legible as the guarantees.


# Part III — The System

## Chapter 5: Layering

### 5.1 The four layers

```text
┌────────────────────────────────────────────────────────────────┐
│  Layer 4: Project & Product                                     │
│  cdf.toml, environments, lockfile, CLI, doctor, status          │
├────────────────────────────────────────────────────────────────┤
│  Layer 3: Extensions                                            │
│  authoring tiers (declarative, Python, WASM, subprocess),       │
│  destinations, formats, HTTP toolkit, secret providers          │
├────────────────────────────────────────────────────────────────┤
│  Layer 2: Engine                                                │
│  DataFusion providers, physical operators, planner, executor    │
├────────────────────────────────────────────────────────────────┤
│  Layer 1: Kernel                                                │
│  types, traits, contract compiler, package model,               │
│  checkpoint ledger, receipts — arrow-rs only: no DataFusion,    │
│  no DuckDB, no Python, no network                               │
└────────────────────────────────────────────────────────────────┘
```

The dependency rule is strict and singular: lower layers never import upper ones. The intuition is a well-run kitchen — the recipe book (kernel) does not know which stove (engine) is installed, the stove does not know which suppliers (extensions) deliver, and none of them knows the dining room (product) exists — and the payoff is the same as in the kitchen: any layer can be replaced, tested, or reasoned about without dragging the others along.

### 5.2 The kernel is engine-free, and why that is not purism

Layer 1 defines `Resource`, `Batch`, `Contract`, `Checkpoint`, `LoadPackage`, `Destination`, `Receipt`, and the transition calculus's state machine, depending on arrow-rs — schemas, arrays, IPC — and nothing heavier. Three concrete purchases justify the discipline. First, the correctness core — the crash matrix, the commit gate, the package lifecycle — tests in milliseconds without a query engine in the harness, and failures mean something; a correctness test that takes minutes and imports half the ecosystem is a correctness test nobody runs. Second, the serialized artifacts are specified by kernel types, so an engine swap — or a second implementation in another language — reads the same manifests, checkpoints, and receipts; the artifacts are the protocol, and the kernel is their reference semantics. Third, embedding: a service that wants receipts and checkpoints but brings its own extraction loop depends on Layer 1 alone.

The one deliberate dependency deserves defense. A kernel that abstracted over Arrow would be abstracting over the industry's agreed memory format for no benefit: arrow-schema and arrow-array are stable, light, and exactly the vocabulary the artifacts need. The kernel's `BatchStream` is a pinned boxed `Stream` of `Result<RecordBatch>` over arrow-rs types; `cdf-engine` adapts it to DataFusion's `SendableRecordBatchStream` at the boundary, in both directions, and no DataFusion type ever appears in a kernel signature.

### 5.3 The engine owns execution; the kernel owns meaning

DataFusion, in Layer 2, owns Arrow expression evaluation, SQL and DataFrame planning, projection/filter/limit pushdown negotiation (including the dynamic filters recent releases push through joins and subqueries), partitioned vectorized execution, and `EXPLAIN`. The kernel owns what a run *means*: which committed state it consumed, which contract governed it, what evidence it must produce, and when a transition may pass the commit gate. The engine's operators — `ContractExec`, `NormalizeExec`, `ProfileExec` — *enforce* decisions; the decisions themselves are Layer 1 values fixed at plan time and serialized into the package. The engine may be re-planned; the meaning may not. This is the separation that lets §4.3's invariants live in a crate that a query-engine upgrade cannot touch.

### 5.4 Bounding the DataFusion relationship

Three rules keep a fast-moving dependency healthy rather than viral. Kernel types never expose DataFusion types in public signatures. Each CDF minor release pins one DataFusion major (D-28) and upgrades deliberately, gated on the golden-package suite. Every CDF-authored physical operator lives behind an internal facade with a hand-rolled fallback path, so an upstream breaking change degrades performance, never correctness. The relationship also runs the other way: where CDF needs something DataFusion almost has — multi-output plans for quarantine being the standing example — CDF builds around the engine rather than forking it, and files the upstream issue, because the ecosystem argument of §2.7 only holds if its members behave like an ecosystem.

### 5.5 Crate map

```text
crates/
  cdf-kernel/          # Layer 1. Types, traits, state machine, artifact specs. arrow-rs only.
  cdf-engine/          # Layer 2. DataFusion providers, operators, planner, executor.
  cdf-contract/        # Contract compiler: policy × observed schema → validation program.
  cdf-package/         # Package builder, reader, replayer, hasher, GC, archive transcoder.
  cdf-state-sqlite/    # CheckpointStore + run ledger over SQLite (WAL).
  cdf-http/            # HTTP toolkit: paginators, limiter, backoff budget, auth sessions.
  cdf-formats/         # arrow-ipc, parquet, ndjson, csv adapters.
  cdf-declarative/     # Tier 0: TOML/YAML → Resource compiler + published JSON Schema.
  cdf-python/          # Tier 2: PyO3 embedding, PyCapsule / C Data Interface bridge. Optional.
  cdf-wasm/            # Tier 3: wasmtime host, WASI 0.3 WIT bindings. Optional, post-MVP.
  cdf-subprocess/      # Tier 4: stdio adapters (Arrow IPC, NDJSON, Singer, Airbyte).
  cdf-dest-duckdb/     # Destination: DuckDB.
  cdf-dest-parquet/    # Destination: Parquet over fs / object_store.
  cdf-dest-postgres/   # Destination: Postgres.
  cdf-project/         # Layer 4: cdf.toml, environments, secrets wiring, lockfile.
  cdf-cli/             # Layer 4: the `cdf` binary.
  cdf-conformance/     # Test-only: resource & destination suites, chaos layer, golden packages.
```

## Chapter 6: Runtime model

### 6.1 Concurrency: two runtimes, on purpose

The intuition is an emergency room with one triage nurse: if the nurse also performs surgeries, triage stops during every operation and the waiting room dies of neglect. An async I/O runtime is the triage nurse — its reactor threads must always be available to notice sockets and timers — and vectorized analytics is surgery. The classic failure of marrying an analytics engine to Tokio is CPU-bound operators occupying reactor threads until network heartbeats miss, connections drop, and throughput collapses in ways profilers struggle to attribute.

CDF adopts the two-runtime pattern DataFusion's maintainers document for exactly this situation. An I/O runtime owns sockets, timers, object-store requests, and destination connections. A CPU pool owns decode, validation kernels, encode, and the engine's operator execution. Bounded channels carry streams between them, and the crossing is explicit in the code, never accidental. Blocking FFI — DuckDB's C API, the Python interpreter — is confined to `spawn_blocking` pools with configured ceilings, so a stalled destination or a slow Python generator can never exhaust the workers extraction depends on. On free-threaded Python (D-25), the Python pool genuinely parallelizes; on GIL builds it degrades gracefully to interleaved producers with the lock held only during production.

### 6.2 Backpressure: byte-bounded, end to end

Every hop in the pipeline is a bounded channel sized in **bytes** — via each batch's `bytes` field — not in messages, because message-count bounds are meaningless when one batch can be a kilobyte and the next a hundred megabytes. A slow destination therefore pressures the resource batch-by-batch, and well-behaved sources — paginated APIs, SQL cursors — simply pause fetching, which is free.

The theory register earns a sentence here: this is Little's law applied as design. Work-in-progress equals throughput times latency; unbounded queues do not add throughput, they add latency and hide it as memory growth until the process dies at the worst moment. Bounding WIP in bytes turns the pipeline into a system whose memory footprint is a configuration value rather than an emergent surprise, and turns "the destination is slow" into visible upstream pausing rather than invisible buffer bloat.

Resources that *cannot* pause — webhook drains, log tails — declare `backpressure: false` in their capability sheet, and the planner then requires a spill policy: overflow batches flush to the package directory early, trading disk for memory under a hard byte ceiling, after which the run fails cleanly with a `Data` error rather than degrading unpredictably. The failure is a policy decision made at plan time, not an accident discovered at 3 a.m.

### 6.3 Memory accounting: one ledger for memory, too

The system that keeps one ledger for state keeps one ledger for memory. DataFusion operators already account through its `MemoryPool`; every CDF component that buffers — the package builder's pending-segment queue, adapter decode buffers, the DuckDB appender staging area — registers against the same pool. The budget is a single project-level number with a sane default. Exceeding it triggers, in order: early segment flush, backpressure, spill, clean failure. `cdf run --explain-memory` prints who held what at peak, from the pool's own records. There is no second, informal memory story to reconcile, which is the point: two accounting systems is how organizations — and processes — go bankrupt without noticing.

### 6.4 Adaptive batch sizing: a control loop with a recorder

Batch size floats between a configured floor and ceiling (default 1k–64k rows, 1–32 MiB), seeded from resource estimates and adjusted by a deliberately simple feedback controller: starved downstream queues grow batches (amortizing per-batch overhead), spill events shrink them (respecting the memory ledger). The control-theory framing is worth keeping in view precisely to stay humble about it — this is a low-gain proportional controller with hysteresis, chosen over anything cleverer because oscillating batch sizes are worse than suboptimal ones, and because the recorder makes optimality a secondary concern: every produced batch's actual size is recorded in the package, and **replay uses recorded batches, never re-derives them.** Adaptive live behavior and deterministic evidence coexist because the adaptation is journaled (this is I3 of the calculus, wearing overalls).

### 6.5 Bounded and unbounded streams

Every plan node carries `Boundedness`, the same distinction DataFusion draws. Bounded plans run to completion and produce one or more packages. Unbounded plans are legal only with three policies fixed at plan time — a checkpoint cadence (`every N batches | every T seconds | on watermark advance`), a package rotation rule, and a late-data watermark policy — and the planner refuses otherwise, because an unbounded stream without a cadence is a promise to never pass the commit gate, and an unbounded stream without rotation is a package that never finishes compacting.

The first executor accepts bounded plans, and unbounded plans in *drain mode*: run until quiescent or until `--max-duration`, then close the window, finish the package, settle, and gate. Drain mode is not a stopgap; it is the honest shape of micro-batch CDC and queue draining, and many "streaming" workloads are drain-mode workloads wearing a costume. The resident streaming supervisor (§25.3) is a later scheduler over unchanged types — the payoff of carrying `Boundedness` from the first commit is that streaming changes *when* plans run, never *what* they are.

### 6.6 Watermarks, stated plainly

A watermark is a claim about event time: "no further data with event time earlier than W is expected." The dataflow literature made watermarks the standard resolution of the tension between latency and completeness — emit results early and revise, or wait for the watermark and emit once — and CDF adopts the vocabulary without adopting a windowing engine: batches may carry event-time low/high watermarks (§10), unbounded plans must declare how late data is treated (recapture into the next window, quarantine, or admit-with-annotation), and the committed cursor's window-close arithmetic (I5) is a watermark statement about *processing* completeness. The full windowing/triggering machinery of stream processors is out of scope on purpose; what CDF needs from the theory is exactly the discipline of never confusing "we have seen no data past W" with "no data past W exists," and the lag tolerance of `CursorSpec` is that discipline priced into every incremental plan.

## Chapter 7: The type system


### 7.1 Arrow is the type system

CDF invents no type lattice. The logical type system is Arrow's, closed, with three annotations carried in `Field` metadata:

```text
cdf:semantic     optional tag: json | uuid | url | currency:<code> | pii:<class> | ...
cdf:source_name  the source's identifier, verbatim, forever
cdf:null_origin  declared | inferred | widened   — why is this field nullable?
```

The intuition for why annotations ride in metadata rather than in wrapper types: a shipping label changes how a parcel is *handled* — fragile, refrigerated, customs-declared — without changing what the parcel *is*, and every conveyor in the warehouse keeps working on labeled parcels unmodified. Semantic tags never change physical execution; they change policy. A `pii:email` tag arms redaction in previews and quarantine artifacts; a `json` tag steers destinations toward their native JSON type; a `currency` tag arms a contract rule. Every DataFusion kernel keeps working untouched, and the annotations ride along for free — the design pays zero execution cost for its semantics.

### 7.2 The fidelity rules

Two source-to-Arrow rules are non-negotiable, because violating them is the signature silent corruption of this product category, and both deserve their doctorate register.

**Decimals stay decimals.** Source `NUMERIC(p, s)` becomes Arrow `Decimal128/256(p, s)`, never `Float64`. The high-school version: floating point cannot represent most decimal fractions exactly — one-tenth in binary is a repeating fraction, the way one-third is in decimal — so financial values routed through floats acquire errors of a few parts in ten quadrillion, which sounds ignorable until millions of them are summed, compared for equality, or grouped by amount, at which point ledgers stop balancing by amounts auditors notice. The deeper version: IEEE 754 doubles carry 53 significand bits, exactly representing integers only to 2^53 and decimal fractions almost never; scaled-integer decimals represent every value in their (precision, scale) domain exactly and make addition associative again, which floating point's rounding breaks. A resource that cannot produce decimals must declare it in its sheet, and the planner surfaces the coercion before the run — the corruption is not merely avoided but made *visible* where it cannot be avoided.

**Timestamps keep their zone story.** Zoned timestamps become `Timestamp(unit, Some("UTC"))` — an unambiguous instant on the global timeline — with the original zone preserved in metadata where it carries meaning (a business's "local close of day" is information, not noise). Naive timestamps — wall-clock readings with no zone — become `Timestamp(unit, None)` and are never silently assumed UTC, because a naive timestamp is not an instant at all: it is a *pattern* that occurs once per timezone per day, twice during daylight-saving fallbacks, and never during spring-forward gaps. The contract rule `preserve_timestamp_timezone` converts a naive-timestamp source from a quarterly incident into a plan-time error, which is the entire fidelity philosophy in one rule: where meaning can be silently lost, make the loss impossible, plan-visible, or contract-gated — and say which.

### 7.3 Destination type mapping is data, not code

Every destination ships a capability sheet (§14.2) whose core is a declared mapping table: Arrow type → destination type → fidelity (`exact | widening | lossy | unsupported`). The planner joins the resource's output schema against the sheet before execution; `lossy` requires the explicit contract allowance `allow_lossy_mapping`, and `unsupported` fails the plan. The sheet is snapshotted into the lockfile, so a driver upgrade that changes a mapping is a reviewable Git diff, not a production surprise. The intuition is a customs declaration: what may cross the border, in what form, with what transformation, is written down, stamped, and checked at the border — never discovered by opening crates on the far side.

### 7.4 Identifiers

Names are the most under-designed corner of most ingestion tools, and the failure modes are all silent: two source fields that fold to one destination column, a renamed column that re-keys every downstream query, a case-folding warehouse that disagrees with a case-preserving one about which table exists. CDF treats identifiers as versioned data with four rules.

The source's exact identifier lives in `cdf:source_name` permanently — normalization never destroys the original. A versioned normalizer — `namecase-v1`: Unicode NFC normalization, then lower snake_case, then the destination's charset filter, then length truncation with an 8-hex-character hash suffix on truncation or collision — derives destination identifiers deterministically; the NFC step exists because Unicode permits visually identical strings with different code-point sequences, and canonicalization before comparison is the only correct order of operations. Collisions after normalization (`userName` and `user_name` in one table) are plan-time hard errors with a rename hint, never last-writer-wins. And the normalizer version is recorded in every schema snapshot and package: bumping it is a breaking change that `cdf diff schema` surfaces, because renaming destination columns re-keys everything downstream — the normalizer is part of the data's identity, and identity functions do not get silent patches. Case-folding destinations (upper-folding warehouses, lower-folding Postgres) are handled by the destination sheet's identifier rules, never by resource authors.

### 7.5 Nested data and variants

Arrow `Struct`, `List`, and `Map` are first-class end to end; nothing forces flattening, because flattening is a policy, not a law of nature. A resource's normalization policy chooses among three treatments. **Keep nested** — the default where the destination supports it, increasingly the right answer as warehouses grow native semi-structured types. **Child-table expansion** — the dlt-style relational unnesting, with deterministic child names via the normalizer, parent keys propagated, and load order recorded in the package so replays reconstruct referential integrity in the right sequence. **Variant capture** — unknown or contract-violating substructure lands in a `_cdf_variant` column tagged `json`, so discovery-phase resources never drop data they cannot yet type; the variant column is the type system's honest "I don't know yet," and promoting a variant to typed columns later is a contract-evolution event, diffed and recorded like any other schema change. The three treatments compose per-path: a resource can keep `address` nested, expand `line_items` to a child table, and variant-capture `metadata`, and the package records which paths received which treatment.


## Chapter 8: The resource model

### 8.1 Two tiers, one identity

Forcing every author to think about projections and filters is the ergonomic failure that makes people flee query-engine-adjacent APIs, so the resource abstraction splits into a minimal contract everyone implements and an optional negotiation contract for resources that can profit from it.

```rust
/// Tier A — every resource. Implementable in an afternoon.
/// BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>  (kernel-owned, arrow-rs only)
trait ResourceStream: Send + Sync {
    fn descriptor(&self) -> &ResourceDescriptor;
    fn partitions(&self, cx: &PlanContext) -> Result<Vec<PartitionSpec>>;
    fn open(&self, part: &PartitionSpec, cx: &ExecContext) -> Result<BatchStream>;
}

/// Tier B — resources that can do less work when asked for less.
trait QueryableResource: ResourceStream {
    fn capabilities(&self) -> &ResourceCapabilities;
    fn negotiate(&self, req: &ScanRequest) -> Result<ScanPlan>;
    // ScanRequest: projection, classified filters, limit, checkpoint view.
    // ScanPlan: what the source will actually do — per-predicate fidelity,
    //           chosen partitioning, expected ordering, cost estimate if available.
}
```

Tier A resources still get planning — the engine wraps them with engine-side projection, filtering, and limits — and participate in contracts, packages, and checkpoints identically; nothing about correctness is gated on sophistication. Tier B resources become DataFusion `TableProvider`s through a generic adapter in `cdf-engine`, inheriting pushdown negotiation, `EXPLAIN`, and SQL access. The split also enforces a discipline structurally that DataFusion documents as convention: `negotiate` runs at plan time and must not perform I/O; `open` does the work. Plans stay cheap enough to produce constantly — for dry runs, CI validation, and diffs — because nothing in planning touches the network.

### 8.2 The descriptor

```rust
struct ResourceDescriptor {
    id: ResourceId,                     // "github.issues"
    schema: SchemaSource,               // Declared(SchemaRef) | Hints(Vec<Hint>) | Discover
    primary_key: Option<Vec<FieldPath>>,
    merge_key: Option<Vec<FieldPath>>,
    cursor: Option<CursorSpec>,
    write_disposition: WriteDisposition,
    contract: ContractRef,
    state_scope: StateScope,            // §13.4
    freshness: Option<FreshnessSpec>,   // expected cadence; arms staleness checks
    trust: TrustLevel,                  // experimental | governed | financial | serving
}
```

`CursorSpec` deserves its own section because it prevents the most common incremental-load bug in production systems.

### 8.3 Cursor theory: why naive max-value advancement is wrong

The intuition: an incremental cursor is a bookmark, and the naive algorithm — read everything past the bookmark, move the bookmark to the largest value seen — assumes the book's pages arrive in order. Real sources violate the assumption two ways. A source may return rows out of cursor order (most APIs make no ordering promise), so the largest value seen can arrive *before* smaller unseen values in the same response window. Worse, a source may *mutate rows behind the bookmark after you have passed it*: a record updated at 12:00:03 can be committed by the source's own infrastructure with an `updated_at` of 12:00:01 — replica lag, application clock skew, batched writes — after a run at 12:00:02 already advanced the cursor to 12:00:02. The naive algorithm skips that record forever, and nobody notices until reconciliation.

The mechanism: `CursorSpec` carries an `ordering` claim — `exact` (a SQL `ORDER BY` on the cursor is; most APIs are not) or `best_effort` — and a `lag` tolerance λ, the declared window in which the source may still produce or mutate rows behind the cursor. For `best_effort` ordering or nonzero lag, the planner uses window-close semantics: the committed output position advances to `max(observed) − λ`, never to the naive maximum (invariant I5). Rows inside the lag window are deliberately re-read by the next run, and the destination's merge disposition (with dedup) absorbs the overlap — at-least-once composed with idempotence, again, this time in miniature. The cost is a sliver of re-read per run; the purchase is that "silently skipped the late update" leaves the vocabulary. Ordering claims are conformance-tested (§20.1), because a claim of `exact` that is not exact re-opens the hole.

### 8.4 Capabilities are claims with consequences

```rust
struct ResourceCapabilities {
    projection: PushdownSupport,                    // None | Full | Fields(set)
    filters: Vec<(PredicateShape, FilterFidelity)>, // e.g. (RangeOn("updated_at"), Inexact)
    limit: PushdownSupport,
    ordering: Option<OrderingClaim>,
    partitioning: PartitioningClaim,                // None | Static(n) | Dynamic(strategy)
    incremental: Option<IncrementalClaim>,          // cursor | log_position | file_manifest
    replay_from_position: bool,
    idempotent_reads: bool,
    backpressure: bool,
    estimates: EstimateSupport,
}
```

The intuition is again the customs declaration: a sheet is a declared, machine-readable statement of what may cross and how, checked at the border. Two rules give it teeth. **Claims are tested:** the conformance suite generates scans exercising every claim and fails resources whose behavior contradicts their sheet — an `Exact` filter claim is fed adversarial values around timezone, collation, and null edges and compared against engine-side ground truth, because the only thing worse than no pushdown is wrong pushdown. **Claims are snapshotted:** the lockfile records each resource's capability sheet, so a connector update that silently loses a capability — a failure mode the Singer/Meltano variant ecosystem knows intimately — surfaces as a reviewable diff rather than as a performance regression six weeks later.

### 8.5 Pushdown theory and practice

The theory register, briefly, because it explains the three-valued fidelity rather than merely asserting it. Filter pushdown is sound only when the source's evaluation of a predicate is *contained* in the engine's: every row the engine would accept, the source must return. `Exact` claims equality of the two evaluations — the strongest claim, requiring bit-level agreement on collation, timezone arithmetic, null ordering, and floating-point comparison, which remote systems rarely provide. `Inexact` claims only containment: the source returns a superset (pruning whole pages, windows, files, or partitions using coarse statistics — the same logic as Parquet row-group pruning), and the engine re-applies the exact predicate to the survivors. Containment is easy to guarantee and captures most of the economic value: the expensive resource is transfer and remote work, and a superset that skips ninety percent of it is ninety percent of the win with none of the semantic risk. `Unsupported` is honesty. API resources default to `Inexact` for exactly this reason, and `cdf explain` renders the negotiation so pushdown stops being folklore:

```text
Scan: hubspot.contacts
  projection: [id, email, updated_at]          (pushed: API fields param)
  filter: updated_at >= '2026-07-01'           (pushed: Inexact — window pruning; re-checked)
  filter: email LIKE '%@acme.com'              (not pushed: Unsupported by source)
  limit: 1000                                  (pushed: page cap)
  partitions: 5 × 1-day windows
  estimated requests: 40    estimated rows: ~38k
  guarantee: effectively-once per key (merge on [id], destination supports package tokens)
```

The final line is the §14.5 derivation, printed with every plan, because a guarantee the operator has not seen is a guarantee that does not exist.

### 8.6 Partitioning

Partitions are the unit of parallelism now and the unit of distribution later (D-11), so the planner treats a resource's natural partitioning as a first-class claim rather than an implementation detail. Each source archetype has a natural grain — cursor windows and page-token ranges for APIs; primary-key ranges, physical partitions, and replication slots for databases; files, prefixes, and row groups for object stores; log streams and offset ranges for CDC — and `partitions()` exposes it. The completeness obligation is conformance-tested: the union of a resource's partitions must equal its unpartitioned scan, with no row belonging to two partitions, because a partitioning that overlaps double-loads and one that gaps silently drops. Partition specs are serialized into plans and packages, which is what makes a partition independently retryable (Chapter 15's retry unit), independently backfillable (`cdf backfill` compiles date ranges into partition sets), and — later — independently leasable to a remote worker.

### 8.7 Sources group resources; the resource stays primary

A `Source` is a named bundle: shared configuration and credentials, a discovery function (list tables, endpoints, streams) whose output is a set of `ResourceDescriptor`s, and defaults members inherit. Nothing in the runtime special-cases sources. The smallest stateful unit — the resource — remains the unit of state, planning, contracts, and conformance, deliberately, because every predecessor that blurred this line ended up with state it could not scope and failures it could not localize.

## Chapter 9: Authoring — how user code gets in

A Rust kernel with no story for non-Rust authorship is a niche tool; a kernel that embeds a scripting language badly is a slow tool with a miserable editor experience. The resolution is a graded ladder in which every tier is chosen for what it is uniquely good at, and the interchange between every tier and the kernel is identical: Arrow record batches plus a `ResourceDescriptor`. Whatever the tier, the moment data crosses into the kernel, the authoring language's involvement ends — Python, WASM, and subprocesses *produce*; CDF *executes*.

### 9.1 The ladder

| Tier | Form | Runs | Data crossing | Editor story | Ships |
|---|---|---|---|---|---|
| 0 | Declarative TOML/YAML | in-process, compiled to native resources | none | published JSON Schema → completion and validation in any editor | MVP |
| 1 | Rust | in-process, statically linked | none | rust-analyzer, full types | MVP |
| 2 | Python | embedded interpreter (PyO3) | Arrow PyCapsule / C Data Interface, zero-copy | fully typed `cdf-sdk`; pyright-clean is a release gate | MVP |
| 3 | WASM Component (WASI 0.3) | wasmtime, sandboxed | native `stream<u8>` of Arrow IPC (one copy) | language-native tooling + published WIT world | post-MVP |
| 4 | Subprocess | external process over stdio | Arrow IPC (preferred) / NDJSON / Singer / Airbyte | whatever the foreign tool has | MVP (IPC + NDJSON); Singer/Airbyte fast-follow |

### 9.2 Tier 0 — declarative resources

Most ingestion is not novel logic; it is a REST endpoint with pagination and auth, a SQL table with a cursor column, or a glob of files. The field proved this from two directions at once — dlt's declarative REST source and Sling's replication YAML — and the LLM era added a third argument: a constrained, schema-validated format is the safest artifact to let an agent write, and agents are now writing thousands of them. Tier 0 makes the declarative layer primary rather than a convenience:

```toml
# resources/github.toml
[source.github]
kind = "rest"
base_url = "https://api.github.com"
auth = { kind = "bearer", token = "secret://env/GITHUB_TOKEN" }
rate_limit = { requests_per_minute = 300, respect_headers = ["Retry-After", "X-RateLimit-Reset"] }

[resource.issues]
path = "/repos/{owner}/{repo}/issues"
params = { state = "all", per_page = 100 }
paginate = { kind = "link_header" }            # or cursor_param | page_number | offset | next_token
records = "$"                                   # JSONPath to the record array
primary_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "5m" }
write_disposition = "merge"
contract = "governed"
partition = { by = "cursor_window", width = "7d" }
```

`cdf-declarative` compiles this into real `QueryableResource` implementations backed by `cdf-http`, which is why declarative resources get pushdown (the `since` param *is* the cursor filter, fidelity `Inexact`), partitioning, retries, and rate limiting without their authors learning those words. `kind = "sql"` and `kind = "files"` receive equivalent treatment: a SQL resource's cursor column becomes a real `WHERE` pushdown at fidelity `Exact` when the dialect permits, and a files resource's prefix layout becomes partition pruning. The JSON Schema for the format ships with every release and registers with SchemaStore, so VS Code, JetBrains, Zed, and Neovim validate and complete it with zero setup; `cdf validate` runs the same schema plus semantic probes — does the cursor field exist in a sample response? does the paginator's shape match the API's? — in CI.

The escape-hatch gradient matters as much as the format. A declarative resource can name a Python or Rust function for exactly the fragment that resists declaration —

```toml
records_transform = "python://./src/gh.py#flatten_reactions"
```

— so outgrowing Tier 0 costs one function, not a rewrite. The gradient is the tier system's whole theory of adoption: every step up the ladder is incremental, and no step abandons the artifacts below it.

### 9.3 Tier 1 — Rust

Rust has no stable ABI, so dynamically loaded `.so` connectors are a trap this design refuses: crash-prone, toolchain-version-locked, and solved better by Tier 3, where sandboxing repays the boundary cost. Rust authorship is therefore static — CDF is a library, and Rust users own a binary:

```rust
use cdf::prelude::*;

#[derive(Resource)]
#[resource(id = "github.issues", primary_key = "id", cursor = "updated_at",
           disposition = "merge", contract = "governed")]
struct GithubIssues { client: GithubClient }

impl ResourceStream for GithubIssues {
    fn partitions(&self, cx: &PlanContext) -> Result<Vec<PartitionSpec>> {
        cx.cursor_windows(Duration::days(7))
    }
    fn open(&self, p: &PartitionSpec, cx: &ExecContext) -> Result<BatchStream> {
        self.client.issues_since(p.window()).into_batch_stream(cx)
    }
}

fn main() -> Result<()> {
    cdf::project::main()   // this binary now speaks the full `cdf` CLI over local resources
}
```

Teams that want Rust resources without owning a binary get `cdf build`: a project-local `extensions/` crate compiled against a pinned toolchain and linked into a project-specific runner. Either way, linkage is static, and the "plugin" boundary for third-party distribution is Tier 3.

### 9.4 Tier 2 — Python, done honestly

Python earns embedding for three reasons no alternative matches. The existing ingestion-authorship population lives there — every dlt user, every Singer tap author. The client-library long tail — Salesforce, NetSuite, SAP wrappers, and ten thousand smaller SDKs — lives there. And Arrow interchange with Python is a solved, standardized, zero-copy problem: the Arrow PyCapsule Interface (`__arrow_c_schema__`, `__arrow_c_array__`, `__arrow_c_stream__`) means any Arrow-producing Python object — pyarrow, pandas with Arrow dtypes, polars, DuckDB results, nanoarrow — crosses into arrow-rs structures by transferring capsule-wrapped C structs whose buffers are reference-counted across the boundary, not by serializing. The mechanism, one level deeper for the reader who will implement it: the producer exports an `ArrowArrayStream` struct wrapped in a named PyCapsule with a destructor; the consumer moves the struct, nulls the release callback, and now owns buffers that Rust reads in place; the capsule's name and destructor exist precisely so that a mistaken consumer fails loudly instead of reading freed memory. A subtle consequence worth stating plainly: **pyarrow is not a hard dependency of `cdf-sdk`.** Authors who yield plain dicts need no Arrow library at all; authors who yield frames bring whichever library produced them, and CDF speaks the protocol, not the package.

```python
# src/github.py — authored against `cdf-sdk` (types + stubs; pyarrow optional)
import cdf

@cdf.resource(primary_key="id", cursor="updated_at",
              write_disposition="merge", contract="governed")
def issues(ctx: cdf.Context):
    """Yield dicts, or any object speaking the Arrow PyCapsule protocol."""
    for page in ctx.http.paginate("/repos/acme/app/issues",
                                  params={"since": ctx.cursor.last}):
        yield page.json()
```

Execution rules keep this from quietly becoming "Python is the runtime":

- `cdf-python` embeds one interpreter per run; resource generators are driven on the dedicated Python pool. On GIL builds, the lock is held only while user code produces the next chunk — conversion and everything downstream is lock-free Rust. On free-threaded 3.14t (D-25), the pool genuinely parallelizes across resources and across partitions of `parallel`-marked resources; semantics are identical on both builds, and CI tests both.
- Yielded dicts batch through the same inference path as NDJSON; yielded PyCapsule-speaking objects cross zero-copy.
- One Python resource cannot stall the world: per-resource watchdogs and byte-bounded channels apply at the boundary like everywhere else.
- `ctx` exposes *CDF's* HTTP toolkit, secrets, cursor view, and logger, so Python authors inherit rate limiting, retries, and redaction instead of importing an HTTP library and re-inventing failure.
- Environment resolution is boring by design: `cdf.toml` pins the interpreter (a venv or uv-managed environment); `cdf doctor` verifies importability, stub versions, and whether the interpreter is free-threaded. No bundled-interpreter magic.
- **The editor story is a shipping requirement, not a hope:** `cdf-sdk` is `py.typed` with complete stubs for `Context`, the decorators, and descriptor kwargs; CI runs pyright over the SDK examples, and a type-checking regression blocks release. An SDK that autocompletes wrong is worse than no SDK.

### 9.5 Tier 3 — WASM Components on WASI 0.3

The distribution problem — running a stranger's connector without trusting it — is what the sandbox is for. CDF's WIT world targets the WASI 0.3 baseline, where async is native to the Component Model's canonical ABI:

```wit
package cdf:resource@0.1.0;

world resource {
    import cdf:host/http;        // host-mediated: rate limits, redaction, egress allowlists apply
    import cdf:host/secrets;     // resolve-by-reference only
    import cdf:host/log;

    export describe: func() -> descriptor;
    export negotiate: func(req: scan-request) -> scan-plan;
    export open: async func(part: partition) -> stream<u8>;   // Arrow IPC stream bytes
}
```

Batches cross as Arrow IPC through a native component `stream<u8>`: one copy per batch through linear memory, host-scheduled on the single event loop the runtime now owns across all components, with cancellation semantics arriving on the 0.3.x train. One copy per batch is a real cost and a fair price: the sandbox terms are the point. No ambient filesystem, no direct sockets; HTTP only through the host import, where CDF's limiter, redaction, and per-source egress allowlists bind strangers' code exactly as they bind CDF's own; secrets resolvable only by reference, so a malicious component can *use* a credential through the host but never *read* it. Guest SDKs begin with Rust; Python follows as componentize-py's 0.3 support matures. The conformance suite runs against components exactly as against native resources, and passing it is the future registry's admission gate (§25.4). The tier is deliberately post-MVP: sandboxed distribution matters only after there is something worth distributing.

### 9.6 Tier 4 — subprocess adapters

The polyglot escape hatch and the compatibility bridge:

```text
any process
  → stdout: Arrow IPC stream    (preferred: schema-exact, fast)
          | NDJSON              (easy: inference + hints)
          | Singer messages     (SCHEMA/RECORD/STATE → descriptor/batches/StateDelta)
          | Airbyte protocol    (catalog + per-stream state honored)
  → cdf-subprocess adapter → ordinary batches → ordinary runtime
```

Adapters translate foreign state into the typed checkpoint model: a Singer `STATE` blob becomes an opaque-but-versioned `ForeignState` delta scoped to the adapter resource and committed under the same gate as native state — honesty about opacity beats pretended structure. Subprocesses are supervised: startup and idle timeouts, stderr captured into the run trace, exit codes mapped onto Chapter 15's taxonomy. Arrow IPC and NDJSON ship first; Singer and Airbyte follow immediately, being parsers over the same machinery.

### 9.7 What the authoring design rejected

Lua and every embedded-scripting-language-as-centerpiece proposal: no data ecosystem, no Arrow story, no typed editor experience — embedding one would make the worst tier the featured one. Dynamic Rust plugins: no stable ABI, and Tier 3 covers dynamic distribution with sandboxing besides. R and notebook runtimes: reachable through Tier 4 now and Tier 3 later; not kernel concerns. A bespoke DSL: Tier 0 is TOML/YAML with a published JSON Schema precisely so nobody builds or learns a new parser, language server, or formatter — the boring format *is* the feature.

## Chapter 10: Batches

### 10.1 The unit of data

The unit is the batch, and a batch is more than its Arrow payload: it is the payload plus the identity and provenance that evidence and replay require.

```rust
struct Batch {
    id: BatchId,                        // ULID; sortable, unique per run
    resource: ResourceId,
    partition: PartitionId,
    schema_hash: SchemaHash,            // hash of the *observed* schema, metadata included
    payload: Payload,                   // Arrow(RecordBatch) | IpcSegment | ParquetRef | Opaque(FileRef)
    rows: u64,
    bytes: u64,
    source_position: Option<SourcePosition>,     // page token, LSN, file+offset, query bounds
    watermarks: Option<(Watermark, Watermark)>,  // event-time low/high where applicable
    stats: BatchStats,                  // per-column min/max/null_count/distinct-estimate
    op: Option<OpColumn>,               // presence of _cdf_op for CDC-shaped batches
}
```

Each field earns its place. `schema_hash` is per-batch, not per-run, because mid-stream drift is real and contracts must catch it at the batch where it happens, not at the run's autopsy. `source_position` makes packages replayable *against the source* for verification, not merely from stored bytes. `stats` are computed once on the hot path with vectorized kernels and spent three times — by the profiler, by the package manifest, and by destinations that can prune (a merge that knows a batch's key range skips scanning outside it). `Opaque` payloads let file-shuttling resources — move these objects, checksum them, record them — live inside the same evidence-and-checkpoint discipline without pretending to be columnar.

### 10.2 Why columnar, from first principles

The high-school register: imagine a spreadsheet stored as a stack of index cards, one card per row. To sum one column, you touch every card and read one cell from each — almost everything you pick up is waste. Now store the spreadsheet as one long strip per column: summing a column reads one strip, start to finish, touching nothing else. That is row versus columnar layout, and data movement's workloads — validate a column, profile a column, cast a column, filter on a column — are all strip-shaped.

The doctorate register: columnar layout converts the workload into long, contiguous, type-homogeneous scans, which is precisely the shape modern hardware rewards. Cache lines arrive full of useful values instead of one value and padding; hardware prefetchers, which predict sequential access, stay engaged; SIMD units apply one instruction to many packed values per cycle; and branch-free kernels over validity bitmaps replace per-row null checks with bitwise operations. Arrow additionally standardizes the layout across process and language boundaries, which is what makes the PyCapsule crossing of §9.4 zero-copy and the IPC format of Chapter 13 a memory-map rather than a parse. Rows never exist as a runtime concept in CDF: row-shaped authoring converts to batches at the boundary, per-row logic inside the engine is a vectorized kernel over batches, and the one place a row survives is the quarantine artifact, where a single row is the natural grain of an error report.


## Chapter 11: Contracts

### 11.1 The lineage: design by contract, for data

The phrase "contract" is borrowed deliberately from design-by-contract, the software methodology in which components declare preconditions, postconditions, and invariants that are *checked*, not merely documented. The methodology's insight was that an interface without enforced obligations is a hope, and hopes do not compose. Data engineering's schema documents are exactly such hopes: a wiki page says `amount` is a non-null decimal, the source starts sending strings, and the wiki page does not stop it. A CDF contract is the enforced version — a precondition on every batch admitted past the extraction boundary, with a defined verdict for every violation — and, like design-by-contract's checks, it is compiled, not interpreted from configuration at each row.

### 11.2 A contract is a compiled program

`cdf-contract` compiles a policy plus an observed schema into a **validation program**: an ordered set of vectorized checks and coercions with a total verdict lattice. The same compiled program is what `cdf explain` prints, what `ContractExec` runs, and what the package serializes — one answer, in three views, to the question "what did the contract do to this run?"

```text
ContractPolicy × ObservedSchema → ValidationProgram
  ├─ schema verdicts, per new-table / new-column / type-change:
  │     admit | admit_as_variant | quarantine | reject_batch | reject_run
  ├─ column programs: casts, decimal/tz enforcement, domain checks,
  │     nullability checks, dedup keys
  └─ row disposition: accept | quarantine(rule_id) | fail
```

"Total" is a load-bearing word, and it is property-tested (§20.4): every cell of every batch receives exactly one disposition. There is no configuration of policy and data for which the program shrugs. Totality is what allows the calculus to treat verdicts V as part of the transition tuple — an undefined verdict would be an undefined transition.

### 11.3 The operator chain

```text
ResourceBatchStream
  → SchemaFingerprintExec     # hash observed schema; catch drift at the batch it occurs
  → ContractExec              # run the ValidationProgram; split accept / quarantine
  → NormalizeExec             # namecase-v1, nested policy, variant capture, _cdf columns
  → ProfileExec               # stats aggregation, freshness, PK-uniqueness sketch
  → LineageExec               # stamp resource, partition, position, run, package
  → PackageSink               # segment into the package; propose state deltas
```

`ContractExec` and `NormalizeExec` are DataFusion `ExecutionPlan`s over the accepted stream; the quarantine side channel is framework-owned per D-3, flowing into `quarantine/part-*.parquet` with `(row, rule_id, error_code, source_position, observed_value_redacted)`. Redaction obeys semantic tags: a `pii:*` field's offending value is hashed in the artifact, never stored — the quarantine record proves *that* and *where* a value failed without republishing *what* it was.

### 11.4 Policy vocabulary and trust presets

```text
schema:   allow_new_table · allow_new_column · allow_type_widening · quarantine_type_narrowing
          allow_unknown_fields · variant_capture · freeze
types:    coerce_types · preserve_decimal_exactness · preserve_timestamp_timezone · allow_lossy_mapping
rows:     nullability · domain/enum · range · regex · freshness · dedup(keys, keep = first|last|fail)
verdicts: admit · admit_as_variant · quarantine · reject_batch · reject_run
```

`reject_run` exists, and the compiler warns when it guards low-severity rules: a run-fatal contract on a wide table converts one bad cell into an outage, so verdict severity is part of the policy on purpose — blast radius is a design input, not an accident.

The full vocabulary compiles from one declared field, `trust`, whose four values expand into complete policies users override piecemeal:

```text
experimental:  evolve everything, variant-capture unknowns, sampled profiling, quarantine off
governed:      evolve columns with a review artifact, full validation, quarantine on, packages retained
financial:     frozen schema, decimal/tz enforcement mandatory, full lineage, receipts required,
               reconciliation counts recorded, long retention
serving:       frozen schema, freshness SLO armed, sampled fast path after N clean runs,
               demote-on-anomaly
```

### 11.5 Promotion and demotion: deployment rings for data

Continuous delivery long ago stopped treating every deploy identically: a change proves itself in a canary ring before general rollout, and a regression demotes it automatically. CDF applies the same ring discipline to *validation depth*. A resource's effective depth moves along a recorded ladder: new resources run discovery-depth regardless of declared trust — everything is a canary at first contact; after N consecutive clean runs on a stable schema hash, validation drops to the sampled fast path where trust permits — the promotion; any drift, anomaly spike, or quarantine event demotes back to full depth — the automatic rollback. Every transition is a ledger event, so "why did last night's run get slow" has a queryable answer: it demoted, and here is the batch that caused it. The ring model resolves the tension that sinks most validation systems — full checking is too expensive to run forever, and no checking is too dangerous to run ever — by making depth a *earned, revocable status* rather than a constant.

### 11.6 Transforms, bounded

In-flight transforms are those expressible per batch with a plan-time schema: rename, cast, redact, derive via DataFusion expressions or registered UDFs, filter, expand nested structures per policy. They run inside `NormalizeExec` and are recorded like every other decision. Anything needing cross-resource joins, whole-table state, or post-load modeling belongs to the transformation tool downstream; the project layer triggers dbt or SQLMesh after a load and hands it the receipt — integration, not absorption. The boundary is principled, not timid: a transform inside the load path must be replayable from the package alone (I3), and cross-table state breaks that property, so it lives where full-table state is native.

## Chapter 12: Load packages — the build artifact of data

### 12.1 What a package is

Continuous delivery's deepest structural idea is the immutable artifact: build once, promote everywhere, and never rebuild per environment — because a rebuild is a chance to differ, and promotion of a verified artifact is not. A load package is that idea for data: the durable, hash-addressed record of one attempted state transition for one resource-partition set — the data, the decisions, and the proofs. Staging is a side effect; evidence is the identity. Everything the run planned, observed, decided, wrote, and was told by the destination lives in the package or is referenced by it, and a package that never passes the gate is discarded or replayed without consequence — an unpromoted artifact, not a corrupted state.

```text
pkg_01J.../                        # directory name = manifest-hash prefix + ULID
  manifest.json                    # identity, file hashes, counts, lifecycle status, signature slot
  plan/
    resource_plan.json             # negotiated ScanPlan, per-predicate fidelity included
    execution_plan.txt             # DataFusion EXPLAIN (physical), verbatim
    validation_program.json        # the compiled contract (§11.2)
  schema/
    observed.arrow.json            # observed schema(s), metadata included, per schema_hash
    output.arrow.json              # post-normalization schema
    diff.json                      # vs. last committed snapshot
  data/
    seg-000001.arrow               # Arrow IPC file, LZ4-framed
    seg-000002.arrow
  quarantine/
    part-000001.parquet
  stats/
    profile.parquet                # per column, per segment
    quality.parquet                # rule outcomes and counts
  lineage/
    batches.parquet                # batch ↔ partition ↔ source_position ↔ segment
  state/
    input_checkpoint.json
    proposed_delta.json
  destination/
    commit_plan.json               # disposition, target, migration DDL, idempotency token
    receipts.json                  # appended as receipts arrive
  trace.jsonl                      # structured events, secrets pre-redacted
```

### 12.2 Format decisions, elaborated

Arrow IPC for `data/`, because a package must round-trip the *exact* observed schema — dictionary encodings, field metadata, extension annotations — and Parquet's type system is a lossy projection of Arrow's; evidence that subtly re-types itself is not evidence. Parquet for `stats/`, `quarantine/`, and `lineage/`, because those are analytical artifacts queried across many packages — `cdf sql` over a glob of packages is a supported and encouraged pattern. Manifests and receipts are canonical JSON, human-readable on purpose: the artifact an operator opens at 3 a.m. should not require a decoder. `cdf package archive` transcodes `data/` to Parquet with a fidelity report for cold storage and lake interchange; the manifest records both forms and their hashes, and replay prefers IPC when present. The IPC choice carries the maintenance obligation named in D-28: arrow-rs majors land quarterly, so IPC byte-stability across each pinned upgrade is a golden-suite release gate, and D-4's revisit trigger swaps the canonical and archive roles if forward-compatibility ever bites in practice.

### 12.3 Lifecycle and the crash matrix

```text
planned → extracting → validated → packaged → loading → loaded → committed → checkpointed → archived
```

`manifest.json` carries the status, updated by atomic rename-over; status plus the checkpoint ledger jointly determine crash behavior. The matrix is the case analysis of §4.4 written as an operations table, and it is normative:

| Crash point | Detection | Recovery |
|---|---|---|
| during extract/validate (pre-`packaged`) | manifest status < `packaged` | partial package is garbage unless the resource claims `replay_from_position`; `cdf resume` restarts the partition or resumes from the last durable segment + recorded position |
| after `packaged`, before any destination write | status `packaged`, no receipts | replay the package into the destination; no source contact needed |
| mid-load | partial receipts / idempotency token present at destination | transactional destinations roll back and replay; idempotent destinations re-drive remaining segments keyed by token |
| after destination commit, before checkpoint commit | receipt durable in `receipts.json` and/or `_cdf_loads`; ledger delta uncommitted | verify the receipt (§14.4), then commit the checkpoint — **this window is what the commit gate exists for; recovery is a pure ledger operation** |
| after checkpoint commit | ledger committed | nothing; the next run reads committed state |

The chaos layer (§20.2) kills the process at each boundary in CI, on every merge, forever. Recovery code that is only exercised by disasters is recovery code that fails during disasters.

### 12.4 Retention and GC

Evidence has a budget. Retention is policy per trust level and environment — `dev: last 5 runs`; `financial` in prod: 400 days — enforced by `cdf package gc`, which refuses to collect any package that is the sole proof of a committed checkpoint still inside its retention window, and which leaves a tombstone: manifest and hashes survive, data is deleted, and the ledger's referential integrity outlives the bytes. An audit three years later can no longer replay the data, but it can still prove exactly what existed, what it hashed to, and when it was collected — the difference between destroying records and archiving them.

### 12.5 Hashing: content addressing and its consequences

Every file is SHA-256'd; the manifest lists `(path, bytes, sha256)`; the manifest itself is canonicalized — JCS-style key ordering, so semantically identical JSON hashes identically — and hashed, and that hash is the package's identity everywhere: ledger, receipts, CLI, idempotency tokens. The structure is a shallow Merkle tree, and it inherits the Merkle properties that made content addressing the backbone of version control and artifact registries: identity is intrinsic (two packages with equal manifest hashes *are* the same evidence, wherever they sit); verification is local (any file's integrity checks against the manifest without trusting the transport); and equality is cheap (the golden-package suite of §20.3 compares one hash, not a tree walk). The manifest reserves `signature: null` with a defined signing input — the manifest hash — so post-MVP signing changes zero layouts, and the same slot becomes the trust surface when third-party connector distribution arrives: hashing delivers integrity now; signatures deliver attribution when strangers ship code.

## Chapter 13: Checkpoints and state — the release ledger

### 13.1 State is a ledger

State is an append-only ledger of typed transitions, never a mutable dict. The current cursor of a resource is a *view* — the latest committed transition — not a cell that gets overwritten.

The intuition is accounting, and the analogy is precise enough to reason with. Bookkeeping abandoned erasable balances centuries ago: a ledger records transactions, a balance is *derived* by summation, and corrections are new entries, never erasures — because the moment history is mutable, audit is impossible and fraud is trivial. The theory register names the same idea's modern software forms: write-ahead logging, where the durable truth is the log and tables are views over it, and event sourcing, where application state is a fold over an append-only event stream. All three traditions converge on the same purchases, and CDF collects each of them as an ordinary feature rather than a heroic one: rewind is moving a head pointer (`cdf state rewind`); audit is reading history (`cdf state history`); bisection — which run first wrote garbage? — is binary search over transitions; and concurrent readers are safe because committed entries never change under them.

```sql
-- cdf-state-sqlite, WAL mode. The shape is normative; the dialect is not.
CREATE TABLE checkpoints (
  checkpoint_id   TEXT PRIMARY KEY,           -- ULID
  pipeline_id     TEXT NOT NULL,
  resource_id     TEXT NOT NULL,
  scope           TEXT NOT NULL,              -- §13.4 serialized scope key
  state_version   INTEGER NOT NULL,           -- schema version of the position payloads
  parent_id       TEXT REFERENCES checkpoints(checkpoint_id),
  input_position  TEXT NOT NULL,              -- JSON, typed by state_version
  output_position TEXT NOT NULL,
  package_hash    TEXT NOT NULL,
  schema_hash     TEXT NOT NULL,
  receipt_id      TEXT,                       -- NULL only while status = 'proposed'
  status          TEXT NOT NULL CHECK (status IN ('proposed','committed','abandoned','rewound')),
  is_head         INTEGER NOT NULL DEFAULT 0, -- maintained inside the commit transaction
  created_at      TEXT NOT NULL,
  committed_at    TEXT
);
CREATE UNIQUE INDEX one_committed_head
  ON checkpoints(pipeline_id, resource_id, scope)
  WHERE is_head = 1;                          -- exactly one head per scope, DB-enforced
```

### 13.2 The commit gate, mechanized

`CheckpointStore::commit(checkpoint_id, receipt)` is the only path from `proposed` to `committed`, and it verifies *structurally* — not by convention — that the receipt covers the package hash and every segment the delta represents. There is no API for writing a committed checkpoint directly; the commit gate is a function signature before it is a discipline, which is invariant I1 made unforgeable by the type system. Rewind appends a `rewound` marker and moves the head; it never deletes history (I2), and it prints which committed packages now sit "ahead of state" so the operator understands the replay that follows.

### 13.3 Positions are typed

```text
CursorPosition  { field, value, window_close: Option<ts> }         # window-close per §8.3
LogPosition     { stream, lsn | offset, tx_boundary }
FileManifest    { prefix, files: [(path, etag | checksum, bytes)] } # object-store incrementals
PageToken       { opaque, issued_at }                               # honest about opacity, still scoped & versioned
Composite       { parts: BTreeMap<PartitionId, Position> }
ForeignState    { protocol: singer | airbyte, blob, blob_hash }     # adapter tier, quarantined type
```

`state_version` gates deserialization; migrating a position type is an explicit, tested path — `cdf state migrate`, with fixtures from every prior version in CI — because silently reinterpreting old positions is how backfills get skipped. `ForeignState` deserves a sentence of philosophy: when a foreign protocol's state is genuinely opaque, the honest design is to say so with a type, hash it, scope it, and commit it under the same gate, rather than to pretend structure the protocol never promised.

### 13.4 Scopes

A scope key namespaces state below the resource: `partition:<id>`, `window:<lo>..<hi>`, `file:<path>`, `stream:<name>`, `schema-contract`, `destination-load`. Different archetypes checkpoint at different grains — a file manifest per prefix, a window per cursor slice, one log position per stream — and forcing them all through one cursor string is the Singer mistake this vocabulary exists never to repeat. Scopes are also the concurrency unit: the store's contract is single-writer per scope, so two runs over disjoint scopes of one resource proceed independently, and the future distributed scheduler leases scopes, not resources.

### 13.5 The store trait

```rust
trait CheckpointStore: Send + Sync {
    fn head(&self, key: &ScopeKey) -> Result<Option<Committed>>;
    fn propose(&self, delta: StateDelta) -> Result<CheckpointId>;
    fn commit(&self, id: CheckpointId, receipt: &Receipt) -> Result<Committed>;
    fn abandon(&self, id: CheckpointId, reason: &str) -> Result<()>;
    fn history(&self, key: &ScopeKey, range: HistoryRange) -> Result<Vec<CheckpointRow>>;
    fn rewind(&self, key: &ScopeKey, to: CheckpointId) -> Result<RewindReport>;
}
```

SQLite (WAL) and in-memory ship first. The trait contract — single-writer per scope, atomic commit, monotone history — is itself conformance-tested, so future Postgres or object-store implementations (with lease fencing for distribution) prove themselves the way destinations do: by passing the suite, not by assurance.

### 13.6 Destination-mirrored state: double-entry for pipelines

Where the destination holds tables, the loader also maintains `_cdf_loads` — package hash, resource, schema hash, counts, receipt, committed-at — and `_cdf_state`, the latest committed position per scope as of the loads the destination has seen. The intuition is double-entry bookkeeping: every transition is recorded in two independent books — the local ledger and the destination's own tables — and the books must reconcile, because an error or an intrusion that touches only one book is *visible* by construction. Three payoffs follow. Disaster recovery: `cdf state recover --from-destination` reconstructs ledger heads from receipts when the local ledger is lost, with an explicit warning that quarantine and lineage evidence is not reconstructible. Warehouse-native audit: the query every dlt operator writes against `_dlt_loads` works here with more to say. Drift detection: a scheduled `cdf doctor` compares ledger heads to destination mirrors and flags divergence — the observable symptom of someone loading around the tool, which is the pipeline equivalent of cash that never hit the register.


## Chapter 14: Destinations

### 14.1 A destination is a commit protocol

```rust
trait Destination: Send + Sync {
    fn sheet(&self) -> &DestinationSheet;
    fn plan_commit(&self, pkg: &PackageView, target: &Target) -> Result<CommitPlan>;
    fn begin(&self, plan: &CommitPlan) -> Result<Box<dyn CommitSession>>;
}
trait CommitSession {
    fn migrate(&mut self, ddl: &MigrationPlan) -> Result<MigrationReceipt>;
    fn write(&mut self, seg: SegmentStream) -> Result<SegmentAck>;
    fn finalize(&mut self) -> Result<Receipt>;   // durable or error; no third state
    fn abort(&mut self) -> Result<()>;
}
```

`plan_commit` runs at plan time and is dry-runnable — it is where "this merge will need DDL: add column X" appears in `cdf explain`, before any run. `finalize` is settlement (§4.1): the moment of truth, returning either a durable receipt or an error, with no third state. The trait's shape makes the destination-acknowledged-state lesson unforgeable: the only way to hand CDF a receipt is to return one from `finalize`, and CDF will not open the commit gate without one.

### 14.2 Capability sheets

A `DestinationSheet` is declared data shipped with the driver and snapshotted into the lockfile: supported dispositions; transaction support (`full | per_table | none`); idempotency mechanism (`package_token | merge_keys | none`); bulk paths (`arrow_ipc | parquet | csv | insert`); the type-mapping table of §7.3; identifier rules — max length, charset, case behavior; migration support per operation (`yes | staged | no`); quarantine-table support; concurrency constraints. The planner consumes the sheet, the conformance suite falsifies it, `cdf explain` cites it. Nothing about a destination is folklore, and nothing about a destination changes without a lockfile diff.

### 14.3 Dispositions

The first release ships three, chosen because they span the semantic space without smuggling in modeling:

**`append`** adds rows, period — the simplest disposition and the only one that cannot be made replay-safe by keys alone, which is why its guarantee row depends entirely on package tokens.

**`replace`** swaps the target atomically where the destination allows — write-then-rename, or transactional truncate-insert — and never delete-then-insert, because a crash between delete and insert leaves an empty table wearing a production name.

**`merge`** upserts on the primary or merge key, with the batch dedup policy applied *first* so merges are deterministic under at-least-once redelivery: dedup-then-merge is a pure function of the package; merge-then-hope is not.

**`cdc_apply`** — ordered application of `_cdf_op` insert/update/delete by source position — ships with the first log-CDC source, since shipping the disposition before a source can exercise it would be an untested promise. Two dispositions are deliberately absent from the loader: `scd2` and `snapshot` are transformations wearing a disposition costume — they encode modeling decisions (validity intervals, history semantics) that belong downstream — and admitting them into the commit path would couple the gate to modeling opinion. `file_materialize` is the Parquet destination's native mode rather than a separate disposition.

### 14.4 Receipts

```json
{
  "receipt_id": "rcpt_01J...",
  "destination": "postgres",
  "target": "raw.github_issues",
  "package_hash": "sha256:9c1e...",
  "segments": [{"seg": "seg-000001", "rows": 60123, "sha256": "..."}],
  "disposition": "merge",
  "idempotency_token": "pkg:9c1e...",
  "txn": {"kind": "postgres", "xid": "12345678"},
  "counts": {"written": 120000, "updated": 3400, "deleted": 0},
  "schema_hash": "sha256:ab...",
  "migrations": [{"ddl": "ALTER TABLE ... ADD COLUMN reactions JSONB", "applied_at": "..."}],
  "committed_at": "2026-07-05T12:00:00Z",
  "verify": {"kind": "count_probe",
             "expr": "select count(*) from raw.github_issues where _cdf_load = 'pkg:9c1e...'"}
}
```

The `verify` clause is the receipt's teeth and the end-to-end argument applied (§4.4): every destination declares how a third party — or the crash-recovery path — confirms the receipt against the destination's own durable state. Transactional destinations declare a probe query; object stores declare a `(key, etag)` manifest; append-only systems declare count-plus-checksum. Recovery in the committed-but-not-checkpointed window runs `verify` before opening the gate, closing the loop between claim and reality — a receipt that cannot be independently verified is a press release, not a receipt.

### 14.5 Idempotency, replay, and the guarantee table

Every commit carries the package hash as its idempotency token. Destinations with `package_token` support record it — a `_cdf_load` column, a load-table row, an object key — and make re-driving the same package a no-op answered with a `duplicate: true` receipt (invariant I4). Destinations with only `merge_keys` idempotency achieve replay-safety for `merge` but not `append`, and the planner says so. `cdf replay package <pkg> --to <dest>` is therefore always safe when in doubt — the confluence result of §4.4 as an operator's calm — converting the scariest moment in ingestion, "did that load land?", into a command.

The guarantee table, derived mechanically from resource capabilities × disposition × destination sheet and printed by every `cdf plan`:

| Extraction | Disposition | Destination idempotency | Observed guarantee |
|---|---|---|---|
| at-least-once | merge (PK) | any | effectively-once per key |
| at-least-once | append | package_token | effectively-once per package |
| at-least-once | append | none | at-least-once; duplicates possible on replay — planner warns |
| at-least-once | replace | atomic swap | effectively-once per target |
| at-least-once | cdc_apply | package_token + ordered apply | effectively-once per position |

The unqualified phrase "exactly-once" appears nowhere in the product. The table is what is true instead, and Chapter 4 is why each row is true.

### 14.6 The first three destinations, with their honest edges

**DuckDB first**, because it closes the local loop: Arrow appender for hot batches, Parquet ingestion for replay, transactional commits, and instant `cdf sql` gratification against loaded data. Two honest edges go straight into its sheet rather than into a footnote. Single-writer database files: concurrent runs targeting one file serialize on a CDF-level lock rather than corrupting. And the bundled-build ICU gap: the Rust crate's bundled DuckDB omits the ICU extension for packaging-size reasons, which affects timezone-dependent operations — and a system whose type rules make timezone honesty non-negotiable (§7.2) cannot shrug at that. The driver either loads ICU at runtime or links a full build, and `cdf doctor` checks which. The crate's DuckDB-encoding version scheme and its LTS branch feed the D-28 pin.

**Parquet/object-store second**, because it exercises partition layout, manifests-as-receipts — `(key, etag, sha256)` lists — lake-style directory schemes, and is the archive path's twin. It is also the designed seam for the lakehouse destinations of D-27: Iceberg and Delta drivers arrive later through iceberg-rust and delta-rs, with the table format's own snapshot metadata embedded in the CDF receipt, so the commit gate and the table's transaction log corroborate each other — two ledgers agreeing beats one ledger trusted, which is §13.6's double-entry principle extended across systems.

**Postgres third**, the transactional reference: real DDL migration, real `ON CONFLICT` merge, xid-bearing receipts, and the same server exercising the source side, so incremental extraction and transactional loading are tested against one honest system. Warehouses — Snowflake, BigQuery, Databricks — arrive after MVP through the same sheet-plus-conformance gate as everything else; breadth is a consequence of the protocol being provable.

## Chapter 15: Errors, retries, and the HTTP toolkit

### 15.1 One taxonomy

Every tier and crate classifies failure identically, because retry policy attached to string-matched error messages is a bug generator:

```text
Transient    network flap, 5xx, lock timeout        → jittered backoff at request level
RateLimited  429, quota headers, driver throttle    → obey Retry-After / token bucket; never a failure
Auth         401/403, expired credential            → one refresh via SecretProvider, then fail actionably
Contract     validation verdicts                    → not errors; verdicts (ch. 11) — never retried
Data         malformed payloads beyond contract     → quarantine or fail per policy; never blind-retried
Destination  commit/migration failures              → retry only restartable sessions; else surface with receipt state
Internal     bugs                                   → fail loudly, full trace, no retry
```

The two "never" rows are the taxonomy's spine. Retrying a contract verdict cannot change it — the data is what it is — and retrying malformed data multiplies load without information. A taxonomy exists precisely to encode which failures are *weather* (retry through it) and which are *facts* (record and route).

### 15.2 Retry discipline and its theory

Retries operate at the smallest safe unit — request, then partition (re-planned when the resource claims `replay_from_position` or `idempotent_reads`), then run — under a run-level retry budget. The budget is the doctrine's theory register: uncoordinated retries are the classic amplifier of metastable failure, where a briefly degraded service is held underwater by the retry storms of its own clients, and each layer retrying independently multiplies load geometrically. Jittered exponential backoff de-synchronizes the herd; the run-level budget caps total amplification, so a dying API fails a run in minutes rather than grinding for hours; and idempotency tokens make the retries that do happen harmless (I4). A partition retry after partial packaging resumes from the last durable segment plus recorded position: the crash matrix and the retry path are the same code, which is the payoff of building recovery first — retry is just a very small crash.

### 15.3 The HTTP toolkit

`cdf-http` exists because every API resource in every framework dies on the same five rocks: pagination drift, rate limits, auth refresh, retry storms, and connection reuse. The toolkit provides paginators — cursor, page, offset, link-header, next-token — with response-shape auto-detection in the spirit the field has converged on, but always *recording which paginator it chose into the plan*, because auto-detection that leaves no trace is a debugging tax; a token-bucket limiter that respects server headers; backoff with budget accounting; session-scoped auth with refresh hooks into `SecretProvider`; and per-request tracing landing in `trace.jsonl` with secrets redacted before formatting. Declarative and Python tiers get it implicitly; Rust gets it as a crate; WASM guests get it as the host import — one implementation of correct HTTP behavior, four doors into it.

## Chapter 16: Observability

CDF's first observability surface is its own artifacts. The ledger is SQLite, the stats are Parquet, the receipts are JSON, and the engine is DataFusion, so the system's operational history is natively queryable:

```sql
select resource_id, sum(rows_written)
from cdf.loads
where committed_at > now() - interval '1 day'
group by 1
```

works out of the box under `cdf sql`. Introspection is a query, not a log grep, and the observability roadmap is mostly "add views," because the facts were designed as data from the start.

Above that, `tracing` instruments every phase with span fields for run, resource, partition, and package IDs; OTLP export is a feature flag, so a shop with an existing observability stack plugs CDF into it without adapters. Three derived signals ship as CLI verbs rather than dashboard suggestions: `cdf doctor` — environment health, secret resolvability, Python interpreter status including free-threading, DuckDB ICU presence, ledger↔destination drift; `cdf inspect run <id>` — the run's whole story: plan, verdicts, receipts, transitions; and `cdf status`, which evaluates `FreshnessSpec` SLOs and exits nonzero when a `serving`-trust resource is stale, making alerting a cron line rather than a platform.

## Chapter 17: Security

### 17.1 Secrets

`secret://provider/key` URIs are the only form a credential takes in any artifact CDF writes — project files, lockfiles, plans, packages, traces, EXPLAIN output, error messages. Resolution happens inside `SecretProvider` implementations (first release: env, file, OS keychain; later: Vault and cloud secret managers) at the moment of use, and resolved values live in zeroizing wrappers registered with the redaction layer, so a value leaking into a panic message is scrubbed *by construction* — the formatter consults the registry — rather than by pattern-matching after the fact. The design principle is that redaction-by-regex is a race the regex eventually loses; redaction-by-registration cannot lose, because the formatter never sees the plaintext outside the registry's control.

### 17.2 Trust boundaries by tier

The authoring ladder is also a trust ladder, and each rung's threat model is stated rather than implied. Tier 0/1 code is the operator's own; it runs with process privileges, and the threat model is *mistakes* — which plans, contracts, and the lockfile address. Tier 2 Python is trusted-but-instrumented: watchdogs, resource limits where the OS provides them, and the framework-brokered `ctx` as the encouraged path to the network. Tier 3 is the untrusted-code answer: capability-scoped WASI 0.3, host-mediated HTTP so limits, redaction, and egress allowlists bind strangers' code, secrets usable by reference but never readable, no ambient authority of any kind. Tier 4 inherits OS process isolation plus supervised stdio. The project file can declare an egress allowlist per source — enforced in `cdf-http` and the WASM host — turning "this connector only talks to api.github.com" from documentation into policy.

### 17.3 Supply chain

`cargo deny` and `cargo vet` gates in CI; lockfiles committed everywhere, including the Python SDK examples; release binaries built reproducibly and checksummed — a system whose product is reproducible evidence should be embarrassed to ship an unreproducible binary; and the package signature slot doubling as the connector-signing story when Tier 3 distribution arrives. The dependency-pin policy of D-28 is itself a supply-chain control: no load-bearing dependency moves in a patch release, ever.

---

# Part IV — The Surface

## Chapter 18: The CLI

Design rules: every command runs headless, exits with meaningful codes, and takes `--json`; every noun in the architecture — resource, capability sheet, plan, package, checkpoint, receipt, contract, schema, run — has an `inspect`; nothing requires a daemon.

```bash
cdf init                                          # scaffold project + one example resource per tier
cdf validate                                      # schema-check project & resources; CI's first line
cdf plan github.issues                            # compile; print plan, guarantee, DDL preview
cdf explain github.issues --where "updated_at >= '2026-07-01'" --columns id,title
cdf run github.issues --to duckdb://local.duckdb
cdf run --all --env prod
cdf preview github.issues --limit 500             # one batch: schema + sample, nothing written
cdf sql "select state, count(*) from github.issues group by 1"
cdf inspect resource|sheet|package|run|receipt <id>
cdf diff schema github.issues --against last
cdf contract freeze|show|test github.issues
cdf state show|history|rewind|migrate|recover github.issues
cdf resume                                        # drain interrupted work per the crash matrix
cdf replay package <pkg> --to <dest>              # idempotent by construction
cdf backfill github.issues --from 2026-01-01 --to 2026-07-01
cdf package ls|gc|verify                          # archive arrives fast-follow
cdf doctor                                        # env, secrets, python, ICU, ledger↔destination drift
cdf status                                        # freshness SLOs; nonzero exit on breach
```

Two commands carry the developer-experience thesis. `cdf preview` is the inspect-one-batch inner loop without a UI. `cdf plan` is §1.3's category shift made tangible — what will be fetched, what pushes down and at what fidelity, what DDL might run, which guarantee applies, which state advances — reviewable in a pull request before anything moves, exactly as infrastructure-as-code taught operations to expect.

## Chapter 19: The project format

### 19.1 Shape

```toml
# cdf.toml
[project]
name = "acme_data"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"
retention = { default = "5 runs" }

[environments.prod]
state = "sqlite:///var/lib/cdf/state.db"
packages = "s3://acme-packages/cdf"
destination = "postgres://secret://aws-sm/prod-dwh"
retention = { default = "90d", financial = "400d" }

[python]
interpreter = ".venv/bin/python"       # cdf doctor reports whether it is free-threaded

[defaults]
contract = "governed"

[resources."github.*"]
source = "resources/github.toml"

[resources."events.raw"]
source = "python://src/events.py#raw_events"
trust = "serving"
freshness = { expect_every = "15m", alert_after = "45m" }
```

TOML for the project file — comments, no indentation traps, the Rust ecosystem's grain; Tier-0 resource files accept TOML or YAML, since the declarative schema is format-agnostic JSON Schema underneath.

### 19.2 Environments and secrets

Environments overlay the project map; the unspecified inherits. The overlay model is the CI/CD promotion model applied to configuration: the same resources, the same contracts, the same code move from dev to prod; only bindings — state store, package store, destination, retention — change per environment, so what was tested is what ships. Secrets appear only as URIs, so the file is committable by construction, and `cdf validate --env prod` confirms resolvability without printing values.

### 19.3 The lockfile locks semantics, not just versions

`cdf.lock` snapshots what Git review should catch: resolved crate and SDK versions and the D-28 dependency tuple; each resource's capability-sheet hash; each destination's full sheet including its type-mapping table; contract snapshots and schema hashes per resource; the normalizer version. A dependency bump that changes a type mapping, drops a capability, or alters normalization becomes a reviewable diff before it becomes a production surprise. Ordinary lockfiles answer "what code will run?"; this one also answers "what will the code *mean*?" — and in a system whose failures are semantic (a silently narrowed type, a silently lost pushdown), the second question is the one that pages people.

## Chapter 20: Conformance and testing

### 20.1 The resource suite

Any resource — native, declarative, Python, WASM, subprocess — runs one suite: descriptor coherence (declared keys exist in the schema; the cursor field exists and orders as claimed); capability truth-telling (every claimed pushdown exercised against engine-side ground truth, `Exact` claims falsified with adversarial timezone, collation, and null edges); partition completeness (the union of partitions equals the unpartitioned scan, with no overlap); position replay (re-opening at a recorded position reproduces the suffix, for resources claiming it); boundedness honesty. Passing the suite *is* the definition of "supported" — the project attaches the word to no weaker evidence — and the suite is public, so third-party connectors prove themselves exactly as first-party ones do. The intuition is certification against a specification: an appliance is not "safe" because its manufacturer is nice; it is safe because a named test laboratory ran a named suite against it, and the mark is checkable.

### 20.2 The destination suite and the chaos layer

Sheet truth-telling: every declared disposition, migration operation, and idempotency mechanism exercised. Receipt verification: every `verify` clause shown to confirm honest loads *and to fail tampered ones* — a verifier that cannot fail is not a verifier. The crash matrix: the chaos layer kills the process at each lifecycle boundary; recovery must terminate in a consistent state with no cursor ahead of durable data. Replay identity: the same package driven twice yields `duplicate: true` or key-idempotent state, per the sheet. Type mappings round-trip at fidelity `exact`. The chaos layer's lineage is chaos engineering's central lesson — the only failure handling that works is failure handling that runs routinely — scaled down from infrastructure to process: every merge, every boundary, forever, in CI where it is cheap, so that production, where it is expensive, holds no first times.

### 20.3 Golden packages and determinism

Given a fixed source fixture, a run must produce a package whose manifest hash equals the committed golden hash — across a hundred repetitions and across operating systems. This is the reproducible-builds discipline imported whole: if the same inputs do not produce the same bytes, then either the inputs are not really the same (hidden state — find it) or the build is not really a function (nondeterminism — remove it), and both are bugs worth more than the feature that waits. The discipline forces the practices that make evidence trustworthy: canonical JSON everywhere; ULIDs seeded in test mode; adaptive batch sizes recorded, with replay using recordings; wall-clock timestamps confined to enumerated unhashed manifest fields. When a change legitimately alters output, the golden diff *is* the review artifact. The suite doubles as the D-28 upgrade gate: an arrow-rs or DataFusion pin bump that perturbs IPC bytes or plan text is caught here, before release.

### 20.4 Property and fuzz targets

Schema inference: arbitrary JSON → schema → generated data → re-inference is a fixed point. The contract compiler: the verdict lattice is total — every cell of every batch receives exactly one disposition, for every policy, checked by generation rather than by example. Position serialization round-trips across `state_version` migrations. The NDJSON, Singer, and Airbyte parsers under adversarial input. Property testing earns its place here for a structural reason: example-based tests check the cases someone imagined, and this system's risk lives precisely in the cases nobody imagined — generative testing is how a small team buys a large team's paranoia.

## Chapter 21: Compatibility

### 21.1 Posture

Compatibility layers are bridges into the native model, never alternate front doors. Everything arriving over a bridge becomes descriptors, batches, typed (or explicitly `ForeignState`) checkpoints, and packages, and is planned, evidenced, and committed like everything native. No bridge bypasses the commit gate.

### 21.2 The dlt shim

`cdf-python` ships an adapter running `@dlt.resource` and `@dlt.source` functions unmodified where feasible: dlt hints — primary key, merge key, `incremental`, write disposition, contract modes — map onto `ResourceDescriptor`; `dlt.current.state` maps onto a scoped state view backed by the ledger; dlt's contract-mode vocabulary (`evolve`, `freeze`, `discard_row`, `discard_value`) maps onto CDF verdicts (`admit`, `freeze`, `quarantine` at row and column grain). The shim's purpose is migration gravity — the largest library-first authorship base can try CDF with existing code and immediately gain plans, packages, and the guarantee table. Its non-goal is bug-for-bug dlt emulation; divergences are a documented migration table, and the deepest divergence is the point: dlt normalizes and loads what arrived, CDF plans what to fetch.

### 21.3 Singer and Airbyte adapters

Tier-4 parsers per §9.6. Airbyte's per-stream state and destination-acknowledgment expectations map cleanly — they inspired the invariant this system mechanizes. Singer's looser state becomes `ForeignState`, honestly scoped. Airbyte *destinations* are explicitly out of scope: the destination side is where the receipt protocol lives, and delegating it to a foreign container would forfeit exactly the guarantees CDF exists to provide.

## Chapter 22: Versioning, licensing, governance

Apache-2.0. One repository. Crates on crates.io under semver; before 1.0, minor releases may break Rust APIs but **may never break serialized artifacts without a migration**. The artifacts — checkpoint schema (`state_version`), package manifest version, capability-sheet format, the `cdf:resource` WIT world, the declarative JSON Schema — are independently versioned specs living in-repo with committed migration paths and CI fixtures from every prior version. The precedence is explicit and permanent: the artifacts are the contract with operators; the Rust API is merely the contract with developers; the former outranks the latter from the first release, because an operator's three-year-old package must open forever, while a developer's compile error is a morning's work. Upstream cadence is governed by D-28: one pinned dependency tuple per CDF minor, upgraded deliberately, gated on golden packages, never moved in a patch.


---

# Part V — The Road

## Chapter 23: MVP

### 23.1 Contents

Kernel, engine, contract compiler, package builder/replayer, SQLite ledger; authoring tiers 0 (REST/SQL/files), 1, 2, and 4 (Arrow IPC + NDJSON); `cdf-http`; destinations DuckDB, Parquet/object-store, Postgres; sources HTTP-paginated API, Postgres snapshot/incremental, Parquet/CSV/JSON files; dispositions append, replace, merge; the Chapter 18 CLI minus `package archive`; both conformance suites and the chaos layer; golden packages in CI; the dlt shim in preview.

### 23.2 The cutline

Explicitly out, each with a seam designed now so its later arrival is an addition rather than a rework: the WASM tier (the WIT world is already specified against WASI 0.3); Singer/Airbyte parsers (fast-follow over existing adapter machinery); log-based CDC and `cdc_apply` (positions already typed); the streaming supervisor (`Boundedness` already carried on every plan node); distributed execution (partitions and packages already the units; Ballista to be evaluated); warehouse and lakehouse destinations (the sheet-plus-conformance gate is the door; the Parquet destination is the Iceberg/Delta seam); package signing (slot reserved, signing input defined); non-SQLite ledger backends (trait sized, conformance-tested); vault-class secret providers; any UI. A cutline without seams is a debt schedule; a cutline with seams is a roadmap.

### 23.3 The demonstration

The system's proof is a single sitting, under five minutes on a laptop, no network beyond one public API. Forty lines of Tier-0 TOML define `github.issues`. `cdf plan` prints pushdown fidelity, the guarantee line, and pending DDL. `cdf run` loads DuckDB; `cdf sql` queries it with pushdown visible in EXPLAIN. `cdf contract freeze`; the fixture drifts a column type; the next run quarantines the offenders and the package shows the verdicts. `kill -9` lands between destination commit and checkpoint commit — the exact window the commit gate exists for; `cdf resume` verifies the receipt against DuckDB and commits the checkpoint without touching the source. `cdf replay` drives the same package into a second database; the second attempt against the first answers `duplicate: true`. `cdf state history` shows every transition. Each beat of the demonstration is one invariant of Chapter 4 performing in public.

## Chapter 24: Research spikes

Eight, with exit criteria that decide rather than describe:

1. **Queryable HTTP resource.** A paginated API as `QueryableResource` through the engine adapter. Exit: the §8.5 EXPLAIN output is real, and request counts drop measurably under projection + filter + limit versus a naive full scan.
2. **Contract compiler + ContractExec.** Exit: 100k-row batches validate at >1 GB/s for type/null/domain rules on a laptop; quarantine artifacts round-trip; the compiled program serializes into a package and reloads.
3. **Package determinism.** Exit: golden-hash stability across 100 runs and across macOS/Linux.
4. **Crash-matrix mechanization.** Exit: the chaos layer covers all five boundaries against DuckDB and Postgres with zero manual steps.
5. **Python bridge.** Exit: PyCapsule crossing measured zero-copy for batches ≥1 MiB; on a GIL build, lock held <5% of wall time on an I/O-bound resource; on 3.14t, two `parallel` resources demonstrate real concurrency with output hashes identical to the GIL run; pyright clean on the SDK examples. This spike is the D-25 proof.
6. **Adaptive batch controller.** Exit: throughput within 10% of hand-tuned fixed sizing across three source shapes, with recorded sizes replaying deterministically.
7. **Destination-sheet joins.** Exit: plan-time type-mapping and identifier checks catch seeded lossy and colliding fixtures on all three MVP destinations, including the DuckDB ICU/timezone case.
8. **Stress harness as tests.** The Appendix B matrix encoded as integration scenarios — high-variety/low-veracity: a thousand mutating JSON schemas through discovery trust; high-volume: the spill path under a 256 MiB budget. Exit: each scenario names the mechanisms that activated, from the ledger, automatically.

## Chapter 25: Beyond MVP

### 25.1 Fast-follow

Singer/Airbyte adapters; `cdf package archive`; the dlt shim to GA; vault-class secret providers; the first warehouse destination through the sheet-plus-conformance gate.

### 25.2 Distributed execution

A scheduler assigning `(resource, partition)` leases to workers; a shared `CheckpointStore` — Postgres or object-store, with lease fencing so a worker presumed dead cannot resurrect and double-commit; package directories on object storage. No shuffle, no new artifact types, no resource rewrites, because the units of distribution were chosen at the beginning: partitions parallelize, packages hand off, scopes serialize. The coordination-theory note from §4.4 governs the design: per partition, the destination remains the sole voter and retries remain harmless, so distribution adds *scheduling*, not consensus — the hard problem was already dissolved by idempotence, and it stays dissolved at any worker count. Before building a scheduler, Ballista is evaluated honestly as substrate versus inspiration. The acceptance test for the claim that the seams were real: both conformance suites pass unchanged against distributed workers.

### 25.3 The streaming supervisor

A resident process running `Unbounded` plans continuously. Checkpoint cadence, package rotation, and watermarks already exist (§6.5–6.6); the supervisor adds lifecycle — drain, pause, resume-from-head — and log-CDC sources whose `LogPosition` state the ledger already models. Streaming arrives as an executor mode over unchanged types, and the continuity doctrine of the Preface is the reason this ordering is safe: a stream that produces gated, replayable, evidence-bearing windows is just a very frequent batch, and CDF built the windows first.

### 25.4 WASM distribution and the registry

Tier 3 ships on the WASI 0.3 baseline; the conformance suite becomes the registry's admission gate; capability sheets and signed manifests become the trust surface for running strangers' connectors; WASI 1.0, when it lands, is the interface-freeze milestone. Connector breadth begins here, after the protocol is provable — the deliberate inversion of the connector-zoo-first industry default, and the reason the registry can promise what catalogs cannot: every listed connector has passed the same falsifying suite as the first-party ones.

### 25.5 Lakehouse destinations

Per D-27: `cdf-dest-iceberg` and `cdf-dest-delta` over iceberg-rust and delta-rs, committing packages into table formats whose own snapshot metadata rides inside the CDF receipt, so the commit gate and the table's transaction log corroborate each other.

---

# Part VI — The Standing Review

## Chapter 26: Design review invariants

These criteria govern every change to the system — code, artifacts, or this book. A proposed change is reviewed by asking which invariants it touches; a change that strengthens one at another's expense must say so in the open.

- **R1 — Gate totality.** Every state-advancing path routes through `CheckpointStore::commit` with a receipt; the crash matrix covers every lifecycle boundary; no unqualified exactly-once claim anywhere; every guarantee derivable from §14.5's table, and every table row derivable from Chapter 4.
- **R2 — Layer discipline.** The kernel compiles with arrow-rs alone — no DataFusion, DuckDB, Python, or network; no upper layer is required by a lower one; nothing with a screen is load-bearing.
- **R3 — Authoring reality.** Every shipping tier has concrete code in this book, a stated editor and type-checking story, a named data-crossing mechanism with its copy cost, and a trust-boundary classification.
- **R4 — Every noun earns its keep.** Every named concept has an owning crate, a lifecycle, a serialized form, and a CLI verb that inspects it. Concepts failing the test are cut, not kept as aspiration.
- **R5 — Decisions, not vibes.** Every question the design raises is answered in the register with a revisit trigger; every rejected alternative is named with its reason; every tradeoff states its cost.
- **R6 — Intent stays backstage.** Internal design-pressure vocabulary (Appendix B's stress axes) never surfaces in public APIs, CLI flags, or artifact fields; declared intent enters through exactly one field, `trust`, and everything else is mechanism.
- **R7 — Buildable.** The MVP has a cutline; every cut item has a designed seam; the demonstration exercises the gate, contracts, packages, replay, and crash recovery in one sitting.
- **R8 — Honest fidelity.** Wherever data can silently lose meaning — decimals, timezones, identifiers, type mappings, pushdown semantics, foreign state — the design makes the loss impossible, plan-visible, or contract-gated, and says which.
- **R9 — Grounded currency.** Every claim about a dependency's capability or status matches its published state as surveyed in Appendix C; when reality moves, the register moves, not a footnote.
- **R10 — One voice.** The book reads as one argument: consistent nouns, consistent cross-references, and the continuous-practice framing used where it names mechanism — the commit gate, the artifact, the promotion ring, the plan — and absent where it would decorate.

## Chapter 27: Open questions, held on purpose

A design that admits no open questions is hiding them. These are held deliberately, each with the trigger that reopens it, and holding them in the open is itself a review invariant (R5): the default contract strictness question awaits real-project telemetry on silent-drift incidents (D-6); `ForeignState` may eventually deserve its own migration tooling if bridged connectors accumulate enough foreign history to matter; the free-threaded-Python default flips when the ecosystem's default interpreter does (D-25); the WASM interface freezes at WASI 1.0, not before (D-26); and the canonical/archive format roles swap only if Arrow IPC forward-compatibility bites in practice (D-4). None of these blocks anything; each is a tripwire with an owner.

---

# Appendix A — Glossary

**Resource** — smallest stateful unit of extraction; declares a descriptor and capabilities; produces batches. **Source** — configuration and discovery bundle over resources. **Batch** — Arrow payload plus identity and provenance. **Scan plan** — the negotiated read: pushdown fidelity, partitions, ordering, estimates. **Contract** — policy compiled into a validation program with a total verdict lattice. **Package** — hash-addressed evidence of one attempted transition; the build artifact of data. **Receipt** — a destination's durable, independently verifiable acknowledgment; the settlement record. **Checkpoint** — a committed state transition; one head per scope. **Commit gate** — the boundary enforced by `CheckpointStore::commit`; nothing passes it without a verified receipt. **Scope** — sub-resource state key (window, file, stream, partition); the single-writer unit. **Sheet** — declared, lockfile-snapshotted capability table for a resource or destination; a tested claim, not folklore. **Trust level** — declared intent expanding into contract, validation, and retention presets. **Disposition** — destination write semantics: append, replace, merge, cdc_apply. **Drain mode** — the executor mode that runs an unbounded plan until quiescent, then settles and gates.

# Appendix B — The stress harness (internal)

Five axes of extremity — velocity, volume, variety, veracity, value — are retained as design pressure and encoded as tests (spike 8), never as API (R6). Each axis names the mechanisms that absorb its extremes. **Velocity** → checkpoint cadence, package rotation, watermarks, backpressure, byte-bounded channels. **Volume** → partitioned plans, pushdown, spill, the memory pool, bulk destination paths, adaptive batches. **Variety** → discovery schemas, variant capture, nested policies, the versioned normalizer, per-batch schema fingerprints. **Veracity** → contract programs, quarantine, profiles, dedup, freshness, cursor-lag semantics, lineage. **Value** → trust levels, retention, receipt verification, reconciliation counts, demote-on-anomaly. The harness's one enforced lesson: a small set of composable mechanisms, planner-selected from declared intent, beats thirty-two hand-built modes — and the public surface stays eight nouns wide.

# Appendix C — Dependency survey, July 2026

The engineering claims in this book rest on the following verified states; each entry names the design element it grounds. Versions are the state of the world at writing and are governed forward by D-28.

- **Apache DataFusion 54** (spring 2026 cycle; the prior release shipped LIMIT-aware Parquet pruning, dynamic filter pushdown through joins and subqueries, and order-of-magnitude cheaper plan cloning; majors land several times a year; Ballista is maintained as the distributed subproject; a subproject family — Python, Java, Comet, an ADBC driver — evidences substrate health). Grounds: §2.7, §5.4's pin policy, §25.2's Ballista evaluation.
- **arrow-rs 58.x → 59** (majors at most quarterly, minors monthly; deprecated APIs held roughly two majors; **object_store relocated to the apache/arrow-rs-object-store repository** on its own cadence). Grounds: D-28, §12.2's IPC byte-stability gate, the kernel's arrow-only dependency posture.
- **Arrow PyCapsule Interface** (standardized `__arrow_c_schema__` / `__arrow_c_array__` / `__arrow_c_stream__` with capsule names and destructors; schema negotiation on export; pyarrow-independent bridging demonstrated in the ecosystem). Grounds: §9.4's interchange design and the "pyarrow is optional" stance.
- **PyO3 0.28/0.29 and CPython 3.14** (free-threaded build officially supported per PEP 779; PyO3 modules default to declaring GIL-independence; `Python::detach` replaces the old `allow_threads`; abi3t stable ABI arriving with 3.15; 3.13t support dropped upstream in favor of 3.14t+). Grounds: D-25, §6.1, §9.4, spike 5.
- **WASI 0.3.0** (released June 11, 2026: native `async func`, `stream<T>`, `future<T>` in the canonical ABI; `wasi:io` absorbed into the Component Model; host-owned event loop across components; Wasmtime 43+ implements, with Component Model async default shortly after; cancellation and stream optimization on the 0.3.x train; WASI 1.0 targeted late 2026/early 2027; guest toolchains for Rust, Go, JavaScript, and Python in progress). Grounds: D-26, §9.5, §25.4.
- **DuckDB 1.5.x / duckdb-rs** (crate semver now encodes the bundled DuckDB version as 1.MAJOR_MINOR_PATCH.x; an LTS branch tracks 1.4; Arrow 58 interop; appender-arrow and vtab-arrow features; **bundled builds omit the ICU extension**, affecting timezone-dependent operations unless ICU is loaded at runtime or a full build is linked). Grounds: §14.6's honest edges, D-28's pin, `cdf doctor`'s ICU check.
- **dlt (mid-2026)** (declarative REST source with auto-detected pagination; schema contracts with `evolve` / `freeze` / `discard_row` / `discard_value` across tables, columns, and data types, applied uniformly to Arrow tables and dataframes; Pydantic discriminated-union routing; delta/iceberg table formats on the filesystem destination; positioned as agent-native, with thousands of LLM-generated declarative sources). Grounds: §2.1, §9.2's agent argument, §21.2's mode mapping, D-27.

# Appendix D — Objections and answers

Anticipated objections, answered in the design's own terms, because a book that only argues with the convinced has not argued.

**"Why Rust and not Go?"** The substrate decides it. The Arrow and DataFusion reference implementations, the zero-copy FFI story into Python, the absence of a garbage collector between vectorized kernels and their deadlines, and a type system strong enough to make the commit gate a signature rather than a convention are all Rust facts. Go would buy faster onboarding and pay for it at every layer that matters here.

**"Why not make DuckDB the engine?"** DuckDB is a superb database and CDF's first destination, but it is a database, not a pipeline state machine: its Rust binding wraps a C API rather than exposing native plans, and custom planning over arbitrary API, CDC, and object-store resources is DataFusion's exact extension surface. The division of labor — DataFusion executes, DuckDB serves the local analytical loop — uses each for what it is.

**"Isn't this just an orchestrator with extra steps?"** No: it is the thing orchestrators orchestrate. CDF schedules nothing, runs headless under any scheduler, and adds what schedulers cannot: plans, evidence, receipts, and a gate. An orchestrator that triggers `cdf run` gains all four without changing.

**"Why not build on Kafka / a log?"** A durable log is a fine transport and a possible future source archetype, but it answers a different question. The log guarantees ordered delivery of bytes; CDF's problem is proving that *meaningful state* advanced only over *verified destination durability* — which is a statement about endpoints (§4.4), not about the pipe. Systems that mistook the log for the guarantee learned the end-to-end argument the expensive way.

**"Exactly-once systems exist — why refuse the term?"** The systems that honestly claim it define it as CDF does: at-least-once delivery composed with idempotent or transactional application, yielding exactly-once *observed effects* within a stated boundary. CDF refuses only the unqualified slogan, and replaces it with a table whose every row states its boundary — which is more than most systems claiming the term provide.

**"Won't packages double storage?"** Packages are staging that refuses to be deleted prematurely, with retention priced per trust level: the `dev` default keeps five runs; only data declared `financial` pays for long evidence, and even it tombstones to hashes after its window. The comparison is not "storage versus no storage" but "priced evidence versus free archaeology," and archaeology is never free.

**"Why not Iceberg as the package format?"** Table formats answer "what is the current state of this table and how did it change?" Packages answer "what exactly did this run read, decide, and prove?" — including quarantine, plans, verdicts, and receipts that no table format models. The two meet at the destination (D-27), where the table's own transaction log rides inside the CDF receipt, and each ledger corroborates the other.

**"Schema-on-read solved this — why contracts?"** Schema-on-read defers the question; it does not answer it. Every deferred schema is eventually read by something with expectations, and the failure surfaces there — later, farther from the source, harder to attribute. Contracts move the verdict to the boundary where provenance is still attached, and variant capture (§7.5) preserves schema-on-read's genuine virtue — never drop what you cannot yet type — inside a governed frame.

**"Is the plan artifact bureaucracy?"** Only in the sense that `terraform plan` is. The plan costs milliseconds (planning does no I/O — §8.1), prints what an operator would otherwise discover in production, and is skippable by exactly nobody, because the planner is the same code path that executes. Bureaucracy is process that produces documents instead of outcomes; the plan is a document that *is* the outcome, pre-verified.
