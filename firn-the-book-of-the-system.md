# firn

## The Book of the System: a Rust-native, DataFusion-powered, contract-governed data movement kernel

**Date:** July 5, 2026
**Status:** Third revision. Supersedes the July 5 handoff report and the first expanded design. This revision renames the project to **firn**, integrates a fresh survey of the dependency landscape as of July 2026, and restructures the whole document as a book: complete enough that the implementation should emerge from it as a consequence, high-level enough that it contains no implementation code, and unambiguous enough that two teams reading it would build compatible systems.

---

## Preface: why "firn"

Firn is snow that has survived at least one melt season. It is no longer weather; it is not yet ice. Under the weight of the seasons above it, firn compacts — air escapes, crystals fuse — until it crosses a density threshold and becomes glacial ice: a permanent, layered, readable record of every year that fell.

That is this system's model of data movement, stated as glaciology:

- **Snowfall** is extraction: raw batches arriving from sources, voluminous and unproven.
- **Firn** is the load package: extracted data compacted with its evidence — schema, contract verdicts, quality profiles, lineage, proposed state — durable, inspectable, but still able to melt (a package that never commits is discarded or replayed without consequence).
- **Ice** is committed state: once a destination has durably acknowledged the data and the checkpoint ledger records the receipt, the transition is part of the glacier — append-only, layered, readable forever.
- **The firn line** is the commit boundary, the single most important line in the system. Above it, everything is recoverable and repeatable. Below it, nothing is ever silently rewritten.

The metaphor earns its place because it names the invariant the whole architecture serves:

> **A source cursor may advance only after all data represented by that cursor has been durably committed to the destination and the destination's receipt has been recorded in the checkpoint ledger. Nothing crosses the firn line without proof.**

Every chapter of this book is machinery to make that sentence cheap, ergonomic, inspectable, and fast.

## How to read this book

Chapters 1–3 establish what firn is, what it learned from its predecessors, and every decision the design has taken, each with a revisit trigger. Chapters 4–16 are the system itself, from layering down to security, ordered so that each chapter depends only on chapters before it. Chapters 17–21 are the surface operators and contributors touch: the CLI, the project format, the conformance gates that define what "supported" means, the compatibility bridges, and governance. Chapters 22–24 are the road: what ships first, what is deliberately cut, and how the cut things arrive later without rewrites. Chapter 25 is the book auditing itself: the verification criteria this text was written against, and the log of the passes that enforced them. Appendices carry the glossary, the internal five-V stress harness, the July 2026 dependency survey that grounds the engineering claims, and the rip list of ideas this design examined and rejected.

Conventions: `firn` in code face is the CLI binary and crate prefix; *firn* in prose is the project. Interface sketches in Rust, TOML, Python, SQL, and WIT appear throughout; they are normative for shape and semantics, not for identifier-level API stability. When a chapter and the Decision Register disagree, the register wins.

---

# Part I — Position

## Chapter 1: What firn is

firn is a data movement kernel: a Rust library, a family of engine and extension crates, and a single CLI that together turn data ingestion from opaque connector execution into a planned, optimizable, contract-governed, replayable state transition over Arrow-native batches.

One sentence per load-bearing noun:

- A **resource** is an optimizable data source: it declares its schema, keys, cursor, partitioning, and pushdown capabilities, and it produces Arrow record batches.
- A **run** compiles resources into read plans, executes them as streams, validates them against contracts, and packages the results.
- A **load package** is the durable, hash-addressed evidence of a run: data, schema, contract decisions, quality profiles, lineage, state deltas, and destination receipts — the firn of the metaphor.
- A **checkpoint** is a typed state transition that commits only after the destination has durably acknowledged the data the checkpoint represents — the moment firn becomes ice.
- A **destination** is a transactional or idempotent commit target that answers in receipts, never a sink that swallows rows.

### 1.1 What firn is not

firn is not a better Singer, a lighter Airbyte, a native Meltano, or dlt ported line-for-line to Rust. It is not a workflow orchestrator, a dbt replacement, a BI tool, or a hosted platform. It is not a connector catalog whose north-star metric is connector count; breadth is an outcome of a provable protocol (Chapter 19), never a roadmap phase. The kernel is headless, embeddable, and CLI-first; anything with a screen sits above it and depends on it, never the reverse.

firn also refuses "exactly-once" as a slogan. Chapter 13 derives the honest guarantee table: at-least-once extraction, composed with idempotent or transactional destination commits, yields effectively-once results at specific destinations under specific, stated conditions — and `firn plan` prints which condition holds before a byte moves.

### 1.2 The category shift

Every predecessor system, whatever its virtues, *runs a configured connector*. firn *compiles a read*. The difference is the difference between a shell script and a program: before execution, the operator can read what will be fetched, which predicates the source will absorb and at what fidelity, how the work will partition, what DDL might run at the destination, which delivery guarantee applies, and which state will advance — and can diff all of that in a pull request. After execution, the run has left a package: evidence that outlives the process, replays without the source, and hashes identically when nothing changed. Data movement becomes as inspectable, replayable, and boring as committing code. That is the entire ambition.

## Chapter 2: The landscape and its lessons

The original handoff surveyed dlt, Singer, Airbyte, Meltano, Sling, Bruin, and Mage. Those verdicts have held through two revisions and a fresh look at each project's mid-2026 state; they are compressed here to what each contributes, with the firn mechanism that inherits each lesson named precisely.

| Predecessor | Keep | Reject | Where it lands in firn |
|---|---|---|---|
| **dlt** | Library-first authoring; resource/source/pipeline model; schema inference and hints; contracts with `evolve`/`freeze`/`discard` modes; Arrow/pandas/polars accepted as yielded data; declarative `rest_api` source; destination-owned schema migration; `_dlt_loads`-style load history | Python row iteration as the execution substrate; state without a formal commit protocol; no pre-run plan artifact; staging that is not evidence | Resource API (ch. 7), authoring tiers (ch. 8), contract compiler (ch. 10), `_firn_loads` mirrors (ch. 12), packages (ch. 11), the dlt shim (ch. 20) |
| **Singer** | A protocol simple enough to debug with pipes; stdin/stdout composability | JSON rows as the data plane; state as an untyped blob; undeclared capabilities | Subprocess tier (§8.6), typed positions with an honest `ForeignState` quarantine type (§12.3), capability sheets (§7.3) |
| **Airbyte** | Destination-acknowledged state; protocol rigor; source/destination actor separation | Containers and inter-process JSON as the default; the platform as the kernel; opaque state semantics | The firn-line invariant itself (§12.2), receipts (§13.4), the Airbyte-source adapter (§20.3) |
| **Meltano** | Git-native projects; environment overlays; lockfiles; awareness that same-named connector *variants* differ in behavior | Inheriting Singer's looseness; packaging as architecture | Project format (ch. 18); the lockfile extended from versions to *semantics* — capability sheets and type mappings are locked (§18.3) |
| **Sling** | One command moves data; operational crispness; YAML replication configs | Thin state and evidence model | CLI design (ch. 17), declarative resources (§8.2) |
| **Bruin** | One project surface for ingestion, quality, comparison | Monolith-first construction | The project layer is a crate that depends on the kernel, never the reverse (ch. 4) |
| **Mage** | Preview one batch, run one partition, inspect everything, locally | UI blocks as the deepest abstraction | `firn preview`, `firn inspect`, `firn replay` (ch. 17); no UI in the kernel's dependency graph |

Two observations from the 2026 re-survey sharpen the picture beyond the original verdicts.

First, **dlt has kept moving in firn's direction, which validates the thesis and raises the bar.** Its declarative REST source, its contract modes applied uniformly to Arrow tables and dataframes, its Pydantic-discriminated event routing, and its delta/iceberg table-format options on the filesystem destination all confirm that the market wants declared intent compiled into behavior. What dlt cannot do — because Python object iteration remains its substrate and its state remains informal relative to a ledger — is precisely firn's ground: pre-run compiled plans, pushdown negotiation with declared fidelity, hash-addressed evidence, and a mechanized commit protocol. The dlt shim (§20.2) exists so that validation flows in firn's favor rather than against it.

Second, **the DataFusion ecosystem has become the strongest possible proof that building on it is a maintained path, not a bet.** delta-rs, LanceDB, InfluxDB 3, GreptimeDB, Comet (accelerating Spark itself), Ballista (distributed execution), and a growing ADBC driver all build on the same extension points firn uses — `TableProvider`, `ExecutionPlan`, optimizer rules, filter-pushdown negotiation. DataFusion's 2026 releases moved exactly the machinery firn cares about: LIMIT-aware Parquet pruning, dynamic filters pushed through joins and subqueries, and order-of-magnitude cheaper plan cloning for systems that plan constantly — which firn is. The dependency is fast-moving (quarterly majors), so Chapter 21 pins how firn rides it without being dragged; but the direction of motion is firn's direction.

## Chapter 3: The Decision Register

A design meant to be built cannot leave questions open. Every decision below is numbered, answered, and given a *revisit trigger* — the observable condition under which it should be reopened. Chapters elaborate; the register rules. Decisions D-1 through D-24 originate in the second revision; D-25 through D-28 are new in this one, forced by the July 2026 dependency survey (Appendix C).

**D-1. How deeply is DataFusion embedded?**
Two tiers. Every resource implements the minimal `ResourceStream` contract (descriptor + partitions + batch stream, built on arrow-rs types only). Resources that can do less work when asked for less additionally implement `QueryableResource`, which the engine wraps in a DataFusion `TableProvider`. DataFusion is mandatory in the engine crate and invisible in the authoring path. *Revisit if* a large fraction of first-party resources hand-roll pushdown the engine could have negotiated.

**D-2. How is user extraction logic supplied?**
A five-tier ladder, cheapest first: (0) declarative TOML/YAML compiled to native resources, validated by a published JSON Schema; (1) Rust, statically linked; (2) Python, embedded via PyO3 with zero-copy Arrow interchange over the Arrow C Data Interface and PyCapsule protocol, shipped with a fully typed `firn-sdk`; (3) WASM Components over a published WIT world, sandboxed, for third-party distribution; (4) subprocess adapters speaking Arrow IPC, NDJSON, Singer, or Airbyte protocol over stdio. Lua, bespoke DSLs, and dynamically loaded Rust plugins are rejected (§8.7). Full treatment in Chapter 8. *Revisit* per D-25 and D-26, which supersede this decision's earlier caveats.

**D-3. How are quarantine side-outputs represented?**
As a framework construct, not a DataFusion plan construct. `ContractExec` yields one accepted stream to the plan and routes rejected rows through a side channel into the package's `quarantine/` artifacts, each row carrying error code, contract rule ID, and source position. Destinations may optionally materialize `_firn_quarantine` tables. *Revisit if* DataFusion grows first-class multi-output plans.

**D-4. What is the canonical package data format?**
Arrow IPC (file format, LZ4-framed) inside a package, because packages must round-trip the exact observed schema, including dictionary encodings and field metadata, and Parquet's type system is a lossy projection of Arrow's. Parquet is the archive and interchange tier (`firn package archive` transcodes with a fidelity report) and the analytical tier (stats, quarantine, lineage are Parquet because they are queried across packages). Manifests and receipts are canonical JSON. Everything is hash-addressed (§11.5). *Revisit if* Arrow IPC forward-compatibility across arrow-rs majors bites in practice.

**D-5. Where does state live?**
Behind a `CheckpointStore` trait from day one; SQLite in WAL mode is the local default and an in-memory store serves tests. Destinations that can hold tables additionally mirror durable load facts into `_firn_loads` and `_firn_state`, enabling ledger reconstruction and warehouse-native audit (§12.6). Postgres- and object-store-backed stores are post-MVP; the trait is sized for them, including the lease semantics distribution will need. *Revisit if* multi-writer local state becomes common before distributed mode ships.

**D-6. How strict are contracts by default?**
Default `evolve` (new tables and columns admitted, widening admitted, narrowing quarantined), because first contact with a source is always discovery — the same conclusion dlt's field experience reached. `firn contract freeze` flips a resource; project-level trust presets set stricter defaults in one line (§10.4). *Revisit if* real-project telemetry shows `evolve` defaults causing silent-drift incidents; the fallback is freeze-after-first-clean-run.

**D-7. How does pushdown declare correctness?**
Per predicate, exactly as DataFusion's `TableProviderFilterPushDown` models it: `Exact` (source semantics provably match engine semantics; the engine drops its filter), `Inexact` (source returns a superset — prunes pages, windows, partitions — and the engine re-applies the exact predicate), `Unsupported`. API resources default to `Inexact` because remote timezone, collation, and consistency semantics rarely match Arrow's bit-for-bit. `firn explain` prints the classification per predicate. *Revisit:* never; DataFusion's production experience settles this.

**D-8. How do bounded and unbounded resources share the runtime?**
Both produce the kernel's `BatchStream`; the difference is a `Boundedness` marker plus mandatory policy: an unbounded plan is illegal without a checkpoint cadence, a package rotation rule, and a watermark strategy. The MVP executor runs bounded plans to completion and unbounded plans in *drain mode* (until quiescent or `--max-duration`), which covers micro-batch CDC and queue draining. A resident streaming supervisor is a later executor mode over the same types (§24.3). *Revisit if* log-CDC demand outruns the streaming milestone; CDC ships first as bounded micro-batches over a log position.

**D-9. Do transformations live in firn?**
In-flight, per-batch, schema-stable transforms yes — rename, cast, redact, derive, filter, nested-structure policy — compiled into the normalize operator and recorded in the package. Multi-table, post-load model graphs no; the project layer triggers dbt/SQLMesh with the load receipt as the handoff artifact (§10.6). *Revisit if* users consistently smuggle post-load SQL into resources.

**D-10. Are package manifests signed?**
Hash-addressed now (SHA-256 content addressing; the canonicalized manifest hash is the package's identity), signature-ready now (a reserved detached-signature slot with a defined signing input), actually signed post-MVP behind a feature flag, with the same slot serving connector signing when the WASM registry arrives. *Revisit if* a compliance-driven design partner needs signatures at MVP.

**D-11. When does distributed execution arrive?**
After local correctness is proven: after the conformance suites pass on three sources and three destinations. The seams exist from the first commit — partitions are the distribution unit, packages are the shuffle-free hand-off, the `CheckpointStore` trait admits a fenced remote implementation. DataFusion's own Ballista subproject demonstrates the pattern and remains a candidate substrate to evaluate rather than reinvent (§24.2). *Owner: the milestone after conformance.*

**D-12. Async runtime and concurrency model?**
Tokio, multi-threaded, because DataFusion, object_store, and every relevant driver live there. CPU-heavy work runs on a separate pool from I/O reactors, following the two-runtime pattern DataFusion's maintainers recommend; blocking FFI (DuckDB, Python) is confined to bounded `spawn_blocking` pools (§5.1).

**D-13. Memory management?**
One accounting ledger: DataFusion's `MemoryPool` for engine operators, extended to framework buffers (package builders, adapter queues). Exceeding budget triggers, in order, early package-segment flush, backpressure, spill, clean failure — never surprise OOM (§5.3).

**D-14. Identifier and naming policy?**
Source-original names preserved verbatim in schema metadata forever. A versioned normalizer (`namecase-v1`) derives destination identifiers deterministically; post-normalization collisions are plan-time hard errors; the normalizer version is recorded in every schema snapshot because changing it re-keys downstream consumers (§6.4).

**D-15. Canonical type system?**
Arrow's, closed, plus three metadata annotations: semantic tags, source-name provenance, nullability provenance. Destination type mappings are declared tables in capability sheets, joined at plan time, snapshotted in the lockfile; lossy mappings require an explicit contract allowance (§6.3).

**D-16. Deletes and CDC semantics?**
Write dispositions include `cdc_apply`; CDC-shaped batches carry a `_firn_op` column and source positions. MVP covers deletion-aware merge for cursor sources that expose deletions; log-based CDC is a post-MVP source archetype whose state shapes (`LogPosition`, transaction boundaries) the ledger already models (§13.3).

**D-17. Retry, rate limit, and error taxonomy?**
One taxonomy — `Transient`, `RateLimited`, `Auth`, `Contract`, `Data`, `Destination`, `Internal` — drives retry at the smallest safe unit (request → partition → run), with a run-level retry budget. A built-in HTTP toolkit gives every declarative and Python API resource correct pagination, rate limiting, backoff, and auth refresh by construction (ch. 14).

**D-18. Secrets?**
References, never values, in every serialized artifact: `secret://provider/key` URIs everywhere; resolution at execution through a `SecretProvider` trait; resolved values live in zeroizing wrappers registered with a redaction layer that scrubs diagnostics by construction (§16.1).

**D-19. Observability?**
`tracing` throughout with optional OTLP export — and, more importantly, the system's own artifacts are the primary observability surface: ledger, packages, receipts, and mirrors are queryable data, and `firn sql` queries the system's own history because the ledger is SQLite, the stats are Parquet, and DataFusion is right there (ch. 15).

**D-20. Scheduling?**
Out of kernel, permanently. The CLI is scheduler-friendly (single binary, exit codes, `--json` everywhere); the library embeds anywhere. `firn run --loop <interval>` exists for local development only.

**D-21. Testing strategy?**
Three pillars: conformance suites that define what "supported" means for any resource or destination; a chaos layer that kills the process at every lifecycle boundary in CI forever; golden-package tests that compare produced evidence hash-by-hash against committed fixtures (ch. 19).

**D-22. Licensing and governance?**
Apache-2.0, one repository, crates on crates.io under semver. The serialized artifacts — checkpoint schema, package manifest, capability sheets, the WIT world, the declarative JSON Schema — are independently versioned specs with committed migrations, because artifacts outlive binaries (ch. 21).

**D-23. Python's role, precisely?**
Authoring and interchange surface, never execution substrate. Python runs to produce batches; from the moment a batch crosses the C Data Interface, everything is Rust. PyO3 lives in the optional `firn-python` crate; the kernel has zero Python in its dependency graph (§8.4).

**D-24. Naming?** *(resolved)*
The project is **firn**. The name satisfies every constraint the placeholder analysis set: two syllables, verb-adjacent ("firn it into the warehouse" reads naturally), `cargo add firn`-able, no collision with incumbent data tools, and — decisively — it *names the architecture*: compaction of raw fall into a permanent layered record, with a firn line that nothing crosses without proof. CLI binary `firn`, crate prefix `firn-`, PyPI package `firn-sdk`, WIT namespace `firn:resource`, dot-directory `.firn/`, project file `firn.toml`.

**D-25. (new) Free-threaded Python posture?**
Python 3.14's free-threaded build is officially supported upstream (no longer experimental), and PyO3 0.28+ targets it, including the renamed `Python::detach` API and the abi3t stable-ABI story arriving with 3.15. firn's posture: **correct on GIL builds, parallel on free-threaded builds, identical semantics on both.** `firn-sdk` and `firn-python` are built and CI-tested against both a GIL 3.12+ interpreter and 3.14t; on free-threaded interpreters, multiple Python resources (and multiple partitions of one `parallel`-marked resource) execute concurrently on the dedicated Python pool, and the historical "one GIL, one producer at a time" ceiling disappears. The design must not *depend* on free-threading — the GIL path remains fully supported — but it must not waste it either. *Revisit if* the ecosystem's default interpreter flips to free-threaded, at which point the GIL path becomes the compatibility mode.

**D-26. (new) WASM tier baseline?**
WASI 0.3.0 shipped on June 11, 2026, bringing native async — `async func`, `stream<T>`, `future<T>` — into the Component Model's canonical ABI, with runtime support in Wasmtime 43+ and Component Model async on by default shortly after. firn's WIT world therefore targets **WASI 0.3 as its baseline**, skipping the 0.2 pollable/stream dance entirely: a guest resource exports `open(partition) -> stream<u8>` of Arrow IPC bytes, the host owns the single event loop across all components, and cancellation semantics arrive with the 0.3.x train. This strengthens the original tier-3 design (host-mediated everything, one copy per batch) and removes its worst ergonomic risk. Guest SDKs: Rust first, Python via componentize-py as its 0.3 support lands. Timing unchanged: designed now, shipped post-MVP (§8.5). *Revisit at* WASI 1.0 (expected late 2026/early 2027) for the registry's long-term interface freeze.

**D-27. (new) Lakehouse table formats?**
Iceberg and Delta are destinations, not package formats. The package remains firn's own evidence layout (D-4); post-MVP, `firn-dest-iceberg` and `firn-dest-delta` commit packages into lakehouse tables through iceberg-rust and delta-rs — both DataFusion-native projects — with the table format's own snapshot/commit metadata carried inside the firn receipt, so the firn line and the table format's transaction log corroborate each other. This is the industry's direction (dlt already exposes both on its filesystem destination) and firn meets it with receipts rather than with a parallel metadata story. *Revisit if* a design partner needs Iceberg at MVP; the Parquet/object-store destination is the designed seam.

**D-28. (new) Upstream cadence policy?**
The load-bearing dependencies move fast on published schedules: arrow-rs releases majors at most quarterly and minors monthly; DataFusion majors land several times a year; duckdb-rs now encodes the bundled DuckDB version in its own semver (1.MAJOR_MINOR_PATCH.x) and maintains an LTS branch; object_store lives in its own Apache repository on its own cadence. firn's policy: each firn minor release pins exactly one (arrow-rs major, DataFusion major, object_store minor, duckdb-rs DuckDB-encoded version) tuple, recorded in the lockfile spec; upgrades are deliberate, tested against the golden-package suite (Arrow IPC byte-stability is a release gate), and never occur in firn patch releases. Deprecation windows upstream (arrow-rs holds deprecated APIs ~two majors) are long enough that firn can always skip at most one major without forking. *Revisit if* an upstream breaks serialized-artifact compatibility, which promotes that dependency to the artifact-spec review process.

---

# Part II — The System

## Chapter 4: Layering

Four layers with a strict dependency direction; lower layers never import upper ones.

```text
┌──────────────────────────────────────────────────────────────┐
│  Layer 4: Project & Product                                   │
│  firn.toml, environments, lockfile, CLI, doctor, status       │
├──────────────────────────────────────────────────────────────┤
│  Layer 3: Extensions                                          │
│  authoring tiers (declarative, Python, WASM, subprocess),     │
│  destinations, formats, HTTP toolkit, secret providers        │
├──────────────────────────────────────────────────────────────┤
│  Layer 2: Engine                                              │
│  DataFusion providers, physical operators, planner, executor  │
├──────────────────────────────────────────────────────────────┤
│  Layer 1: Kernel                                              │
│  types, traits, contract compiler, package model,             │
│  checkpoint ledger, receipts — arrow-rs only; no DataFusion,  │
│  no DuckDB, no Python, no network                             │
└──────────────────────────────────────────────────────────────┘
```

### 4.1 The kernel is engine-free, and why that is not purism

Layer 1 defines `Resource`, `Batch`, `Contract`, `Checkpoint`, `LoadPackage`, `Destination`, `Receipt`, and the state machine connecting them, depending on arrow-rs (schemas, record batches, IPC) and nothing heavier. Three concrete purchases. First, the correctness core — the crash matrix, the firn-line invariant, the package lifecycle — tests in milliseconds without a query engine in the harness, and failures mean something. Second, the serialized artifacts are specified by kernel types, so an engine swap, or a second implementation in another language, reads the same manifests, checkpoints, and receipts; the artifacts are the protocol, and the kernel is their reference semantics. Third, embedding: a service that wants receipts and checkpoints but brings its own extraction loop depends on Layer 1 alone.

The one deliberate dependency is arrow-rs itself. A kernel that abstracted over Arrow would be abstracting over the industry's agreed memory format for no benefit; arrow-schema and arrow-array are stable, light, and exactly the vocabulary the artifacts need. The kernel's `BatchStream` is a pinned boxed `Stream` of `Result<RecordBatch>` over arrow-rs types; `firn-engine` adapts it to DataFusion's `SendableRecordBatchStream` at the boundary, in both directions.

### 4.2 The engine owns execution; the kernel owns meaning

DataFusion, in Layer 2, owns Arrow expression evaluation, SQL and DataFrame planning, projection/filter/limit pushdown negotiation (including the dynamic filters recent releases push through joins and subqueries), partitioned vectorized execution, and `EXPLAIN`. The kernel owns what a run *means*: which committed state it consumed, which contract governed it, what evidence it must produce, and when a checkpoint may cross the firn line. The engine's operators — `ContractExec`, `NormalizeExec`, `ProfileExec` — *enforce* decisions; the decisions themselves are Layer 1 values fixed at plan time and serialized into the package. The engine may be re-planned; the meaning may not.

### 4.3 Bounding the DataFusion relationship

Three rules keep a fast-moving dependency healthy rather than viral. Kernel types never expose DataFusion types in public signatures. Each firn minor release pins one DataFusion major (D-28) and upgrades deliberately, gated on the golden-package suite. And every firn-authored physical operator lives behind an internal facade with a hand-rolled fallback, so an upstream breaking change degrades performance, never correctness. The inverse relationship also matters: where firn needs something DataFusion almost has — multi-output plans for quarantine being the standing example — firn builds around the engine rather than forking it, and files the upstream issue.

### 4.4 Crate map

```text
crates/
  firn-kernel/          # Layer 1. Types, traits, state machine, artifact specs. arrow-rs only.
  firn-engine/          # Layer 2. DataFusion providers, operators, planner, executor.
  firn-contract/        # Contract compiler: policy × observed schema -> validation program.
  firn-package/         # Package builder, reader, replayer, hasher, GC, archive transcoder.
  firn-state-sqlite/    # CheckpointStore + run ledger over SQLite (WAL).
  firn-http/            # HTTP toolkit: paginators, limiter, backoff budget, auth sessions.
  firn-formats/         # arrow-ipc, parquet, ndjson, csv adapters.
  firn-declarative/     # Tier 0: TOML/YAML -> Resource compiler + published JSON Schema.
  firn-python/          # Tier 2: PyO3 embedding, PyCapsule/C-Data bridge. Optional.
  firn-wasm/            # Tier 3: wasmtime host, WASI 0.3 WIT bindings. Optional, post-MVP.
  firn-subprocess/      # Tier 4: stdio adapters (Arrow IPC, NDJSON, Singer, Airbyte).
  firn-dest-duckdb/     # Destination: DuckDB.
  firn-dest-parquet/    # Destination: Parquet over fs / object_store.
  firn-dest-postgres/   # Destination: Postgres.
  firn-project/         # Layer 4: firn.toml, environments, secrets wiring, lockfile.
  firn-cli/             # Layer 4: the `firn` binary.
  firn-conformance/     # Test-only: resource & destination suites, chaos layer, golden packages.
```

## Chapter 5: Runtime model

### 5.1 Concurrency

Tokio, multi-threaded. The classic failure of marrying an analytics engine to an I/O runtime is CPU-bound operators starving network reactors; firn adopts the two-runtime pattern DataFusion's maintainers document for exactly this: an I/O runtime owns sockets, timers, object-store requests, and destination connections; a CPU pool owns decode, validation kernels, and encode; bounded channels carry streams between them. Blocking FFI — DuckDB's C API, the Python interpreter — is confined to `spawn_blocking` pools with configured ceilings, so a stalled destination or a slow Python generator can never exhaust the workers extraction depends on. On free-threaded Python (D-25), the Python pool genuinely parallelizes; on GIL builds it degrades gracefully to interleaved producers with the GIL held only during production.

### 5.2 Backpressure

Every hop is a bounded channel sized in bytes (via each batch's `bytes` field), not messages. A slow destination therefore pressures the resource batch-by-batch, and well-behaved sources — paginated APIs, SQL cursors — simply pause fetching. Resources that cannot pause (webhook drains, log tails) declare `backpressure: false` in their capability sheet, and the planner then *requires* a spill policy: overflow batches flush to the package directory early, trading disk for memory under a hard byte ceiling, after which the run fails cleanly with a `Data` error rather than degrading unpredictably.

### 5.3 Memory accounting

One ledger for memory, mirroring the ledger for state. DataFusion operators already account through its `MemoryPool`; firn components that buffer — the package builder's pending-segment queue, adapter decode buffers, the DuckDB appender staging area — register against the same pool. The budget is one project-level number with a sane default. Exceeding it triggers, in order: early segment flush, backpressure, spill, clean failure. `firn run --explain-memory` prints who held what at peak, from the pool's own records; there is no second, informal memory story to reconcile.

### 5.4 Adaptive batch sizing

Batch size floats between a configured floor and ceiling (default 1k–64k rows, 1–32 MiB), seeded from resource estimates and adjusted by a simple controller: starved downstream queues grow batches; spill shrinks them. Every produced batch's actual size is recorded in the package, and **replay uses recorded batches, never re-derives them** — which is how adaptive live behavior coexists with deterministic evidence (§19.3).

### 5.5 Bounded and unbounded streams

Every plan node carries `Boundedness`, the same distinction DataFusion draws. Bounded plans run to completion and produce one or more packages. Unbounded plans are legal only with a checkpoint cadence (`every N batches | every T seconds | on watermark advance`), a package rotation rule, and a late-data watermark policy — the planner refuses otherwise. The MVP executor accepts bounded plans and unbounded plans in drain mode; the resident streaming supervisor (§24.3) is a later scheduler over unchanged types. Carrying `Boundedness` from the first commit is what makes streaming an extension instead of a rewrite.

## Chapter 6: The type system

### 6.1 Arrow is the type system

firn invents no type lattice. The logical type system is Arrow's, closed, with three annotations carried in `Field` metadata:

```text
firn:semantic     optional tag: json | uuid | url | currency:<code> | pii:<class> | ...
firn:source_name  the source's identifier, verbatim, forever
firn:null_origin  declared | inferred | widened   — why is this field nullable?
```

Semantic tags never change physical execution; they change policy. A `pii:email` tag arms redaction in previews and quarantine artifacts; a `json` tag steers destinations toward their native JSON type; a `currency` tag arms a contract rule. Keeping semantics in metadata rather than wrapper types means every DataFusion kernel keeps working untouched — the annotations ride along for free.

### 6.2 The fidelity rules

Two source-to-Arrow rules are non-negotiable, because violating them is the signature silent corruption of this product category:

1. **Decimals stay decimals.** Source `NUMERIC(p,s)` becomes Arrow `Decimal128/256(p,s)`, never `Float64`. A resource that cannot produce decimals must declare it, and the planner surfaces the coercion before the run.
2. **Timestamps keep their zone story.** Zoned timestamps become `Timestamp(unit, Some("UTC"))` with the original zone preserved in metadata where it carries meaning; naive timestamps become `Timestamp(unit, None)` and are never silently assumed UTC. The contract rule `preserve_timestamp_timezone` converts a naive-timestamp source from a quarterly incident into a plan-time error.

### 6.3 Destination type mapping is data, not code

Every destination ships a capability sheet (§13.2) whose core is a declared mapping table: Arrow type → destination type → fidelity (`exact | widening | lossy | unsupported`). The planner joins the resource's schema against the sheet before execution; `lossy` requires the explicit contract allowance `allow_lossy_mapping`, and `unsupported` fails the plan. The sheet is snapshotted into the lockfile, so a driver upgrade that changes a mapping is a reviewable Git diff, not a production surprise.

### 6.4 Identifiers

Names are the most under-designed corner of most ingestion tools; firn treats them as versioned data. The source's exact identifier lives in `firn:source_name` permanently. A versioned normalizer — `namecase-v1`: Unicode NFC → lower snake_case → destination charset filter → length truncation with an 8-hex-char hash suffix on truncation or collision — derives destination identifiers deterministically. Collisions after normalization (`userName` and `user_name` in one table) are plan-time hard errors with a rename hint, never last-writer-wins. The normalizer version is recorded in every schema snapshot and package; bumping it is a breaking change that `firn diff schema` surfaces, because renaming destination columns re-keys everything downstream. Case-folding destinations (upper-folding warehouses, lower-folding Postgres) are handled by the destination sheet's identifier rules, never by resource authors.

### 6.5 Nested data and variants

Arrow `Struct`, `List`, and `Map` are first-class end to end; nothing forces flattening. A resource's normalization policy chooses among: keep nested (default where the destination supports it); dlt-style child-table expansion (deterministic child names via the normalizer, parent keys propagated, load order recorded in the package); or **variant capture** — unknown or contract-violating substructure lands in a `_firn_variant` column tagged `json`, so discovery-phase resources never drop data they cannot yet type. Promoting a variant to typed columns is a contract-evolution event, diffed and recorded like any other schema change.

## Chapter 7: The resource model

### 7.1 Two tiers, one identity

Forcing every author to think about projections and filters is the ergonomic failure that makes people flee query-engine-adjacent APIs, so the resource trait splits.

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
    // ScanPlan: what the source will do — per-predicate fidelity, chosen
    //           partitioning, expected ordering, cost estimate if available.
}
```

Tier A resources still get planning — the engine wraps them with engine-side projection, filtering, and limits — and participate in contracts, packages, and checkpoints identically. Tier B resources become DataFusion `TableProvider`s through a generic adapter in `firn-engine`, inheriting pushdown negotiation, `EXPLAIN`, and SQL access. The split enforces DataFusion's own discipline structurally: `negotiate` runs at plan time and must not do I/O; `open` does the work.

### 7.2 The descriptor

```rust
struct ResourceDescriptor {
    id: ResourceId,                     // "github.issues"
    schema: SchemaSource,               // Declared(SchemaRef) | Hints(Vec<Hint>) | Discover
    primary_key: Option<Vec<FieldPath>>,
    merge_key: Option<Vec<FieldPath>>,
    cursor: Option<CursorSpec>,
    write_disposition: WriteDisposition,
    contract: ContractRef,
    state_scope: StateScope,            // §12.4
    freshness: Option<FreshnessSpec>,   // expected cadence; arms staleness checks
    trust: TrustLevel,                  // experimental | governed | financial | serving
}
```

`CursorSpec` carries the field that prevents the most common incremental-load bug in the wild: an `ordering` claim (`exact` — a SQL `ORDER BY` is; most APIs are not — or `best_effort`) and a `lag` tolerance, the window in which the source may still mutate rows behind the cursor. Inexact ordering or nonzero lag forces window-close semantics — the committed cursor advances to `max(cursor) − lag`, never to the naive maximum — so records written with a stale `updated_at` after the cursor passed are caught by the next window instead of skipped forever.

### 7.3 Capabilities are claims with consequences

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

Two rules give the sheet teeth. **Claims are tested:** the conformance suite generates scans exercising every claim and fails resources whose behavior contradicts their sheet — an `Exact` filter claim is fed adversarial values around timezone, collation, and null edges and compared against engine-side ground truth. **Claims are snapshotted:** the lockfile records each resource's capability sheet, so a connector update that silently loses a capability — a failure mode the Meltano variant ecosystem knows well — surfaces as a reviewable diff.

### 7.4 Pushdown fidelity in practice

`firn explain` renders the negotiation, turning pushdown from folklore into a reviewable artifact:

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

The last line is the delivery-guarantee derivation of §13.5, printed with every plan because a guarantee the operator has not seen is a guarantee that does not exist.

### 7.5 Sources group resources; the resource stays primary

A `Source` is a named bundle: shared configuration and credentials, a discovery function (list tables, endpoints, streams) whose output is a set of `ResourceDescriptor`s, and defaults members inherit. Nothing in the runtime special-cases sources. The smallest stateful unit — the resource — remains the unit of state, planning, contracts, and conformance, deliberately, because every predecessor that blurred this line ended up with state it could not scope.


## Chapter 8: Authoring — how user code gets in

A Rust kernel with no story for non-Rust authorship is a niche tool; a kernel that embeds a scripting language badly is a slow tool with a miserable editor experience. The resolution is a graded ladder in which every tier is chosen for what it is uniquely good at, and the interchange between every tier and the kernel is identical: Arrow record batches plus a `ResourceDescriptor`. Whatever the tier, the moment data crosses into the kernel, the authoring language's involvement ends.

### 8.1 The ladder

| Tier | Form | Runs | Data crossing | Editor story | Ships |
|---|---|---|---|---|---|
| 0 | Declarative TOML/YAML | in-process, compiled to native resources | none | published JSON Schema → completion + validation in any editor | MVP |
| 1 | Rust | in-process, statically linked | none | rust-analyzer, full types | MVP |
| 2 | Python | embedded interpreter (PyO3) | Arrow PyCapsule / C Data Interface, zero-copy | fully typed `firn-sdk`; pyright-clean is a release gate | MVP |
| 3 | WASM Component (WASI 0.3) | wasmtime, sandboxed | native `stream<u8>` of Arrow IPC (one copy) | language-native tooling + published WIT world | post-MVP |
| 4 | Subprocess | external process over stdio | Arrow IPC (preferred) / NDJSON / Singer / Airbyte | whatever the foreign tool has | MVP (IPC+NDJSON); Singer/Airbyte fast-follow |

### 8.2 Tier 0 — declarative resources

Most ingestion is not novel logic; it is a REST endpoint with pagination and auth, a SQL table with a cursor column, or a glob of files. dlt's `rest_api` source and Sling's replication YAML proved from opposite directions that a good declarative layer deletes code from the majority case — dlt now even reports thousands of LLM-generated declarative sources, which is itself an argument: a constrained, schema-validated format is the safest thing to let an agent write. Tier 0 makes that layer primary:

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

`firn-declarative` compiles this into real `QueryableResource` implementations backed by `firn-http`, which is why declarative resources get pushdown (the `since` param *is* the cursor filter, fidelity `Inexact`), partitioning, retries, and rate limiting without their authors learning those words. `kind = "sql"` and `kind = "files"` get equivalent treatment. The JSON Schema for the format ships with every release and registers with SchemaStore, so VS Code, JetBrains, Zed, and Neovim validate and complete it with zero setup; `firn validate` runs the same schema plus semantic probes (does the cursor field exist in a sample response?) in CI.

The escape-hatch gradient matters as much as the format: a declarative resource can name a Python or Rust function for exactly the piece that resists declaration —

```toml
records_transform = "python://./src/gh.py#flatten_reactions"
```

— so outgrowing Tier 0 costs one function, not a rewrite.

### 8.3 Tier 1 — Rust

Rust has no stable ABI, so dynamically loaded `.so` connectors are a trap this design refuses: crash-prone, toolchain-locked, and solved better by Tier 3 where sandboxing repays the boundary cost. Rust authorship is therefore static — firn is a library, and Rust users own a binary:

```rust
use firn::prelude::*;

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
    firn::project::main()   // this binary now speaks the full `firn` CLI over local resources
}
```

Teams that want Rust resources without owning a binary get `firn build`: a project-local `extensions/` crate compiled against a pinned toolchain and linked into a project-specific runner. Either way, linkage is static.

### 8.4 Tier 2 — Python, done honestly

Python earns embedding for three reasons no alternative matches: the existing ingestion-authorship population lives there (every dlt user, every Singer tap author); the client-library long tail — Salesforce, NetSuite, SAP wrappers — lives there; and Arrow interchange with Python is a solved, standardized, zero-copy problem. The Arrow PyCapsule Interface (`__arrow_c_schema__`, `__arrow_c_array__`, `__arrow_c_stream__`) means any Arrow-producing Python object — pyarrow, pandas-with-ArrowDtype, polars, DuckDB results, nanoarrow — crosses into arrow-rs structures by moving capsule-wrapped C structs, not by serializing. `firn-python` consumes the protocol directly (via a pyo3-arrow-style bridge), which has a subtle consequence worth stating: **pyarrow is not a hard dependency of `firn-sdk`.** Authors who yield plain dicts need no Arrow library at all; authors who yield frames bring whichever library produced them.

```python
# src/github.py — authored against `firn-sdk` (types + stubs; pyarrow optional)
import firn

@firn.resource(primary_key="id", cursor="updated_at",
               write_disposition="merge", contract="governed")
def issues(ctx: firn.Context):
    """Yield dicts, or any object speaking the Arrow PyCapsule protocol."""
    for page in ctx.http.paginate("/repos/acme/app/issues",
                                  params={"since": ctx.cursor.last}):
        yield page.json()
```

Execution rules that keep this from quietly becoming "Python is the runtime":

- `firn-python` embeds one interpreter per run; resource generators are driven on the dedicated Python pool. On GIL builds, the lock is held only while user code produces the next chunk — conversion and everything downstream is lock-free Rust. On free-threaded 3.14t (D-25), the pool genuinely parallelizes across resources and across partitions of `parallel`-marked resources; semantics are identical on both builds, and firn's CI tests both.
- Yielded dicts batch through the same inference path as NDJSON; yielded PyCapsule-speaking objects cross zero-copy.
- One Python resource cannot stall the world: per-resource watchdogs and byte-bounded channels apply at the boundary like everywhere else.
- `ctx` exposes *firn's* HTTP toolkit, secrets, cursor view, and logger, so Python authors inherit rate limiting, retries, and redaction instead of importing `requests` and re-inventing failure.
- Environment resolution is boring by design: `firn.toml` pins the interpreter (a venv or uv-managed environment); `firn doctor` verifies importability, stub versions, and whether the interpreter is free-threaded. No bundled-interpreter magic.
- **The editor story is a shipping requirement:** `firn-sdk` is `py.typed` with complete stubs for `Context`, the decorators, and descriptor kwargs; firn's CI runs pyright over the SDK examples, and a type-checking regression blocks release.

### 8.5 Tier 3 — WASM Components on WASI 0.3

The distribution problem — running a stranger's connector without trusting it — is what the sandbox is for, and the ground shifted under this tier between revisions: WASI 0.3.0 is released, with native `async func`, `stream<T>`, and `future<T>` in the Component Model's canonical ABI, the host owning one event loop across all components, and Wasmtime shipping it enabled by default. firn's WIT world targets 0.3 as baseline (D-26):

```wit
package firn:resource@0.1.0;

world resource {
    import firn:host/http;        // host-mediated: rate limits, redaction, egress allowlists apply
    import firn:host/secrets;     // resolve-by-reference only
    import firn:host/log;

    export describe: func() -> descriptor;
    export negotiate: func(req: scan-request) -> scan-plan;
    export open: async func(part: partition) -> stream<u8>;   // Arrow IPC stream bytes
}
```

Batches cross as Arrow IPC through a native component stream: one copy per batch, host-scheduled, cancellation arriving with the 0.3.x train. The sandbox terms are the point — no ambient filesystem, no direct sockets, HTTP only through the host import where firn's limiter, redaction, and per-source egress allowlists apply to strangers' code exactly as to firn's own. Guest SDKs begin with Rust; Python follows as componentize-py's 0.3 support matures. The conformance suite runs against components exactly as against native resources, and passing it is the future registry's admission gate (§24.4). Deliberately post-MVP: sandboxed distribution matters only after there is something worth distributing.

### 8.6 Tier 4 — subprocess adapters

The polyglot escape hatch and the compatibility bridge:

```text
any process
  → stdout: Arrow IPC stream    (preferred: schema-exact, fast)
          | NDJSON              (easy: inference + hints)
          | Singer messages     (SCHEMA/RECORD/STATE → descriptor/batches/StateDelta)
          | Airbyte protocol    (catalog + per-stream state honored)
  → firn-subprocess adapter → ordinary batches → ordinary runtime
```

Adapters translate foreign state into the typed checkpoint model: a Singer `STATE` blob becomes an opaque-but-versioned `ForeignState` delta scoped to the adapter resource and committed under the same firn-line invariant as native state — honesty about opacity beats pretended structure. Subprocesses are supervised: timeouts, stderr captured into the run trace, exit codes mapped onto Chapter 14's taxonomy. Arrow IPC and NDJSON ship at MVP; Singer and Airbyte follow immediately, being parsers over the same machinery.

### 8.7 What the authoring design rejected

Lua and every embedded-scripting-language-as-centerpiece proposal: no data ecosystem, no Arrow story, no typed editor experience — embedding one would make the worst tier the featured one. Dynamic Rust plugins: no stable ABI, and Tier 3 covers dynamic distribution with sandboxing besides. R and notebook runtimes: reachable through Tier 4 now, Tier 3 later; not kernel concerns. A bespoke DSL: Tier 0 is TOML/YAML with a published JSON Schema precisely so nobody builds or learns a new parser, LSP, or formatter.

## Chapter 9: Batches

The unit of data is the batch, and a batch is more than its Arrow payload: it is payload plus the identity and provenance that evidence and replay require.

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
    op: Option<OpColumn>,               // presence of _firn_op for CDC-shaped batches
}
```

Each field earns its place. `schema_hash` is per-batch, not per-run, because mid-stream drift is real and contracts must catch it at the batch where it happens. `source_position` makes packages replayable *against the source* for verification, not merely from stored bytes. `stats` are computed once on the hot path with vectorized kernels and spent three times — profiler, package manifest, and destinations that can prune (a merge that knows a batch's key range skips scanning outside it). `Opaque` payloads let file-shuttling resources (move these objects, checksum them, record them) live inside the same evidence-and-checkpoint discipline without pretending to be columnar.

Rows never exist as a runtime concept. Row-shaped authoring converts to batches at the boundary; per-row logic inside the engine is a vectorized kernel over batches. The one place a row survives is the quarantine artifact, where a single row is the natural grain of an error report.

## Chapter 10: Contracts

### 10.1 A contract is a compiled program

A contract is not configuration consulted by scattered conditionals. `firn-contract` compiles a policy plus an observed schema into a **validation program**: an ordered set of vectorized checks and coercions with a total verdict lattice. The same compiled program is what `firn explain` prints, what `ContractExec` runs, and what the package serializes — one answer with three views to the question "what did the contract do to this run?"

```text
ContractPolicy × ObservedSchema → ValidationProgram
  ├─ schema verdicts, per new-table / new-column / type-change:
  │     admit | admit_as_variant | quarantine | reject_batch | reject_run
  ├─ column programs: casts, decimal/tz enforcement, domain checks,
  │     nullability checks, dedup keys
  └─ row disposition: accept | quarantine(rule_id) | fail
```

### 10.2 The operator chain

```text
ResourceBatchStream
  → SchemaFingerprintExec     # hash observed schema; catch drift at the batch it occurs
  → ContractExec              # run the ValidationProgram; split accept / quarantine
  → NormalizeExec             # namecase-v1, nested policy, variant capture, _firn columns
  → ProfileExec               # stats aggregation, freshness, PK-uniqueness sketch
  → LineageExec               # stamp resource, partition, position, run, package
  → PackageSink               # segment into the package; propose state deltas
```

`ContractExec` and `NormalizeExec` are DataFusion `ExecutionPlan`s over the accepted stream; the quarantine side channel is framework-owned per D-3, flowing into `quarantine/part-*.parquet` with `(row, rule_id, error_code, source_position, observed_value_redacted)`. Redaction obeys semantic tags: a `pii:*` field's offending value is hashed in the artifact, never stored.

### 10.3 Policy vocabulary

```text
schema:   allow_new_table · allow_new_column · allow_type_widening · quarantine_type_narrowing
          allow_unknown_fields · variant_capture · freeze
types:    coerce_types · preserve_decimal_exactness · preserve_timestamp_timezone · allow_lossy_mapping
rows:     nullability · domain/enum · range · regex · freshness · dedup(keys, keep = first|last|fail)
verdicts: admit · admit_as_variant · quarantine · reject_batch · reject_run
```

`reject_run` exists, and the compiler warns when it guards low-severity rules: a run-fatal contract on a wide table converts one bad cell into an outage, so verdict severity is part of the policy on purpose.

### 10.4 Trust levels compile to presets

The five-V analysis (Appendix B) surfaces in the API as one field, `trust`, whose four values expand into full policies users override piecemeal:

```text
experimental:  evolve everything, variant-capture unknowns, sampled profiling, quarantine off
governed:      evolve columns with a review artifact, full validation, quarantine on, packages retained
financial:     frozen schema, decimal/tz enforcement mandatory, full lineage, receipts required,
               reconciliation counts recorded, long retention
serving:       frozen schema, freshness SLO armed, sampled fast-path after N clean runs,
               demote-on-anomaly
```

These are the original "lanes," renamed to what they are — planner presets keyed on declared intent — and stripped of any temptation to become marketing.

### 10.5 Promotion and demotion

A resource's *effective* validation depth moves along a recorded ladder: new resources run discovery-depth regardless of trust; after N consecutive clean runs on a stable schema hash, validation drops to the sampled fast path where trust permits; any drift, anomaly spike, or quarantine event demotes back to full depth. Every transition is a ledger event, so "why did last night's run get slow" has a queryable answer — it demoted, and here is the batch that caused it.

### 10.6 Transforms, bounded

In-flight transforms are those expressible per batch with a plan-time schema: rename, cast, redact, derive via DataFusion expressions or registered UDFs, filter, expand nested structures per policy. They run inside `NormalizeExec` and are recorded like every other decision. Anything needing cross-resource joins, whole-table state, or post-load modeling belongs to the transformation tool downstream; the project layer triggers dbt or SQLMesh after a load and hands it the receipt — integration, not absorption.


## Chapter 11: Load packages — the firn itself

### 11.1 What a package is

A load package is the durable, hash-addressed record of one attempted state transition for one resource-partition set: the data, the decisions, and the proofs. Staging is a side effect; evidence is the identity. Everything the run planned, observed, decided, wrote, and was told by the destination lives in the package or is referenced by it — this is the compacted layer between snowfall and ice, and like real firn, a package that never crosses the line melts without consequence.

```text
pkg_01J.../                        # directory name = manifest-hash prefix + ULID
  manifest.json                    # identity, file hashes, counts, lifecycle status, signature slot
  plan/
    resource_plan.json             # negotiated ScanPlan, per-predicate fidelity included
    execution_plan.txt             # DataFusion EXPLAIN (physical), verbatim
    validation_program.json        # the compiled contract (§10.1)
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

### 11.2 Format decisions, elaborated

Arrow IPC for `data/` because a package must round-trip the *exact* observed schema — dictionary encodings, field metadata, extension annotations — and Parquet's type system is a lossy projection of Arrow's. Parquet for `stats/`, `quarantine/`, and `lineage/` because those are analytical artifacts queried across many packages: `firn sql` over a glob of packages is a supported and encouraged pattern. `firn package archive` transcodes `data/` to Parquet with a fidelity report for cold storage and lake interchange; the manifest records both forms and their hashes, and replay prefers IPC when present. The IPC choice carries a maintenance obligation named in D-28: arrow-rs majors land quarterly, so IPC byte-stability across the pinned upgrade is a golden-suite release gate, and if forward-compatibility ever bites in practice, D-4's revisit trigger flips the canonical/archive roles.

### 11.3 Lifecycle and the crash matrix

```text
planned → extracting → validated → packaged → loading → loaded → committed → checkpointed → archived
```

`manifest.json` carries the status, updated by atomic rename-over; status plus the checkpoint ledger jointly determine crash behavior. The matrix is normative:

| Crash point | Detection | Recovery |
|---|---|---|
| during extract/validate (pre-`packaged`) | manifest status < `packaged` | partial package is garbage unless the resource claims `replay_from_position`; `firn resume` restarts the partition or resumes from the last durable segment + recorded position |
| after `packaged`, before any destination write | status `packaged`, no receipts | replay the package into the destination; no source contact needed |
| mid-load | partial receipts / idempotency token present at destination | transactional destinations roll back and replay; idempotent destinations re-drive remaining segments keyed by token |
| after destination commit, before checkpoint commit | receipt durable in `receipts.json` and/or `_firn_loads`; ledger delta uncommitted | verify the receipt (§13.4), then commit the checkpoint — **this window is what the firn line exists for; recovery is a pure ledger operation** |
| after checkpoint commit | ledger committed | nothing; the next run reads committed state |

The chaos layer (§19.2) kills the process at each boundary in CI, on every merge, forever.

### 11.4 Retention and GC

Evidence has a budget. Retention is policy per trust level and environment (`dev: last 5 runs`; `financial` in prod: 400 days), enforced by `firn package gc`, which refuses to collect any package that is the sole proof of a committed checkpoint still inside its retention window, and which leaves a tombstone — manifest and hashes survive, data deleted — so the ledger's referential integrity outlives the bytes. This is the glacier flowing: old layers thin, but the record of the layer never disappears.

### 11.5 Hashing and the signature slot

Every file is SHA-256'd; the manifest lists `(path, bytes, sha256)`; the manifest itself is canonicalized (JCS-style key ordering) and hashed, and that hash is the package's identity everywhere — ledger, receipts, CLI, idempotency tokens. The manifest reserves `signature: null` with a defined signing input (the manifest hash), so post-MVP signing changes zero layouts, and the same slot is the trust surface when third-party connector distribution arrives. Two packages with equal manifest hashes are the same evidence; the golden-package suite rests on exactly that property.

## Chapter 12: Checkpoints and state — the ice

### 12.1 State is a ledger

State is an append-only ledger of typed transitions, never a mutable dict. The current cursor of a resource is a *view* — the latest committed transition — not a cell that gets overwritten. This one representational choice buys rewind, audit, bisection ("which run first wrote garbage?"), and safe concurrent readers for free, and it is the literal sense in which committed state is ice: layered, ordered, readable, never silently rewritten.

```sql
-- firn-state-sqlite, WAL mode. The shape is normative; the dialect is not.
CREATE TABLE checkpoints (
  checkpoint_id   TEXT PRIMARY KEY,           -- ULID
  pipeline_id     TEXT NOT NULL,
  resource_id     TEXT NOT NULL,
  scope           TEXT NOT NULL,              -- §12.4 serialized scope key
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

### 12.2 The invariant, mechanized

`CheckpointStore::commit(checkpoint_id, receipt)` is the only path from `proposed` to `committed`, and it verifies *structurally* — not by convention — that the receipt covers the package hash and every segment the delta represents. There is no API for writing a committed checkpoint directly; the firn line is a type-system fact before it is a discipline. Rewind (`firn state rewind --to chk_x`) appends a `rewound` marker and moves the head; it never deletes history, and it prints which committed packages now sit "ahead of state" so the operator understands the replay that follows.

### 12.3 Positions are typed

```text
CursorPosition  { field, value, window_close: Option<ts> }         # window-close per §7.2 lag
LogPosition     { stream, lsn | offset, tx_boundary }
FileManifest    { prefix, files: [(path, etag | checksum, bytes)] } # object-store incrementals
PageToken       { opaque, issued_at }                               # honest about opacity, still scoped & versioned
Composite       { parts: BTreeMap<PartitionId, Position> }
ForeignState    { protocol: singer | airbyte, blob, blob_hash }     # adapter tier, quarantined type
```

`state_version` gates deserialization; migrating a position type is an explicit, tested path (`firn state migrate`) with fixtures from every prior version in CI — because silently reinterpreting old positions is how backfills get skipped.

### 12.4 Scopes

A scope key namespaces state below the resource: `partition:<id>`, `window:<lo>..<hi>`, `file:<path>`, `stream:<name>`, `schema-contract`, `destination-load`. Different archetypes checkpoint at different grains — a file manifest per prefix, a window per cursor slice, one log position per stream — and forcing them through one cursor string is the Singer mistake this table exists never to repeat.

### 12.5 The store trait

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

SQLite (WAL) and in-memory ship at MVP. The trait contract — single-writer per scope, atomic commit, monotone history — is itself conformance-tested, so future Postgres or object-store implementations (with lease fencing for distribution) prove themselves the way destinations do.

### 12.6 Destination-mirrored state

Where the destination holds tables, the loader also maintains `_firn_loads` (package hash, resource, schema hash, counts, receipt, committed_at) and `_firn_state` (latest committed position per scope as of the loads it has seen). Three payoffs. Disaster recovery: `firn state recover --from-destination` reconstructs ledger heads from receipts when the local ledger is lost, with an explicit warning that quarantine and lineage evidence is not reconstructible. Warehouse-native audit: the query every dlt user already writes against `_dlt_loads` works here with more to say. Drift detection: a scheduled `firn doctor` compares ledger heads to destination mirrors and flags divergence — the observable symptom of someone loading around the tool.

## Chapter 13: Destinations

### 13.1 A destination is a commit protocol

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

`plan_commit` runs at plan time and is dry-runnable — it is where "this merge will need DDL: add column X" appears in `firn explain`. `finalize` is the moment of truth. The shape forces the Airbyte lesson structurally: the only way to hand firn a receipt is to return from `finalize`, and firn will not commit a checkpoint without one.

### 13.2 Capability sheets

A `DestinationSheet` is declared data shipped with the driver and snapshotted into the lockfile: supported dispositions; transaction support (`full | per_table | none`); idempotency mechanism (`package_token | merge_keys | none`); bulk paths (`arrow_ipc | parquet | csv | insert`); the type-mapping table of §6.3; identifier rules (max length, charset, case behavior); migration support per operation (`yes | staged | no`); quarantine-table support; concurrency constraints. The planner consumes the sheet, the conformance suite falsifies it, `firn explain` cites it. Nothing about a destination is folklore.

### 13.3 Dispositions

MVP ships `append`; `replace` (atomic swap where the destination allows — write-then-rename or transactional truncate-insert, never delete-then-insert); and `merge` (upsert on primary/merge key, with the batch dedup policy applied first so merges are deterministic under at-least-once redelivery). `cdc_apply` — ordered application of `_firn_op` insert/update/delete by source position — ships with the first log-CDC source. `scd2` and `snapshot` are deliberately absent from the loader: they are transformations wearing a disposition costume, and admitting them early smuggles modeling semantics into the commit path. `file_materialize` is the Parquet destination's native mode rather than a separate disposition.

### 13.4 Receipts

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
             "expr": "select count(*) from raw.github_issues where _firn_load = 'pkg:9c1e...'"}
}
```

The `verify` clause is the receipt's teeth: every destination declares how a third party — or the crash-recovery path — confirms the receipt against the destination itself. Transactional destinations declare a probe query; object stores declare a `(key, etag)` manifest; append-only systems declare count-plus-checksum. Recovery in the committed-but-not-checkpointed window runs `verify` before committing the checkpoint, closing the loop between claim and reality.

### 13.5 Idempotency, replay, and the guarantee table

Every commit carries the package hash as its idempotency token. Destinations with `package_token` support record it — a `_firn_load` column, a load-table row, an object key — and make re-driving the same package a no-op answered with a `duplicate: true` receipt. Destinations with only `merge_keys` idempotency achieve replay-safety for `merge` but not `append`, and the planner says so. `firn replay package <pkg> --to <dest>` is therefore always safe when in doubt, converting the scariest moment in ingestion — "did that load land?" — into a command.

The guarantee table, derived mechanically from resource capabilities × disposition × destination sheet and printed by every `firn plan`:

| Extraction | Disposition | Destination idempotency | Observed guarantee |
|---|---|---|---|
| at-least-once | merge (PK) | any | effectively-once per key |
| at-least-once | append | package_token | effectively-once per package |
| at-least-once | append | none | at-least-once; duplicates possible on replay — planner warns |
| at-least-once | replace | atomic swap | effectively-once per target |
| at-least-once | cdc_apply | package_token + ordered apply | effectively-once per position |

The unqualified phrase "exactly-once" appears nowhere in the product. The table is what is true instead.

### 13.6 The first three destinations, with their honest edges

**DuckDB first**, because it closes the local loop: Arrow appender for hot batches, Parquet ingestion for replay, transactional commits, and instant `firn sql` gratification. Two honest edges go straight into its sheet rather than into a footnote. Single-writer database files: concurrent runs targeting one file serialize on a firn-level lock rather than corrupting. And the bundled-build ICU gap: the Rust crate's bundled DuckDB omits the ICU extension (crates.io size limits), which affects timezone-aware operations — for a system whose type rules make timezone honesty non-negotiable (§6.2), the destination driver either loads ICU at runtime or links a full build, and `firn doctor` checks which. The crate's DuckDB-encoding version scheme and LTS branch feed the D-28 pin.

**Parquet/object-store second**, because it exercises partition layout, manifests-as-receipts (`(key, etag, sha256)` lists), lake-style directory schemes, and is the archive path's twin — and because it is the designed seam for the lakehouse destinations of D-27: Iceberg and Delta drivers arrive post-MVP through iceberg-rust and delta-rs, with the table format's own snapshot metadata embedded in the firn receipt so the firn line and the table's transaction log corroborate each other.

**Postgres third**, the transactional reference: real DDL migration, real `ON CONFLICT` merge, xid-bearing receipts, and the same server exercising the source side. Warehouses (Snowflake, BigQuery, Databricks) arrive after MVP through the same sheet-plus-conformance gate as everything else; breadth is a consequence of the protocol being provable.

## Chapter 14: Errors, retries, and the HTTP toolkit

One taxonomy, used by every tier and crate:

```text
Transient    network flap, 5xx, lock timeout        → jittered backoff at request level
RateLimited  429, quota headers, driver throttle    → obey Retry-After / token bucket; never a failure
Auth         401/403, expired credential            → one refresh via SecretProvider, then fail actionably
Contract     validation verdicts                    → not errors; verdicts (ch. 10) — never retried
Data         malformed payloads beyond contract     → quarantine or fail per policy; never blind-retried
Destination  commit/migration failures              → retry only restartable sessions; else surface with receipt state
Internal     bugs                                   → fail loudly, full trace, no retry
```

Retries operate at the smallest safe unit — request, then partition (re-planned when the resource claims `replay_from_position` or `idempotent_reads`), then run — under a run-level retry budget so a dying API fails a run in minutes, not hours. A partition retry after partial packaging resumes from the last durable segment plus recorded position: the crash matrix and the retry path are the same code, which is the payoff of building recovery first.

`firn-http` exists because every API resource dies on the same five rocks: pagination drift, rate limits, auth refresh, retry storms, connection reuse. The toolkit provides paginators (cursor, page, offset, link-header, next-token — with response-shape auto-detection in the spirit of dlt's `paginate()`, but always recording *which* paginator it chose into the plan, because auto-detection that leaves no trace is a debugging tax), a token-bucket limiter that respects server headers, backoff with budget accounting, session-scoped auth with refresh hooks into `SecretProvider`, and per-request tracing landing in `trace.jsonl` with secrets redacted before formatting. Declarative and Python tiers get it implicitly; Rust gets it as a crate; WASM guests get it as the host import.

## Chapter 15: Observability

firn's first observability surface is its own artifacts. Ledger, packages, receipts, and mirrors are queryable data; `firn sql` mounts them, and

```sql
select resource_id, sum(rows_written)
from firn.loads
where committed_at > now() - interval '1 day'
group by 1
```

works out of the box, because the ledger is SQLite, the stats are Parquet, and DataFusion is right there. Introspection is a query, not a log grep.

Above that, `tracing` instruments every phase with span fields for run, resource, partition, and package IDs; OTLP export is a feature flag. Three derived signals ship as CLI verbs rather than dashboard suggestions: `firn doctor` (environment health, secret resolvability, Python interpreter status, ledger↔destination drift); `firn inspect run <id>` (the run's whole story — plan, verdicts, receipts, transitions); and `firn status`, which evaluates `FreshnessSpec` SLOs and exits nonzero when a `serving`-trust resource is stale, making alerting a cron line.

## Chapter 16: Security

### 16.1 Secrets

`secret://provider/key` URIs are the only form a credential takes in any artifact firn writes — project files, lockfiles, plans, packages, traces, EXPLAIN output, error messages. Resolution happens inside `SecretProvider` implementations (MVP: env, file, OS keychain; post-MVP: Vault and cloud secret managers) at the moment of use, and resolved values live in zeroizing wrappers registered with the redaction layer, so a value leaking into a panic message is scrubbed by construction — the formatter consults the registry — rather than by pattern-matching after the fact.

### 16.2 Trust boundaries by tier

Tier 0/1 code is the operator's own; it runs with process privileges, and the threat model is mistakes — which plans and contracts address. Tier 2 Python is trusted-but-instrumented: watchdogs, resource limits where the OS provides them, and the framework-brokered `ctx` as the encouraged path to the network. Tier 3 is the untrusted-code answer: capability-scoped WASI 0.3, host-mediated HTTP so limits, redaction, and egress allowlists bind strangers' code, no ambient authority. Tier 4 inherits OS process isolation plus supervised stdio. The project file can declare an egress allowlist per source — enforced in `firn-http` and the WASM host — turning "this connector only talks to api.github.com" from documentation into policy.

### 16.3 Supply chain

`cargo deny` and `cargo vet` gates in CI; lockfiles committed everywhere, including the Python SDK examples; release binaries built reproducibly and checksummed; and the package signature slot doubling as the connector-signing story when Tier 3 distribution arrives. The dependency-pin policy of D-28 is itself a supply-chain control: no load-bearing dependency moves in a patch release, ever.


# Part III — The Surface

## Chapter 17: The CLI

Design rules: every command runs headless, exits with meaningful codes, and takes `--json`; every noun in the architecture — resource, plan, package, checkpoint, receipt, contract, schema, run — has an `inspect`; nothing requires a daemon.

```bash
firn init                                          # scaffold project + one example resource per tier
firn validate                                      # schema-check project & resources; CI's first line
firn plan github.issues                            # compile; print plan, guarantee, DDL preview
firn explain github.issues --where "updated_at >= '2026-07-01'" --columns id,title
firn run github.issues --to duckdb://local.duckdb
firn run --all --env prod
firn preview github.issues --limit 500             # one batch: schema + sample, nothing written
firn sql "select state, count(*) from github.issues group by 1"
firn inspect resource|package|run|receipt <id>
firn diff schema github.issues --against last
firn contract freeze|show|test github.issues
firn state show|history|rewind|migrate|recover github.issues
firn resume                                        # drain interrupted work per the crash matrix
firn replay package <pkg> --to <dest>              # idempotent by construction
firn backfill github.issues --from 2026-01-01 --to 2026-07-01
firn package ls|gc|verify                          # archive arrives fast-follow
firn doctor                                        # env, secrets, python, ICU, ledger↔destination drift
firn status                                        # freshness SLOs; nonzero exit on breach
```

Two commands carry the developer-experience thesis. `firn preview` is Mage's inspect-one-batch loop without a UI. `firn plan` is the category shift of §1.2 made tangible: what will be fetched, what pushes down and at what fidelity, what DDL might run, which guarantee applies, which state advances — reviewable in a pull request before anything moves.

## Chapter 18: The project format

### 18.1 Shape

```toml
# firn.toml
[project]
name = "acme_data"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.firn/state.db"
packages = ".firn/packages"
destination = "duckdb://.firn/dev.duckdb"
retention = { default = "5 runs" }

[environments.prod]
state = "sqlite:///var/lib/firn/state.db"
packages = "s3://acme-packages/firn"
destination = "postgres://secret://aws-sm/prod-dwh"
retention = { default = "90d", financial = "400d" }

[python]
interpreter = ".venv/bin/python"       # firn doctor reports whether it is free-threaded

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

### 18.2 Environments and secrets

Environments overlay the project map; the unspecified inherits. Secrets appear only as URIs, so the file is committable by construction, and `firn validate --env prod` confirms resolvability without printing values.

### 18.3 The lockfile locks semantics, not just versions

`firn.lock` snapshots what Git review should catch: resolved crate/SDK versions and the D-28 dependency tuple; each resource's capability-sheet hash; each destination's full sheet including its type-mapping table; contract snapshots and schema hashes per resource; the normalizer version. A dependency bump that changes a type mapping, drops a capability, or alters normalization becomes a reviewable diff before it becomes a production surprise — Meltano's lockfile lesson, extended from packaging to meaning.

## Chapter 19: Conformance and testing

### 19.1 The resource suite

Any resource — native, declarative, Python, WASM, subprocess — runs one suite: descriptor coherence (declared keys exist in the schema; the cursor field exists and orders as claimed); capability truth-telling (every claimed pushdown exercised against engine-side ground truth, `Exact` claims falsified with adversarial timezone/collation/null edges); partition completeness (the union of partitions equals the unpartitioned scan); position replay (re-opening at a recorded position reproduces the suffix, for resources claiming it); boundedness honesty. Passing the suite *is* the definition of "supported," and the suite is public, so third-party connectors prove themselves exactly as first-party ones do.

### 19.2 The destination suite and the chaos layer

Sheet truth-telling: every declared disposition, migration operation, and idempotency mechanism exercised. Receipt verification: every `verify` clause shown to confirm honest loads and to fail tampered ones. The crash matrix: the chaos layer kills the process at each lifecycle boundary; recovery must terminate in a consistent state with no cursor ahead of durable data. Replay identity: the same package driven twice yields `duplicate: true` or key-idempotent state, per the sheet. Type mappings round-trip at fidelity `exact`.

### 19.3 Golden packages and determinism

Given a fixed source fixture, a run must produce a package whose manifest hash equals the committed golden hash — across a hundred repetitions and across operating systems. This forces the discipline that makes evidence trustworthy: canonical JSON everywhere; ULIDs seeded in test mode; adaptive batch sizes recorded, with replay using recordings rather than re-deriving; wall-clock timestamps confined to enumerated unhashed manifest fields. When a change legitimately alters output, the golden diff *is* the review artifact. The suite doubles as the D-28 upgrade gate: an arrow-rs or DataFusion pin bump that perturbs IPC bytes or plan text is caught here, before release.

### 19.4 Property and fuzz targets

Schema inference (arbitrary JSON → schema → generated data → re-inference is a fixed point); the contract compiler (the verdict lattice is total — every cell of every batch receives exactly one disposition); position serialization round-trips across `state_version` migrations; the NDJSON, Singer, and Airbyte parsers under adversarial input.

## Chapter 20: Compatibility

### 20.1 Posture

Compatibility layers are bridges into the native model, never alternate front doors. Everything arriving over a bridge becomes descriptors, batches, typed (or explicitly `ForeignState`) checkpoints, and packages, and is planned, evidenced, and committed like everything native. No bridge bypasses the firn line.

### 20.2 The dlt shim

`firn-python` ships an adapter running `@dlt.resource` / `@dlt.source` functions unmodified where feasible: dlt hints — primary key, merge key, `incremental`, write disposition, contract modes — map onto `ResourceDescriptor`; `dlt.current.state` maps onto a scoped state view backed by the ledger; dlt's contract-mode vocabulary (`evolve`, `freeze`, `discard_row`, `discard_value`) maps onto firn verdicts (`admit`, `freeze`, `quarantine` at row and column grain). The shim's purpose is migration gravity — the largest library-first authorship base can try firn with existing code and immediately gain plans, packages, and the guarantee table. Its non-goal is bug-for-bug dlt emulation; divergences are a documented migration table, and the deepest divergence is the point: dlt normalizes and loads what arrived, firn plans what to fetch.

### 20.3 Singer and Airbyte adapters

Tier-4 parsers per §8.6. Airbyte's per-stream state and destination-acknowledgment expectations map cleanly — they inspired the invariant. Singer's looser state becomes `ForeignState`, honestly scoped. Airbyte *destinations* are explicitly out of scope: the destination side is where the receipt protocol lives, and delegating it to a foreign container would forfeit exactly the guarantees firn exists to provide.

## Chapter 21: Versioning, licensing, governance

Apache-2.0. One repository. Crates on crates.io under semver; pre-1.0, minor releases may break Rust APIs but **may never break serialized artifacts without a migration**. The artifacts — checkpoint schema (`state_version`), package manifest version, capability-sheet format, the `firn:resource` WIT world, the declarative JSON Schema — are independently versioned specs living in-repo with committed migration paths and CI fixtures from every prior version. The artifacts are the contract with operators; the Rust API is merely the contract with developers; the former outranks the latter from the first release. Upstream cadence is governed by D-28: one pinned dependency tuple per firn minor, upgraded deliberately, gated on golden packages, never moved in a patch.

---

# Part IV — The Road

## Chapter 22: MVP

### 22.1 Contents

Kernel, engine, contract compiler, package builder/replayer, SQLite ledger; authoring tiers 0 (REST/SQL/files), 1, 2, and 4 (Arrow IPC + NDJSON); `firn-http`; destinations DuckDB, Parquet/object-store, Postgres; sources HTTP-paginated API, Postgres snapshot/incremental, Parquet/CSV/JSON files; dispositions append, replace, merge; the Chapter 17 CLI minus `package archive`; both conformance suites and the chaos layer; golden packages in CI; the dlt shim in preview.

### 22.2 The cutline

Explicitly out, each with a designed seam: the WASM tier (WIT world already specified against WASI 0.3); Singer/Airbyte parsers (fast-follow over existing adapter machinery); log-based CDC and `cdc_apply` (positions already typed); streaming supervisor (`Boundedness` already carried); distributed execution (partitions and packages already the units; Ballista to be evaluated); warehouse and lakehouse destinations (sheet + conformance gate; Parquet destination is the Iceberg/Delta seam); package signing (slot reserved, input defined); non-SQLite ledger backends (trait sized, conformance-tested); vault-class secret providers; any UI.

### 22.3 The killer demo

Forty lines of Tier-0 TOML define `github.issues`. `firn plan` prints pushdown fidelity, the guarantee line, and pending DDL. `firn run` loads DuckDB; `firn sql` queries it with pushdown visible in EXPLAIN. `firn contract freeze`; the fixture drifts a column type; the next run quarantines the offenders and the package shows the verdicts. `kill -9` lands between destination commit and checkpoint commit; `firn resume` verifies the receipt against DuckDB and commits the checkpoint without touching the source. `firn replay` drives the same package into a second database; the second attempt against the first answers `duplicate: true`. `firn state history` shows every transition. Under five minutes on a laptop, no network beyond GitHub.

## Chapter 23: Research spikes

Eight, with exit criteria that decide rather than describe:

1. **Queryable HTTP resource.** A paginated API as `QueryableResource` through the engine adapter. Exit: the §7.4 EXPLAIN output is real, and request counts drop measurably under projection + filter + limit versus a naive full scan.
2. **Contract compiler + ContractExec.** Exit: 100k-row batches validate at >1 GB/s for type/null/domain rules on a laptop; quarantine artifacts round-trip; the compiled program serializes into a package and reloads.
3. **Package determinism.** Exit: golden-hash stability across 100 runs and across macOS/Linux.
4. **Crash-matrix mechanization.** Exit: the chaos layer covers all five boundaries against DuckDB and Postgres with zero manual steps.
5. **Python bridge.** Exit: PyCapsule crossing measured zero-copy for batches ≥1 MiB; on a GIL build, lock held <5% of wall time on an I/O-bound resource; on 3.14t, two `parallel` resources demonstrate real concurrency with identical output hashes to the GIL run; pyright clean on the SDK examples. This spike is the D-25 proof.
6. **Adaptive batch controller.** Exit: throughput within 10% of hand-tuned fixed sizing across three source shapes, with recorded sizes replaying deterministically.
7. **Destination-sheet joins.** Exit: plan-time type-mapping and identifier checks catch seeded lossy and colliding fixtures on all three MVP destinations, including the DuckDB ICU/timezone case.
8. **Five-V harness as tests.** The Appendix B matrix encoded as integration scenarios (high-variety/low-veracity: a thousand mutating JSON schemas through discovery trust; high-volume: the spill path under a 256 MiB budget). Exit: each scenario names the mechanisms that activated — from the ledger, automatically.

## Chapter 24: Beyond MVP

### 24.1 Fast-follow

Singer/Airbyte adapters; `firn package archive`; the dlt shim to GA; vault-class secret providers; the first warehouse destination through the sheet + conformance gate.

### 24.2 Distributed execution

A scheduler assigning `(resource, partition)` leases to workers; a shared `CheckpointStore` (Postgres or object-store with lease fencing); package directories on object storage. No shuffle, no new artifact types, no resource rewrites — and before building a scheduler, an honest evaluation of Ballista, DataFusion's own distributed subproject, as substrate versus inspiration. The acceptance test for the claim that the seams were real: both conformance suites pass unchanged against distributed workers.

### 24.3 The streaming supervisor

A resident process running `Unbounded` plans continuously. Checkpoint cadence, package rotation, and watermarks already exist (§5.5); the supervisor adds lifecycle — drain, pause, resume-from-head — and log-CDC sources whose `LogPosition` state the ledger already models. Streaming arrives as an executor mode over unchanged types: the payoff of carrying `Boundedness` from the first commit.

### 24.4 WASM distribution and the registry

Tier 3 ships on the WASI 0.3 baseline; the conformance suite becomes the registry's admission gate; capability sheets and signed manifests become the trust surface for running strangers' connectors; WASI 1.0, when it lands, is the interface-freeze milestone. Connector breadth begins here, after the protocol is provable — the deliberate inversion of the connector-zoo-first industry default.

### 24.5 Lakehouse destinations

Per D-27: `firn-dest-iceberg` and `firn-dest-delta` over iceberg-rust and delta-rs, committing packages into table formats whose own snapshot metadata rides inside the firn receipt, so the firn line and the table's transaction log corroborate each other — two ledgers agreeing beats one ledger trusted.

---

# Part V — The Audit

## Chapter 25: Verification

### 25.1 The criteria

Fixed before the final passes, applied to every chapter, and intended to outlive this book as review criteria for the implementation:

- **V1 — Invariant totality.** Every state-advancing path routes through `CheckpointStore::commit` with a receipt; the crash matrix covers every lifecycle boundary; no unqualified exactly-once claim anywhere; every guarantee derivable from §13.5's table.
- **V2 — Layer discipline.** The kernel compiles with arrow-rs alone — no DataFusion, DuckDB, Python, or network; no upper layer is required by a lower one; nothing with a screen is load-bearing.
- **V3 — Authoring reality.** Every shipping tier has concrete code in this book, a stated editor and type-checking story, a named data-crossing mechanism with its copy cost, and a trust-boundary classification.
- **V4 — Every noun earns its keep.** Every named concept has an owning crate, a lifecycle, a serialized form, and a CLI verb that inspects it. Concepts failing the test are cut, not kept as aspiration.
- **V5 — Decisions, not vibes.** Every question the design has ever raised is answered in the register with a revisit trigger; every rejected alternative is named with its reason; every tradeoff states its cost.
- **V6 — The five Vs stay backstage.** No public API, CLI flag, or artifact field named for velocity, volume, variety, veracity, or value; their influence appears only as mechanisms and as the Appendix B test harness.
- **V7 — Buildable.** The MVP has a cutline; every cut item has a designed seam; the killer demo exercises the invariant, contracts, packages, replay, and crash recovery in one sitting.
- **V8 — Honest fidelity.** Wherever data can silently lose meaning — decimals, timezones, identifiers, type mappings, pushdown semantics, foreign state — the design makes the loss impossible, plan-visible, or contract-gated, and says which.
- **V9 — Grounded currency.** *(new this revision)* Every claim about a dependency's capability or status is checked against its mid-2026 published state and recorded in Appendix C; where reality moved (free-threaded Python, WASI 0.3, arrow-rs cadence, duckdb-rs versioning, object_store's new home), the design moved with it, in the register, not in a footnote.
- **V10 — One voice.** *(new this revision)* The book reads as one argument: consistent nouns, consistent cross-references, the metaphor used where it names mechanism (firn line, ice, melt) and absent where it would decorate.

### 25.2 The pass log

**Pass 1 — alignment against the handoff (first revision).** Every handoff section expanded, compressed with a pointer, or ripped with a reason. Fixed then: the monolithic `Resource` trait split; plan-time lightness made structural (`negotiate` plans, `open` works); quarantine resolved against DataFusion's single-output model; `_dlt_loads` admiration turned into the mirror mechanism.

**Pass 2 — criteria sweep (first revision).** V1: partition retries unified with the crash-matrix resume path. V2: contract expression *lowering* moved engine-side so the compiler stays kernel-side. V4 kills: standalone devtools binaries; `snapshot`/`scd2` dispositions; a freestanding run-ledger concept folded into the checkpoint ledger with `firn inspect run` as its verb. V6: lane vocabulary removed. V8: `CursorSpec` lag and window-close semantics added; the normalizer versioning rule added.

**Pass 3 — adversarial read (first revision).** Can Python deadlock the runtime? No — pool confinement, bounded channels, watchdogs. Can two runs corrupt one DuckDB file? No — sheet-declared constraint, firn-level lock. Can a lockfile-invisible change alter destination types? No — mappings are locked. Can replay double-append? Only on `append` with no token, and the planner warns — accepted as honest. Does anything require the network at plan time? Schema `Discover` did; plans may now run against the last snapshot with an explicit `--offline` staleness marker.

**Pass 4 — this revision's research sweep (V9).** Findings and their consequences: Python 3.14's free-threaded build is officially supported and PyO3 targets it → D-25, §8.4 rewritten, spike 5 extended with a free-threading exit criterion. WASI 0.3.0 released June 11, 2026 with native async streams → D-26, the WIT world rewritten to export `open` as an async func returning `stream<u8>`, the 0.2 pollable design deleted. arrow-rs and DataFusion cadences published (quarterly majors, monthly minors; object_store in its own Apache repo) → D-28 and the golden-suite upgrade gate. duckdb-rs's DuckDB-encoded versioning and bundled-ICU gap → §13.6's honest edges and a `firn doctor` check. dlt's 2026 state (declarative REST source, contract modes over Arrow frames, delta/iceberg options, LLM-generated sources) → Chapter 2's re-survey, §8.2's agent argument, §20.2's mode mapping, and D-27. The PyCapsule protocol's maturity and pyo3-arrow's pyarrow-optional stance → §8.4's "pyarrow is not a hard dependency."

**Pass 5 — cohesion edit (V10).** Read end-to-end as an editor. Renamed every artifact, table, and URI from the placeholder to firn (`_firn_loads`, `_firn_op`, `_firn_variant`, `firn.toml`, `.firn/`, `firn:resource`). Unified cross-reference style to chapter/section numbers. Confined the metaphor to where it names mechanism — the firn line (§12.2), ice (§12.1), melt (§11.1), the glacier flowing (§11.4) — and cut three decorative uses. Verified every forward reference resolves and every code block's identifiers match the prose around it. Confirmed the guarantee line added to §7.4's EXPLAIN example matches §13.5's derivation. Residual open items, carried honestly: D-6's default-strictness telemetry question; whether `ForeignState` deserves its own migration tool; the exact free-threaded-Python default flip (D-25's trigger). Each keeps its revisit trigger rather than a pretended answer.

### 25.3 The rip list

Ideas examined and removed, with reasons — kept in the book because a design is defined as much by its refusals: the literal `VVector` (condemned in the handoff; barred by V6); lanes as product vocabulary (presets do the work without the branding); the monolithic resource trait (ergonomics); dynamic Rust plugins (no stable ABI; Tier 3 does distribution with sandboxing); Lua and every bespoke DSL (no ecosystem, no editor story); `scd2` and `snapshot` dispositions (transformations in costume); standalone devtools binaries (no user pulling); signing at MVP (hashing delivers integrity now; signatures deliver attribution, needed only when strangers' code ships); vector-store destinations at MVP (no designed semantics; would be a checkbox); Airbyte destination delegation (would forfeit the receipt protocol); connector breadth as a roadmap phase (breadth is an outcome of the conformance gate); and a WASI 0.2 pollable-based WIT world (obsoleted by 0.3 before it was ever built — the cheapest rip in the book).

### 25.4 North star

> Build firn: a Rust-native, DataFusion-powered kernel in which every resource is an optimizable Arrow stream, every plan is reviewable before it runs, every contract is a compiled program, every run compacts into hash-addressed evidence, every destination answers in receipts, and nothing crosses the firn line without proof — so that moving data becomes as inspectable, replayable, and boring as committing code.

---

# Appendix A — Glossary

**Resource** — smallest stateful unit of extraction; declares a descriptor and capabilities; produces batches. **Source** — configuration and discovery bundle over resources. **Batch** — Arrow payload plus identity and provenance. **Scan plan** — the negotiated read: pushdown fidelity, partitions, ordering, estimates. **Contract** — policy compiled into a validation program with a total verdict lattice. **Package** — hash-addressed evidence of one attempted transition; the firn. **Receipt** — a destination's durable, independently verifiable acknowledgment. **Checkpoint** — a committed state transition; one head per scope; the ice. **Firn line** — the commit boundary enforced by `CheckpointStore::commit`; nothing crosses it without a receipt. **Scope** — sub-resource state key (window, file, stream, partition). **Sheet** — declared, lockfile-snapshotted capability table for a resource or destination. **Trust level** — declared intent expanding into contract, validation, and retention presets. **Disposition** — destination write semantics: append, replace, merge, cdc_apply.

# Appendix B — The five-V stress harness (internal)

Retained as design pressure and encoded as tests (spike 8), never as API. Each axis names the mechanisms that absorb its extremes. **Velocity** → checkpoint cadence, package rotation, watermarks, backpressure, byte-bounded channels. **Volume** → partitioned plans, pushdown, spill, the memory pool, bulk destination paths, adaptive batches. **Variety** → discovery schemas, variant capture, nested policies, the versioned normalizer, per-batch schema fingerprints. **Veracity** → contract programs, quarantine, profiles, dedup, freshness, cursor-lag semantics, lineage. **Value** → trust levels, retention, receipt verification, reconciliation counts, demote-on-anomaly. The harness's one lesson, enforced: a small set of composable mechanisms, planner-selected from declared intent, beats thirty-two hand-built modes — and the public surface stays eight nouns wide.

# Appendix C — Dependency survey, July 2026

The engineering claims in this book rest on the following verified states; each entry names the design element it grounds. Versions are the state of the world at writing and are governed forward by D-28.

- **Apache DataFusion 54** (April–May 2026 cycle; 53 shipped LIMIT-aware Parquet pruning, dynamic filter pushdown through joins and subqueries, and order-of-magnitude cheaper plan cloning; majors land several times a year; Ballista maintained as the distributed subproject; a growing subproject family — Python, Java, Comet, an ADBC driver — evidences substrate health). Grounds: Chapter 2's proof-point argument, §4.3's pin policy, §24.2's Ballista evaluation.
- **arrow-rs 58.x → 59** (majors at most quarterly, minors monthly; deprecated APIs held roughly two majors; **object_store relocated to the apache/arrow-rs-object-store repository** on its own cadence). Grounds: D-28, §11.2's IPC byte-stability gate, the kernel's arrow-only dependency posture.
- **Arrow PyCapsule Interface** (standardized `__arrow_c_schema__` / `__arrow_c_array__` / `__arrow_c_stream__` with capsule destructors and schema negotiation; pyo3-arrow demonstrates pyarrow-independent zero-copy bridging). Grounds: §8.4's interchange design and the "pyarrow is optional" stance.
- **PyO3 0.28/0.29 and CPython 3.14** (free-threaded build officially supported per PEP 779; PyO3 modules default to declaring GIL-independence; `Python::detach` replaces `allow_threads`; abi3t stable ABI arriving with 3.15; 3.13t support dropped upstream in favor of 3.14t+). Grounds: D-25, §5.1, §8.4, spike 5.
- **WASI 0.3.0** (released June 11, 2026: native `async func`, `stream<T>`, `future<T>` in the canonical ABI; `wasi:io` absorbed; host-owned event loop across components; Wasmtime 43+ implements, with Component Model async default shortly after; cancellation and stream optimization on the 0.3.x train; WASI 1.0 targeted late 2026/early 2027; guest toolchains for Rust/Go/JS/Python in progress). Grounds: D-26, §8.5, §24.4.
- **DuckDB 1.5.x / duckdb-rs** (crate semver now encodes the bundled DuckDB version as 1.MAJOR_MINOR_PATCH.x; an LTS branch tracks 1.4 "Andium"; Arrow 58 interop; appender-arrow and vtab-arrow features; **bundled builds omit the ICU extension**, affecting timezone-dependent operations unless loaded at runtime or linked fully). Grounds: §13.6's honest edges, D-28's pin, `firn doctor`'s ICU check.
- **dlt (mid-2026)** (declarative `rest_api` source with auto-detected pagination; schema contracts with `evolve`/`freeze`/`discard_row`/`discard_value` across tables, columns, and data types, applied uniformly to Arrow tables and dataframes; Pydantic discriminated-union routing; delta/iceberg table formats on the filesystem destination; positioned as agent-native with thousands of LLM-generated declarative sources). Grounds: Chapter 2's re-survey, §8.2, §20.2's mode mapping, D-27.

# Appendix D — The rip list, consolidated

See §25.3. Reproduced as an appendix pointer so that readers who begin at the appendices — reviewers do — encounter the refusals alongside the glossary rather than only inside the audit chapter.
