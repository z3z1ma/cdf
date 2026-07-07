# CDF

CDF is the Continuous Data Framework.

It is a Rust-native, DataFusion-powered framework for data movement. Its job is
to make a load behave less like a connector run and more like a build: planned
before it has effects, captured as an artifact, checked against contracts,
committed through a destination receipt, and recoverable without guessing.

CDF is ambitious on purpose. It draws from dlt, Singer, Airbyte, Meltano, Sling,
Mage, Arrow, DataFusion, reproducible builds, conformance testing, and
operational systems that have to survive bad days. The goal is a next generation
data system, not a larger connector catalog.

If a pipeline reads from GitHub, writes to DuckDB, and moves a cursor, CDF wants
the repository, the package store, the destination, and the checkpoint ledger to
answer ordinary questions:

- What was the planned read?
- Which filters did the source handle, and with what fidelity?
- What schema and type information was observed?
- Which rows passed, froze, or went to quarantine?
- What package was produced?
- What did the destination commit?
- Which receipt proves it?
- Which state transition was recorded?
- Can the same package be replayed somewhere else?

That is one part of "continuous" here. CDF is also meant to handle real
continuous input: streams, generators, micro-batches, and resident execution.
The shared requirement is that each batch, window, or stream segment leaves
evidence that survives the process.

## what CDF is building

CDF is a system of explicit artifacts. No one artifact carries the whole design.

- A resource describes a stateful extraction unit: schema, keys, cursor,
  partitioning, contracts, trust, and capabilities.
- A scan plan says what will be read, what pushes down, what remains for the
  engine, what DDL might happen, and what guarantee applies.
- A contract compiles policy into a validation program with a verdict for every
  cell of every batch.
- A package is hash-addressed evidence of an attempted transition: Arrow data,
  schemas, verdicts, quarantine, lineage, stats, state deltas, commit plans, and
  receipts.
- A receipt is the destination's durable acknowledgment that data landed in a
  form CDF can verify later.
- A checkpoint is an append-only, typed state transition. It commits only after
  the relevant receipt exists and verifies.
- A capability sheet is a machine-readable claim about a resource or
  destination. The conformance suite tries to falsify it.
- A project file and lockfile make the meaning of a data project reviewable:
  resource bindings, destination sheets, type mappings, contract snapshots,
  normalizer versions, and dependency pins.
- The CLI gives every noun an inspection path, because a hidden artifact is not
  much better than no artifact.

The result should feel like infrastructure-as-code applied to ingestion. The
operator can review the plan before bytes move, inspect the package after a run,
query the ledger, recover after a crash, and replay the same evidence into a
different target.

## how a load moves

A normal CDF run moves through the same model whether the source was declared in
TOML, written in Rust, produced by Python, hosted in WASM, or bridged through a
subprocess.

1. The resource declaration lowers into descriptors and capabilities.
2. The planner negotiates projection, filters, limits, partitioning, ordering,
   pushdown fidelity, contracts, and destination work.
3. The runtime executes over Arrow batches, with DataFusion used where query
   planning and execution belong.
4. Contracts classify the data before it is treated as loadable.
5. The package builder records the data and the evidence around it.
6. The destination plans and finalizes a commit, returning a verifiable receipt.
7. The checkpoint store records the state transition as append-only history.
8. Recovery, replay, status, and audit commands use the same artifacts instead
   of asking the source to repeat itself perfectly.

CDF avoids slogans like "exactly once." It prints the real boundary: effectively
once per key, per package, per target, per position, or at least once with
duplicate risk. If a guarantee is not earned by the source, disposition, and
destination, the plan should say that before anything runs.

## authoring

CDF has several authoring paths because real data work enters from several
directions.

- Tier 0: declarative TOML or YAML for REST, SQL, and file resources.
- Tier 1: native Rust resources.
- Tier 2: Python resources through a typed SDK and Arrow interchange.
- Tier 3: WASM Components for sandboxed third-party distribution.
- Tier 4: subprocess adapters over Arrow IPC, NDJSON, Singer, or Airbyte.

The tiers are not separate runtimes with separate rules. They are doors into the
same kernel model: descriptors, partitions, Arrow batches, positions, packages,
receipts, and checkpoints.

This matters for people and for agents. A simple API should be expressible as a
small resource file. A harder source can move into Python or Rust. A third-party
connector can live behind WASM or a supervised process. Once it crosses into
CDF, it has to plan, package, commit, and pass conformance like everything else.

## operator surface

The finished CLI is intended to stay headless, scriptable, and inspectable.

```bash
cdf init
cdf validate
cdf plan github.issues
cdf explain github.issues --where "updated_at >= '2026-07-01'"
cdf preview github.issues --limit 500
cdf run github.issues --to duckdb://local.duckdb
cdf sql "select state, count(*) from github.issues group by 1"
cdf inspect resource|sheet|package|run|receipt <id>
cdf contract freeze|show|test github.issues
cdf state show|history|rewind|migrate|recover github.issues
cdf resume
cdf replay package <pkg> --to postgres://...
cdf doctor
cdf status
```

The command spelling may change while the implementation settles. The operator
experience should not: plan the read, run it, inspect the evidence, verify the
destination receipt, and keep state as history rather than folklore.

## trust and operations

CDF treats operational facts as data.

- The checkpoint ledger, load records, receipts, stats, and package manifests
  are meant to be queryable.
- `secret://...` URIs are the only credential form written to artifacts.
  Resolved values stay behind `SecretProvider` and the redaction registry.
- Trust levels such as `experimental`, `governed`, `financial`, and `serving`
  expand into contract, retention, freshness, and promotion behavior.
- Golden packages check that fixed inputs produce fixed output.
- The chaos layer kills runs at lifecycle boundaries so recovery code runs in
  CI, not for the first time in production.
- Serialized artifacts have versions and migrations. An old package should
  remain readable even when Rust APIs change.

## what CDF is not

CDF is not a scheduler. It should run under Airflow, cron, GitHub Actions,
Kubernetes, a future distributed supervisor, or whatever already owns time.

CDF is not a dbt replacement. Downstream modeling tools should receive a receipt
and build from there.

CDF is not a BI tool, hosted platform, or UI-first product. Those can sit above
the framework. They should not be load-bearing below it.

CDF is not trying to win by counting connectors. A connector is supported when
its claims pass the conformance suite.

## repository layout

```text
crates/
  cdf-kernel/          core types, traits, positions, checkpoints, receipts
  cdf-engine/          DataFusion planning and execution integration
  cdf-contract/        contract compiler and validation model
  cdf-package/         package builder, reader, replay, archive support
  cdf-state-sqlite/    SQLite and in-memory checkpoint stores
  cdf-http/            pagination, retry, auth, egress, and redaction tools
  cdf-formats/         Arrow-native file readers and writers
  cdf-declarative/     Tier 0 resource compiler
  cdf-python/          Python bridge and dlt preview shim
  cdf-wasm/            WASM component boundary
  cdf-subprocess/      stdio protocols and adapters
  cdf-dest-duckdb/     DuckDB destination
  cdf-dest-parquet/    Parquet and object-store destination
  cdf-dest-postgres/   Postgres destination
  cdf-project/         project files, lockfile, secrets, local runtime
  cdf-cli/             the `cdf` command
  cdf-conformance/     resource, destination, chaos, and golden-package suites
```

The lower crates define the artifact formats and core contracts. The engine
crate owns the DataFusion boundary. Extension crates handle sources and
destinations. The project and CLI crates turn those pieces into something an
operator can use.

## current state

This repository is under active construction. `VISION.md` is the book of the
system and remains the clearest description of the target design.

The workspace already contains kernel types, package and checkpoint machinery,
contract code, project files, destination crates, declarative and file resource
work, Python and subprocess foundations, CLI commands, and conformance harnesses.
Some paths are narrower than the final design while the runtime is being built
out.

Useful entry points:

- [`VISION.md`](VISION.md): the full system vision
- [`QUALITY.md`](QUALITY.md): the Rust quality and verification procedure
- [`.10x/`](.10x/): durable project memory, decisions, specs, tickets, evidence,
  and reviews

## development

This is a Cargo workspace.

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test --workspace --all-targets --locked --no-fail-fast
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Significant changes should follow `QUALITY.md`. Start with fast checks, run the
deeper gates before closing important work, and record durable evidence in
`.10x/` when the result needs to outlive terminal scrollback.

Local CodeQL runs should use the reusable database workflow described in the
quality notes. Rebuilding that database every time is a waste of a good morning.
