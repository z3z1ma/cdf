Status: superseded
Created: 2026-07-19
Updated: 2026-07-19

# Iceberg Arrow 59 dependency admission

## Question

Which Apache Iceberg Rust dependency can back CDF's first-class Iceberg source without introducing an Arrow-major conversion boundary, a second DataFusion tuple, or a source-owned transport authority, and what build and supply-chain cost does that choice impose?

## Sources and Methods

- Inspected Apache Iceberg Rust `main` at commit `db4f6091850814b83989721afe12aa9e4406d6b3` on 2026-07-19: <https://github.com/apache/iceberg-rust/tree/db4f6091850814b83989721afe12aa9e4406d6b3>.
- Inspected published `iceberg 0.9.1` metadata with `cargo info`; it uses Arrow/Parquet 57 and declares Rust 1.92.
- Inspected the open 0.10.0 release tracker on 2026-07-19: <https://github.com/apache/iceberg-rust/issues/2527>.
- Inspected the upstream Arrow 58 dependency update: <https://github.com/apache/iceberg-rust/pull/2206>.
- Patched a local clone of Apache `main` by changing only `arrow-{arith,array,buffer,cast,ord,schema,select,string}` and `parquet` from 58 to 59.1.0. The umbrella `arrow` workspace dependency remained 58 because the core `iceberg` package does not use it and changing it would affect unrelated workspace members.
- Ran `cargo check -p iceberg --lib` and compiled the REST and Glue catalog packages against the patched core.
- Built a standalone Arrow 59 consumer that converts an Iceberg schema through `iceberg::arrow::schema_to_arrow_schema`, combines it with an Arrow 59 array in a `RecordBatch`, and executes the result.
- Added `cdf-source-iceberg` against the local compatibility branch and inspected the normal dependency graph with `cargo tree`.
- Measured clean `cdf-runtime` and marginal `cdf-source-iceberg` checks into an isolated target directory.
- Ran `cargo deny check`, `cargo audit --deny warnings`, and `cargo vet --locked` against the candidate graph.
- Inspected the upstream core module/dependency topology, including catalog, scan, schema, manifest Avro, encryption, writer, cache, HTTP error conversion, and injectable `Storage`/`StorageFactory` interfaces.

## Findings

### Version alignment

- Published `iceberg 0.9.1` is not compatible with CDF's Arrow 59 type identity because it uses Arrow/Parquet 57.
- Apache `main` is also not admissible unchanged: it currently uses Arrow/Parquet 58 and declares Rust 1.94.
- The nine-component Arrow/Parquet 59.1 edit compiled without any Iceberg source-code change. The compatibility commit is locally recorded as `3834cff15fb0fad83b39a35ed100f68fd0a55d27` on top of Apache `db4f6091850814b83989721afe12aa9e4406d6b3`.
- The standalone consumer proved actual Rust type identity: the Iceberg-produced schema and CDF's Arrow 59 arrays form a native Arrow 59 `RecordBatch`; no JSON, IPC, FFI, or conversion layer is involved.
- The candidate `cdf-source-iceberg` graph contains Arrow/Parquet 59.1 and no `iceberg-datafusion`, DataFusion 58 tuple, or other Iceberg catalog/storage crate.

### Authority and crate boundaries

- CDF must depend only on the core `iceberg` package. `iceberg-catalog-rest`, `iceberg-catalog-glue`, and `iceberg-storage-opendal` compile, but they bring catalog- or storage-owned HTTP/AWS/OpenDAL clients. Admitting them would create competing credential, egress, retry, memory, and telemetry authorities.
- Apache `main` now exposes injectable `Storage` and `StorageFactory` traits in the core package. A later CDF bridge can therefore route Iceberg metadata and manifest reads through `cdf-object-access` without changing Iceberg planning semantics.
- `cdf-source-iceberg` keeps `iceberg` types private. Its public foundation currently returns CDF runtime descriptors, JSON configuration schema, and Arrow 59 schema values only.

### Build impact

- A clean locked `cargo check -p cdf-runtime` completed in 10.46 seconds and produced 117,740 KiB in the isolated target directory.
- Checking `cdf-source-iceberg` immediately afterward added 15.99 seconds and increased that target directory to 410,000 KiB: a marginal 292,260 KiB (about 285.4 MiB).
- A standalone release compatibility probe took 45.71 seconds to reach a probe-source type error on its first clean build; after correcting the probe, the incremental build and run took 1.00 second. The linked one-row probe was 1.1 MiB and the isolated target directory was 605 MiB. These are dependency-cost context, not product-binary measurements.
- Iceberg core currently has no functional feature partition. Read planning, writer code, encryption, manifest Avro, cache, and a Reqwest error conversion are compiled together. The normal source graph contained 319 rendered `cargo tree` lines and 74 dependencies not yet covered by CDF's cargo-vet policy.

### Supply chain

- `cargo deny check` passed all advisory, bans, license, and source checks for the local-path candidate. Apache Iceberg Rust is Apache-2.0 licensed.
- `cargo audit --deny warnings` reported only the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` exception inherited through Arrow/Parquet.
- `cargo vet --locked` failed with 74 unvetted transitive dependencies. They include the Avro, encryption, cache, derive, and support graph. This must be resolved before admission; the failure is not waived.
- `cargo vet suggest --locked` panicked in cargo-vet 0.10.2 while the Iceberg dependency was a local path dependency. Re-run against the final git-pinned source before classifying this as a persistent cargo-vet defect.
- The CDF development compiler is Rust 1.96.1, which satisfies Iceberg `main`'s Rust 1.94 declaration.

### Feature-fork assessment

- A CDF-only read feature would be substantially more invasive than the Arrow version patch because Iceberg's scan and table paths deliberately share manifest Avro, delete handling, object caching, and encryption with other modules.
- Removing encryption from the source graph would also make encrypted Iceberg tables unsupported, conflicting with a first-class source rather than merely pruning unused writer behavior.
- Writer-only feature partitioning could be valuable upstream, but it is not required for runtime correctness and does not justify making CDF the long-term owner of a large semantic fork before upstream accepts such boundaries.

## Conclusions

CDF should admit Apache Iceberg Rust core from a tightly pinned fork containing only the nine Arrow/Parquet 59.1 dependency edits. The fork must not include CDF-specific transport, catalog, scan, or semantic changes. CDF-owned REST and Glue catalog bindings and the `cdf-object-access` storage bridge must be implemented in `cdf-source-iceberg`; upstream Iceberg remains the table/spec/planning authority.

The fork removal trigger is the first upstream Iceberg revision or release that uses the CDF Arrow/Parquet tuple and passes CDF's graph, conformance, and performance gates. A future upstream feature split may reduce build cost, but CDF should not preemptively carry that larger fork. The dependency is not admitted until the external fork is published at an immutable revision and cargo-vet coverage is complete.

## Limits

- This investigation proves dependency/type compatibility and a narrow schema bridge, not catalog correctness, snapshot planning, deletion semantics, or data-file execution.
- The build measurements are from the current macOS development host and isolated target directories; they are not EC2 release-envelope results.
- REST and Glue upstream catalog packages were compile-probed only and are explicitly excluded from the product graph.
- The compatibility patch exists only in a local clone until external fork creation and push are explicitly authorized.
