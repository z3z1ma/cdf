Status: active
Created: 2026-07-19
Updated: 2026-07-19

# Dependency tuple migration guard

CDF resolves one public Arrow/Parquet major across first-party crates, DataFusion, Iceberg, Python interchange, and destination drivers. The current tuple is governed by `.10x/decisions/secure-arrow58-ecosystem-tuple.md`: Arrow/Parquet 58.3, registry DataFusion 54, PyO3 0.29, DuckDB `1.10504.0`, immutable Apache Iceberg revision `db4f6091850814b83989721afe12aa9e4406d6b3`, and the bounded Arrow-rs fork at `2865fdfc2351303f37f3f8ca5e45fece682ab0b7`.

During dependency and release review, inspect the newest mutually compatible Arrow, DataFusion, Iceberg, DuckDB, object-store, and Python-interchange candidates as one tuple. A version number alone is not a migration trigger. Open a tuple migration ticket only when the candidate:

- resolves one Arrow/Parquet major without a conversion bridge;
- preserves the Map row codec, CSV header validation, and fixed Thrift dependency required by CDF;
- does not introduce a second DataFusion execution tuple or `iceberg-datafusion` into the native source path;
- passes golden package/replay, Python PyCapsule, source/destination, supply-chain, performance, and build-graph gates; and
- removes more owned compatibility surface than it adds.

Individual Arrow-rs package patches may be removed as official packages acquire the required behavior, provided Cargo metadata still proves one Rust type identity. The Iceberg git pin may move only through a ticket that checks snapshot/schema semantics and catalog compatibility, not merely compilation.

Crates.io publication remains blocked for any crate whose distributable dependency graph contains a disallowed git or path dependency. Binary prereleases and checksummed artifacts remain permitted. This is a graph property checked at release time, not a permanent DataFusion-specific prohibition.
