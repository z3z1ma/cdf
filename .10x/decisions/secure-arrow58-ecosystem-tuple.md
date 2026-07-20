Status: active
Created: 2026-07-19
Updated: 2026-07-19

# Secure Arrow 58 ecosystem tuple

## Context

CDF requires one same-major Arrow tuple across first-party crates, DataFusion, Iceberg, Parquet, Python, and private destination drivers. The former Arrow-59 tuple used an unreleased DataFusion git revision because the published DataFusion 54 line uses Arrow/Parquet 58. Apache Iceberg Rust `main` also uses Arrow/Parquet 58, so retaining Arrow 59 would require a CDF-owned Iceberg compatibility fork.

The prior Arrow-58 attempt was rejected only because `pyo3-arrow 0.17` selected vulnerable PyO3 0.28 and `parquet 58.3.0` selected vulnerable Thrift 0.17. Focused executable investigation on 2026-07-19 proved that these are separable dependency problems rather than reasons CDF itself must remain on Arrow 59.

The user ratified the secure Arrow-58 recommendation on 2026-07-19 and explicitly authorized the smallest necessary Parquet fork or other bounded correction.

## Decision

CDF will align first-party Arrow and Parquet use, published DataFusion 54, Apache Iceberg Rust, and the private DuckDB driver on the Arrow/Parquet 58 line. The temporary DataFusion Arrow-59 git pin is removed.

`cdf-python` will remain on PyO3 0.29 and remove `pyo3-arrow`. A narrow CDF-owned adapter will consume the standard `__arrow_c_array__` and `__arrow_c_stream__` PyCapsules directly into Arrow 58 through Arrow's C Data and C Stream interfaces. This is a foreign-runtime boundary over one Arrow major, not an Arrow-major bridge. Unsafe ownership transfer must be isolated, validate capsule identity, and prove release exactly once.

CDF will pin one minimal Apache Arrow-rs 58 fork at immutable commit `2865fdfc2351303f37f3f8ca5e45fece682ab0b7`. Its admitted delta is closed to three independently upstreamed changes required by CDF: Parquet's Thrift 0.17 dependency is replaced with 0.23; Arrow Row's Map codec is backported from Apache commit `c36e926c0c8cee4ffefcd4eda96c6c11ac1a8632`; and Arrow CSV's header validation is backported from Apache commit `9f37683968e8ecdd5f8f32333ee4f6f5f0efa319`. The forked packages depend explicitly on published Arrow 58.3 companion crates so Cargo cannot form equal-version git/registry type identities.

The fork remains source-pinned and may not acquire CDF-specific behavior. Its removal trigger is the first supported official Arrow-rs release containing all three capabilities and passing CDF's golden, supply-chain, and performance gates. Individual package patches may be removed earlier when the corresponding official package meets those gates without splitting the Arrow tuple.

CDF will use an immutable upstream Apache Iceberg Rust revision already on Arrow/Parquet 58. CDF will not carry the proposed Arrow-59 Iceberg fork.

No reachable untrusted-input advisory exception is admitted. The existing separately ratified `paste` maintenance advisory remains governed by its own decision.

## Alternatives Considered

### Retain Arrow 59 and fork Iceberg

Rejected. It preserves an unreleased DataFusion git pin and creates a larger, less stable fork at the table-semantics boundary when the actual security correction belongs in one Parquet dependency declaration.

### Downgrade Python to PyO3 0.28 through `pyo3-arrow 0.17`

Rejected. CDF needs only record-batch and record-batch-stream imports, both already standardized by the Arrow PyCapsule interface. Downgrading would reintroduce known PyO3 advisories and retain an unnecessary wrapper dependency.

### Add Thrift 0.23 directly beside Parquet 58

Rejected. Cargo proves `parquet 58.3.0` requires `thrift ^0.17`; a direct 0.23 dependency resolves a second copy and leaves vulnerable 0.17 reachable.

### Accept the Thrift advisory temporarily

Rejected as the default. CDF ingests untrusted Parquet metadata, and the correction is a tested one-line dependency update with a materially smaller ownership surface than the Iceberg fork it eliminates.

## Consequences

The Arrow-major repin is artifact-sensitive and must pass golden package, replay, Python boundary, Parquet, DataFusion, destination, and supply-chain gates before closure. Existing golden changes must be explained rather than regenerated blindly.

The PyCapsule importer becomes an explicit CDF FFI safety boundary with targeted malformed-producer and release-lifetime tests.

The Arrow-rs fork is deliberately upstream-derived and short-lived. No CDF behavior, transport authority, codec policy, or Iceberg logic may enter it. The Map codec and CSV validation backports preserve CDF's existing semantics rather than adding a compatibility shim or weakening behavior to fit Arrow 58.

Crates.io publication is no longer blocked by a DataFusion git source once every remaining dependency source satisfies release policy.
