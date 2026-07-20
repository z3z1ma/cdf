Status: done
Created: 2026-07-19
Updated: 2026-07-19

# Arrow 58 Python and Parquet security path

## Question

Can CDF align to Arrow 58 while retaining patched PyO3 without an Arrow-major bridge, and can Parquet 58 avoid vulnerable Thrift without moving the security fix into the larger Iceberg boundary?

## Sources and Methods

- Inspected CDF's `pyo3-arrow` use sites in `cdf-python`; only `PyRecordBatch` and `PyRecordBatchReader` imports are used.
- Inspected the published `pyo3-arrow` 0.17, 0.18, and 0.19 manifests.
- Built a standalone release probe with `arrow-array = 58.3.0`, `arrow-schema = 58.3.0`, and `pyo3 = 0.29.0`, with no `pyo3-arrow` dependency.
- Ran that probe against the project's real Python 3.12 / PyArrow 25 installation, importing a `RecordBatch` through `__arrow_c_array__` and a `RecordBatchReader` through `__arrow_c_stream__`.
- Ran `cargo tree` and `cargo audit --deny warnings` against the Python probe.
- Inspected `parquet 58.3.0` and the active Arrow-rs `58_maintenance` manifest.
- Built a standalone graph containing `parquet = 58.3.0` plus a direct `thrift = 0.23.0`, inspected both inverse dependency trees, attempted `cargo update -p thrift@0.17.0 --precise 0.23.0`, and scanned with Cargo Audit and OSV Scanner.
- Inspected Apache Arrow-rs PR 10208 and the active 58.4 release issue 10349 on 2026-07-19.
- Compiled CDF against Arrow 58.3 and identified two Arrow-59 APIs required by existing behavior: Map encoding in `arrow-row` and CSV header validation in `arrow-csv`.
- Backported the exact upstream Apache commits implementing those features, resolved the Map-codec conflict by preserving Arrow 58's null-buffer representation, and ran each forked crate's unit and documentation suites.
- Ran CDF's exact-row Map canonicalization suite, direct PyCapsule unit tests, and four ignored real-PyArrow array/stream tests.

## Findings

- `pyo3-arrow 0.17` binds Arrow 58 to PyO3 0.28; no published feature selects Arrow 58 with PyO3 0.29.
- CDF does not need the broader wrapper surface. Arrow 58 itself exposes the C Data and C Stream import primitives required by CDF's two Python paths.
- The executable probe successfully imported both real PyArrow objects with Arrow 58 and PyO3 0.29. Its normal graph contained only Arrow 58 and PyO3 0.29, and Cargo Audit reported no vulnerability.
- This boundary preserves one Rust Arrow type system and transfers existing Arrow buffers through the standard stable ABI. It is not an Arrow-major conversion.
- `parquet 58.3.0` declares `thrift = "0.17"` unconditionally; disabling default features does not remove it. The active maintenance branch has the same declaration.
- Adding Thrift 0.23 directly produced both 0.17 and 0.23. Cargo refused to replace 0.17 with 0.23 because 0.23 does not satisfy `^0.17`.
- OSV Scanner reported `GHSA-2f9f-gq7v-9h6m` for Thrift 0.17. Cargo Audit did not report that GitHub-only advisory, so both scanners remain necessary.
- Arrow-rs PR 10208 changed only the Parquet Thrift declaration from 0.17 to 0.23, required no source edits, and reported 1,084 passing Parquet tests. It was closed because Arrow-rs `main` had already removed Thrift, not because the backport was invalid; the author invited a maintenance-branch backport.
- Arrow 58's RowConverter does not encode Map while CDF's exact-row dedup contract does. Apache commit `c36e926c0c8cee4ffefcd4eda96c6c11ac1a8632` is the upstream implementation; its Arrow-58 backport passed 92 unit tests and 8 documentation tests, and CDF's 90-test contract suite remained green.
- Arrow 58's CSV builder lacks the header-validation API used by CDF's delimited codec. Apache commit `9f37683968e8ecdd5f8f32333ee4f6f5f0efa319` is the upstream implementation; its Arrow-58 backport passed 71 unit tests and 12 documentation tests.
- The final fork commit is `2865fdfc2351303f37f3f8ca5e45fece682ab0b7`. Cargo metadata proves a single Arrow/Parquet 58.3 type system, PyO3 0.29 only, Thrift 0.23 only, registry DataFusion 54, and no `pyo3-arrow` or `iceberg-datafusion` dependency.
- All four real-PyArrow tests pass when the PyArrow 25 test process uses its system allocator. Without `ARROW_DEFAULT_MEMORY_POOL=system`, PyArrow's mimalloc crashes while constructing a later test fixture on a new Rust test-harness OS thread, before entering CDF's importer. This is a host/PyArrow allocator-test limitation, not a product-path requirement.

## Conclusions

CDF can align its Python boundary to Arrow 58 cleanly by deleting `pyo3-arrow` and owning a small, safety-tested PyCapsule importer on PyO3 0.29.

Published Parquet 58 cannot eliminate vulnerable Thrift through Cargo resolution or feature selection. Until an official compatible release exists, the smallest safe correction is one immutable Arrow-rs fork containing the security dependency update and the two exact upstream feature backports CDF already relies upon. That fork is materially smaller and more stable than an Iceberg Arrow-version fork and lets CDF consume Apache Iceberg Rust upstream directly without semantic downgrades.

## Limits

The Python probe proved real array/stream compatibility and dependency cleanliness, not the full CDF Python conformance matrix or every Arrow logical type. The production adapter still requires release-once, malformed-capsule, metadata, dictionary, nested, empty-batch, and stream-error tests.

The Parquet PR's full 1,084-test result is upstream-reported. CDF ran focused Parquet compilation and the complete unit/doc suites for the two feature-bearing forked crates; broader golden and untrusted-input suites remain product-level gates rather than claims of this dependency investigation.
