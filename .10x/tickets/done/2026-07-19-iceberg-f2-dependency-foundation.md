Status: done
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md

# Iceberg F2: aligned dependency and source-crate foundation

## Scope

Align CDF, published DataFusion, Apache Iceberg Rust, and the private DuckDB driver on Arrow/Parquet 58; retain patched PyO3 0.29 through a narrow CDF-owned Arrow PyCapsule importer; pin one minimal Arrow-rs 58 fork containing only the required Thrift security correction and exact upstream Map-row/CSV-header backports; establish the first-party `cdf-source-iceberg` crate boundary; and prove that no Arrow-major conversion or second DataFusion tuple enters the graph.

## Non-goals

No catalog network calls, scans, object-access implementation, Glue external tables, source-position change, product registration, Arrow-major bridge, CDF-specific fork behavior, or advisory exception for reachable untrusted-input parsing.

## Acceptance Criteria

- CDF first-party crates, published DataFusion, Apache Iceberg core, and the DuckDB private driver resolve on Arrow/Parquet 58 with no `iceberg-datafusion` hot-path dependency or DataFusion git source.
- `cdf-python` uses PyO3 0.29 and imports Arrow C Data/C Stream PyCapsules directly into Arrow 58; `pyo3-arrow`, PyO3 0.28, and any Arrow-major bridge are absent.
- Parquet 58 resolves only patched Thrift 0.23 through an immutable minimal fork; the exact upstream Map-row and CSV-header-validation behavior CDF needs is retained; Thrift 0.17 is absent; and every package patch has an explicit upstream-removal trigger.
- Supply-chain, advisory, license, MSRV, build-time/size, feature, and pin evidence is recorded.
- `cargo tree` proves one workspace Arrow/Parquet major and no unintended DataFusion tuple.
- The new source crate exposes no Iceberg/AWS types to kernel/runtime/project/engine and has descriptor/config-schema golden foundations.
- Clean and incremental build impact is measured rather than assumed.

## References

- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/decisions/arrow-datafusion-tuple-policy.md`
- `.10x/decisions/secure-arrow58-ecosystem-tuple.md`
- `.10x/specs/iceberg-source.md`

## Assumptions

- User-ratified 2026-07-19: align CDF to Arrow 58, retain PyO3 0.29 through a CDF-owned standard Arrow PyCapsule adapter, use the smallest necessary Parquet security fork rather than an Iceberg fork, and continue the full Iceberg program after alignment; permanent Arrow-major conversion remains prohibited.

## Journal

- 2026-07-19: Execution started after F1 closed. Published `iceberg 0.9.1` is Arrow/Parquet 57; Apache main at `db4f6091850814b83989721afe12aa9e4406d6b3` is Arrow/Parquet 58 and Rust 1.94, so neither is admissible unchanged under the one-tuple policy.
- 2026-07-19: A local Apache-main compatibility branch changed only the eight component Arrow dependencies and Parquet from 58 to 59.1.0 (the umbrella `arrow` dependency remains 58 because it is not in the core crate graph). `cargo check -p iceberg --lib` passed without source changes. `iceberg-catalog-rest` and `iceberg-catalog-glue` also compiled against the patched core, but their own Reqwest/AWS/OpenDAL authorities are not admissible into CDF execution; F2 will admit core only and later catalog bindings will remain CDF-owned behind injected HTTP/object access.
- 2026-07-19: A standalone release probe compiled a CDF Arrow 59 array into an `iceberg::arrow`-produced Arrow schema and ran successfully, proving type identity rather than JSON/IPC conversion. The first clean release compile took 45.71 seconds before a probe-source type error; the fixed incremental build and run took 1.00 second. The linked one-row probe was 1.1 MiB and its isolated target tree 605 MiB; these are standalone upper-context measurements, not CDF product binary deltas.
- 2026-07-19: Established the new `cdf-source-iceberg` boundary with a deterministic option-schema/descriptor golden and a narrow Iceberg-schema-to-Arrow-59 bridge. Iceberg types remain private to the adapter. Runtime build-graph laws now require Arrow/Parquet 59, forbid Iceberg DataFusion/DataFusion and Arrow/Parquet 58, and enforce sibling-source isolation.
- 2026-07-19: Clean isolated `cargo check -p cdf-runtime --locked` took 10.46 seconds and produced 117,740 KiB. The immediately following marginal `cargo check -p cdf-source-iceberg --locked` took 15.99 seconds and grew the target to 410,000 KiB, a 292,260 KiB (about 285.4 MiB) increment.
- 2026-07-19: `cargo deny check` passed advisories, bans, licenses, and sources. `cargo audit --deny warnings` found only the existing ratified `paste` advisory. `cargo vet --locked` correctly failed on 74 newly unvetted Iceberg transitive dependencies; no waiver has been applied. `cargo vet suggest --locked` panicked in cargo-vet 0.10.2 while the dependency was a local path and will be retried against the final git pin.
- 2026-07-19: Inspected Iceberg core's featureless module graph. A larger CDF-only read-feature fork is rejected for this admission: it would couple CDF to upstream module surgery and risk dropping encrypted-table support. The retained fork policy is the minimal nine dependency-version edits, with removal on the first Arrow-59-compatible upstream revision that passes CDF's gates. Full findings are in `.10x/research/superseded/2026-07-19-iceberg-arrow59-dependency-admission.md`.
- 2026-07-19: `cargo vet regenerate exemptions --locked` generated the 74 exact `safe-to-deploy` entries required by the existing project policy; `cargo vet --locked` then passed with 32 fully audited, one partially audited, and 543 exempted packages. The final immutable git package still needs its exact `audit-as-crates-io` policy and exemption after publication.
- 2026-07-19: Focused verification remains green: all three `cdf-source-iceberg` unit tests plus doc tests, the Arrow-59/engine-free graph law, sibling-source isolation, and generic-compiler exclusion. `git diff --check` passed over the owned patch.
- 2026-07-19: User superseded the Arrow-59 fork direction after executable probes proved Arrow 58 can retain PyO3 0.29 without `pyo3-arrow`, while Parquet 58's sole remaining blocker is an unconditional Thrift 0.17 dependency that can be corrected by the much smaller one-line security fork. The useful source-crate boundary remains; the local Iceberg Arrow-59 fork premise and generated exemptions are being removed.
- 2026-07-19: Published `z3z1ma/arrow-rs` branch `cdf/parquet-58.3-thrift-0.23` and pinned commit `25346798a7f75facaca94156b52abac28e86b9f5`. The fork changes Parquet's Thrift dependency to 0.23 and makes the patched Parquet package depend explicitly on published Arrow/Parquet companion crates; this packaging step prevents equal-version git/crates.io Arrow duplicates from forming distinct Rust type identities. Focused Parquet Arrow check passed.
- 2026-07-19: Full CDF compilation exposed two existing Arrow-59 semantics absent from published Arrow 58: `arrow-row` Map encoding used by exact-row dedup and `arrow-csv` header validation used by the delimited codec. Weakening either behavior or adding a CDF compatibility shim was rejected. Backported the exact upstream Apache commits `c36e926c0c8cee4ffefcd4eda96c6c11ac1a8632` and `9f37683968e8ecdd5f8f32333ee4f6f5f0efa319`; the final immutable fork commit is `2865fdfc2351303f37f3f8ca5e45fece682ab0b7`.
- 2026-07-19: Fork verification passed: `arrow-row` 92 unit + 8 doc tests, `arrow-csv` 71 unit + 12 doc tests, and CDF contract 90 tests with 2 performance tests ignored. The Arrow-58 test-fixture adaptation uses the native `MapArray::new_from_strings`; production exact-row behavior is unchanged.
- 2026-07-19: The CDF-owned Python importer consumes checked `arrow_schema`, `arrow_array`, and `arrow_array_stream` capsules through Arrow's C interfaces and moves array/stream ownership exactly once. `cdf-python` passed 30 tests with 7 slow/environment tests ignored; all 4 real-PyArrow tests passed under `ARROW_DEFAULT_MEMORY_POOL=system`. The environment variable avoids a PyArrow-25 mimalloc crash on a later Rust test-harness OS thread before CDF code executes and is not a product requirement.
- 2026-07-19: Final locked metadata proves one Arrow/Parquet 58.3 tuple, DataFusion 54 from crates.io, Iceberg 0.10 at Apache commit `db4f6091850814b83989721afe12aa9e4406d6b3`, PyO3 0.29 only, Thrift 0.23 only, and no `pyo3-arrow`, PyO3 0.28, `iceberg-datafusion`, Arrow 59, Parquet 59, or duplicate equal-name Arrow packages.
- 2026-07-19: Final supply-chain gates passed: `cargo deny check`; `cargo vet --locked` (32 fully audited, 1 partially audited, 539 exact exemptions); and both scanners report only the existing ratified unmaintained `paste 1.0.15` advisory. The Arrow-rs and Iceberg git packages use `audit-as-crates-io` policy where their source identity matches the published crate, with immutable revisions still enforced by Cargo.
- 2026-07-19: Clean Arrow-58 runtime check took 10.99 seconds and 124,144 KiB in an isolated target. Adding `cdf-source-iceberg` marginally took 23.28 seconds and brought the target to 418,832 KiB: 294,688 KiB (287.8 MiB) incremental build storage. This is the admitted Iceberg core/Avro/Parquet compile cost, measured rather than hidden.
- 2026-07-19: `cdf-source-iceberg` passed 3 unit tests and its Arrow-58/engine-free and sibling-isolation graph laws. The broader runtime build-graph suite passed 6/7; its only failure is the separately owned `.10x/tickets/2026-07-19-p0-runtime-egress-build-graph-regression.md`. A clean `HEAD` archive independently contains the same 85 packages against the existing 67-package ceiling, proving F2 did not cause or encode that regression.
- 2026-07-19: Workspace all-target compilation reached the unrelated active runtime/project lane and stopped at its in-progress missing `PartitionId` import in `cdf-project`; F2 does not touch or mask that concurrent work. Formatting and `git diff --check` pass for the combined workspace state.

## Blockers

None. The user explicitly authorized the minimal Parquet fork and complete implementation on 2026-07-19.

## Evidence

- **One Arrow/Parquet tuple and published DataFusion:** locked `cargo metadata` and `cargo tree` show Arrow/Parquet 58.3 only, registry DataFusion 54 only, Apache Iceberg core at its immutable revision, and no `iceberg-datafusion`; `iceberg_source_graph_is_arrow58_native_and_engine_free` passes.
- **Patched Python without a bridge:** `cdf-python` has no `pyo3-arrow`; its 30-test normal suite and four-test real-PyArrow array/stream matrix pass on PyO3 0.29 and Arrow 58.
- **Secure Parquet dependency:** Cargo metadata contains Thrift 0.23 only. The forked Parquet compiles, the forked Row/CSV unit and doc suites pass, and Cargo Audit plus OSV report no Thrift advisory.
- **Supply chain:** `cargo deny check` and `cargo vet --locked` pass. Cargo Audit and OSV agree on only the pre-existing, separately ratified `paste` maintenance advisory.
- **Boundary:** the new crate's descriptor/schema goldens pass; runtime graph laws prove sibling-source isolation, generic-compiler exclusion, Arrow-58 type identity, and absence of DataFusion/engine dependencies.
- **Cost:** isolated measurements record 10.99 seconds/124,144 KiB for runtime and a further 23.28 seconds/294,688 KiB for Iceberg core.

## Review

**Verdict: pass.** Fresh adversarial inspection found no critical, significant, minor, or nit findings in the owned implementation. The PyCapsule boundary validates capsule identities before unsafe access, moves array/stream ownership once, preserves schema metadata and empty-batch row counts, and is exercised by synthetic ownership tests plus real nested/dictionary/empty/error/cancellation/lifetime PyArrow cases. The fork is closed to exact upstream deltas, uses one immutable revision, and does not introduce CDF semantics. The Iceberg crate exposes only CDF/Arrow types and remains absent from generic compiler/runtime/engine graphs.

**Residual risk:** PyArrow 25's mimalloc crashes in this host's sequential Rust test-harness threads unless its test process selects the system allocator; product code does not set or depend on that variable. The pre-existing runtime graph ceiling failure remains independently owned and was reproduced from clean `HEAD`. Full workspace compilation is temporarily bounded by the concurrent runtime lane, while all F2 owner packages and graph/supply-chain gates pass.

## Retrospective

The original Arrow-59 direction optimized for CDF's then-current tuple rather than the ecosystem's convergent tuple. Executable probes showed the Python blocker was a removable wrapper dependency and the Parquet blocker was a tiny security delta. Compiling the entire consumer surface then found two additional behavior-bearing APIs; carrying their exact upstream implementations was both smaller and safer than weakening CDF or inventing compatibility layers. Future tuple decisions should begin with interface-level falsification and compile the behavior-bearing consumers before accepting a fork boundary.
