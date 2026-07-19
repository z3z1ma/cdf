Status: blocked
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md

# Iceberg F2: Arrow 59 dependency and source-crate foundation

## Scope

Select and admit an Apache Iceberg Rust revision aligned to CDF Arrow/Parquet 59, establish the dependency feature/pin policy and first-party `cdf-source-iceberg` crate boundary, and prove no Arrow-major conversion or second DataFusion tuple enters the graph.

## Non-goals

No catalog network calls, scans, object-access implementation, Glue external tables, source-position change, or product registration.

## Acceptance Criteria

- The admitted Iceberg core/catalog dependency resolves on Arrow/Parquet 59 with no `iceberg-datafusion` hot-path dependency.
- Supply-chain, advisory, license, MSRV, build-time/size, feature, and pin evidence is recorded; any fork/revision has an explicit upstream/removal trigger.
- `cargo tree` proves one first-party Arrow/Parquet major and no unintended DataFusion tuple.
- The new source crate exposes no Iceberg/AWS types to kernel/runtime/project/engine and has descriptor/config-schema golden foundations.
- Clean and incremental build impact is measured rather than assumed.

## References

- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/decisions/arrow-datafusion-tuple-policy.md`
- `.10x/decisions/datafusion-git-pin-arrow59-tuple.md`
- `.10x/specs/iceberg-source.md`

## Assumptions

- User-ratified 2026-07-19: a tightly pinned Apache revision/fork is acceptable when upstream has no Arrow 59 release; permanent conversion is not.

## Journal

- 2026-07-19: Execution started after F1 closed. Published `iceberg 0.9.1` is Arrow/Parquet 57; Apache main at `db4f6091850814b83989721afe12aa9e4406d6b3` is Arrow/Parquet 58 and Rust 1.94, so neither is admissible unchanged under the one-tuple policy.
- 2026-07-19: A local Apache-main compatibility branch changed only the eight component Arrow dependencies and Parquet from 58 to 59.1.0 (the umbrella `arrow` dependency remains 58 because it is not in the core crate graph). `cargo check -p iceberg --lib` passed without source changes. `iceberg-catalog-rest` and `iceberg-catalog-glue` also compiled against the patched core, but their own Reqwest/AWS/OpenDAL authorities are not admissible into CDF execution; F2 will admit core only and later catalog bindings will remain CDF-owned behind injected HTTP/object access.
- 2026-07-19: A standalone release probe compiled a CDF Arrow 59 array into an `iceberg::arrow`-produced Arrow schema and ran successfully, proving type identity rather than JSON/IPC conversion. The first clean release compile took 45.71 seconds before a probe-source type error; the fixed incremental build and run took 1.00 second. The linked one-row probe was 1.1 MiB and its isolated target tree 605 MiB; these are standalone upper-context measurements, not CDF product binary deltas.
- 2026-07-19: Established the new `cdf-source-iceberg` boundary with a deterministic option-schema/descriptor golden and a narrow Iceberg-schema-to-Arrow-59 bridge. Iceberg types remain private to the adapter. Runtime build-graph laws now require Arrow/Parquet 59, forbid Iceberg DataFusion/DataFusion and Arrow/Parquet 58, and enforce sibling-source isolation.
- 2026-07-19: Clean isolated `cargo check -p cdf-runtime --locked` took 10.46 seconds and produced 117,740 KiB. The immediately following marginal `cargo check -p cdf-source-iceberg --locked` took 15.99 seconds and grew the target to 410,000 KiB, a 292,260 KiB (about 285.4 MiB) increment.
- 2026-07-19: `cargo deny check` passed advisories, bans, licenses, and sources. `cargo audit --deny warnings` found only the existing ratified `paste` advisory. `cargo vet --locked` correctly failed on 74 newly unvetted Iceberg transitive dependencies; no waiver has been applied. `cargo vet suggest --locked` panicked in cargo-vet 0.10.2 while the dependency was a local path and will be retried against the final git pin.
- 2026-07-19: Inspected Iceberg core's featureless module graph. A larger CDF-only read-feature fork is rejected for this admission: it would couple CDF to upstream module surgery and risk dropping encrypted-table support. The retained fork policy is the minimal nine dependency-version edits, with removal on the first Arrow-59-compatible upstream revision that passes CDF's gates. Full findings are in `.10x/research/2026-07-19-iceberg-arrow59-dependency-admission.md`.
- 2026-07-19: `cargo vet regenerate exemptions --locked` generated the 74 exact `safe-to-deploy` entries required by the existing project policy; `cargo vet --locked` then passed with 32 fully audited, one partially audited, and 543 exempted packages. The final immutable git package still needs its exact `audit-as-crates-io` policy and exemption after publication.
- 2026-07-19: Focused verification remains green: all three `cdf-source-iceberg` unit tests plus doc tests, the Arrow-59/engine-free graph law, sibling-source isolation, and generic-compiler exclusion. `git diff --check` passed over the owned patch.

## Blockers

Local implementation and investigation remain unblocked. Closing dependency admission requires publishing the nine-line Arrow/Parquet patch to a pinned external fork; creating or pushing that fork still requires explicit external-write confirmation. Cargo-vet coverage must then be completed against the immutable git source.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
