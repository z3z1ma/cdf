Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Source extension boundary audit

## Question

What must change so adding a source requires only a source driver/composition/conformance entry and automatically receives discovery, planning, state, retries, performance scheduling, CLI add/doctor, package evidence, and preview/run parity?

## Sources and methods

Inspected kernel `ResourceStream`/`QueryableResource`/capabilities, declarative source/resource models and compilation, REST/SQL/file runtimes, schema discovery dispatch, project run wrappers, CLI/project imports, Cargo crate placement, and the active source/destination extension invariant.

## Findings

The lower execution boundary is promising: generic project runtime already consumes `dyn QueryableResource`, and `ResourceStream` owns descriptor/schema/partition planning/open/attestation. This seam should be preserved and enriched rather than replaced.

Above it, Tier-0 is closed. `SourceDeclaration` and `CompiledResourcePlan` enumerate REST, SQL, and files. Common `ResourceDeclaration` contains source-specific fields (`path`, `query`, `table`, `glob`, pagination/records, format/compression). Compilation, capabilities, predicate fidelity, validation, discovery, runtime dependency construction, and error wording match those enum variants.

REST and SQL require wrapper resources because `CompiledResource::open` implements only files. SQL runtime is specifically Postgres and imports Postgres source types from the destination crate, conflating source and destination build/ownership. Project `ProjectRunSource` exposes concrete convenience constructors. Discovery has source/format-specific adapters and matches despite a generic artifact model.

Adding a source would therefore edit declarative structs/schema/compiler, discovery dispatcher, runtime construction, project/CLI wiring, and tests. It would also risk scheduler branches for rate limits, partition concurrency, retries, poll working sets, snapshot/attestation, and pushdown because current `ResourceCapabilities` lacks several P3 execution declarations.

The config authoring problem cannot be solved by an unvalidated free-form map. CDF needs an open driver registry while preserving JSON-Schema/editor validation, canonical lock diffs, secret references, and ergonomic source-specific top-level fields.

## Conclusion

Create a neutral `SourceDriver` registry in `cdf-runtime` around the existing kernel resource traits. Parse source/resource documents into a common typed envelope plus driver-owned raw option objects validated by driver JSON-Schema fragments. Drivers compile canonical opaque plan artifacts, resolve runtime resources through injected services, implement discovery/add/doctor, and declare extended execution capabilities.

Move first-party physical sources into dependency-isolated source crates. Files compose transports/formats; REST composes HTTP; Postgres source no longer lives in a destination crate. Project/generic CLI consume only the registry. Conformance registers fixtures as data.

## Limits

This audit does not design dynamic plugins or every future source. SX1 first migrates existing file/REST/Postgres behavior through compatibility adapters; later source breadth uses the same contract.
