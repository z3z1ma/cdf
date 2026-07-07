Status: active
Created: 2026-07-07
Updated: 2026-07-07

# Project Destination Driver Registry

## Context

`VISION.md` Chapter 14 describes destinations as commit protocols, and Chapter 16/18 require run, replay, resume, and inspect paths to compose through the general run spine rather than destination-specialized paths. The P0 stop-line makes this structural cleanup mandatory before CDF opens more destination, source-archetype, CDC/Kafka, or streaming-supervisor lanes.

Workstream A closed the kernel session gap with `CommitSession::write_segment`, required `DestinationProtocol::begin`, and trait-level `DestinationProtocol::verify`. That makes the destination commit action generic after package-aware planning exists.

Workstream B now has a different boundary problem:

- `crates/cdf-project/src/runtime.rs` exposes closed `ProjectRunDestination` and `ProjectRunResource` enums.
- `cdf-project` contains public destination-specialized replay/recovery families for DuckDB, Parquet, and Postgres.
- CLI run, replay, and resume each duplicate destination URI/project resolution.
- DuckDB, Parquet, and Postgres still require package-aware planning inputs before the kernel `begin` call can be used safely.
- The kernel must remain package-free and must not depend on `cdf-package`, `cdf-project`, URI resolution, or destination driver private planning types.

The key constraint is Postgres. A safe Postgres package commit needs a `PostgresLoadPlan`, including columns from the package schema, target parsing, dedup policy, merge keys, DDL, staging, mirrors, and verification clauses. The generic kernel `DestinationProtocol::plan_commit` intentionally does not carry those project/package-specific inputs.

## Decision

`cdf-project` will own a project-level destination driver registry and object-safe destination runtime adapter layer.

The generic project runtime will operate over:

- `ResourceStream` or `QueryableResource` after resource resolution;
- `ProjectDestinationRuntime` after destination URI/project-config resolution;
- kernel `DestinationProtocol` and `CommitSession` after package-aware destination planning.

The project destination adapter shape is:

```rust
pub trait ProjectDestinationDriver {
    fn schemes(&self) -> &'static [&'static str];

    fn resolve(
        &self,
        uri: &str,
        context: &ProjectResolutionContext<'_>,
    ) -> Result<Box<dyn ProjectDestinationRuntime>>;
}

pub trait ProjectDestinationRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol;

    fn describe(&self) -> ProjectDestinationDescription;

    fn prepare_package_commit(
        &mut self,
        package_dir: &Path,
        reader: &PackageReader,
        inputs: &PackageReplayInputs,
        context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit>;
}
```

`PreparedDestinationCommit` will contain:

- the kernel `DestinationCommitRequest`;
- the kernel `CommitPlan`;
- receipt-source/reporting policy sufficient to preserve existing duplicate/package-receipt reporting;
- any adapter-owned pending context needed so `ProjectDestinationRuntime::protocol().begin(commit, plan)` is safe.

The generic replay/recovery skeleton will:

1. open and verify package artifacts;
2. ask the resolved destination runtime to prepare a package commit when a destination mutation is needed;
3. propose the checkpoint before destination mutation;
4. update the package to loading before destination mutation;
5. begin the kernel session, apply migrations, feed package segments one by one, and finalize to a receipt;
6. validate receipt identity;
7. verify the receipt through `DestinationProtocol::verify`;
8. commit or reuse the checkpoint through `CheckpointStore`;
9. update package status to checkpointed.

Recovery with a supplied durable receipt will skip destination commit preparation and mutation, then validate identity, verify through the destination protocol, commit or reuse the checkpoint, and update package status.

Failpoint injection will be modeled as a destination-agnostic runtime stage hook over generic replay/recovery stages. Existing DuckDB failpoint names may remain only as temporary test compatibility adapters while callers migrate; Workstream B closure requires specialized public failpoint wrappers to be deleted or made non-public.

`cdf-project` may include built-in drivers for current first-party destinations:

- `duckdb://` backed by `cdf-dest-duckdb`;
- `parquet://` backed by `cdf-dest-parquet`;
- `postgres://` backed by `cdf-dest-postgres`.

Adding a future destination must require a destination crate plus a project driver registration, not changes to generic run, replay, recovery, or chaos logic.

Resource construction will follow the same rule: generic orchestration consumes `&dyn ResourceStream` / `&dyn QueryableResource`, while project/CLI resolution constructs concrete file, REST, and SQL resources with their dependencies before entering the generic runtime.

## Alternatives considered

Extend `cdf_kernel::DestinationProtocol` with package-aware planning.

Rejected. It would contaminate the kernel with package/project concepts and destination-private planning requirements. The kernel decision for Workstream A deliberately kept kernel sessions package-free.

Keep closed `ProjectRunDestination` and only deduplicate helper functions.

Rejected. It would reduce some local duplication but still require orchestrator edits for every destination and would not satisfy the P0 requirement that future destinations register without changing replay/recovery/orchestration logic.

Use generics over concrete destination types.

Rejected. Static generics do not solve URI/project resolution, CLI resume/replay runtime selection, or heterogeneous registries. Object-safe adapters are the smaller product boundary for `cdf-project`.

Move all URI parsing into `cdf-cli`.

Rejected. CLI can own human-facing command errors and redaction, but project runtime must own durable resolution semantics so run, replay, resume, conformance, and future non-CLI callers share one driver graph.

## Consequences

`cdf-project` gets one adapter layer, but the kernel stays clean and package-free.

DuckDB, Parquet, and Postgres keep destination-specific package planning in small adapter modules rather than in the generic replay/recovery skeleton.

CLI run, replay, and resume must migrate away from direct destination-specialized runtime calls and duplicated destination enums.

Conformance/golden/chaos helpers must migrate to generic project APIs before specialized wrapper families are deleted.

`runtime.rs` must become a module facade with focused submodules. The split should follow `.10x/knowledge/rust-crate-organization.md`: types, resource resolution, destination registry/adapters, run orchestration, replay/recovery, ledger events, state-delta/artifacts, receipt validation, and tests must not remain as one monolithic implementation unit.

The generic runtime must preserve the verified-package-before-segment-write invariant recorded in `.10x/reviews/2026-07-07-streaming-commit-session-api-review.md`.
