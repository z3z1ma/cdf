Status: done
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/done/2026-07-11-p0-destination-extension-boundary.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx3-generic-lock-doctor-replay.md

# P0 DX4: destination extension conformance and build-graph law

## Scope

Replace repeated concrete conformance enums/factories with one data-driven adapter catalog, add the quasar-driver extension law and static dependency/import gates, and measure the Cargo rebuild graph for a destination-only edit.

## Acceptance criteria

- One fixture catalog entry enrolls a destination in every applicable shared law.
- Generic conformance assertions contain no destination-name match arms.
- Static tests prevent `cdf-project`/generic CLI imports of concrete destinations.
- Before/after `cargo build --timings` or equivalent evidence shows a destination-only edit no longer rebuilds destination-neutral runtime/project crates unnecessarily.

## Blockers

None. DX3 is terminal.

## Progress and notes

- 2026-07-11: Strict all-target inspection confirms the concrete destination conformance factories are stale after destination constructors moved behind runtime adapters: `cdf-conformance` still calls removed `ResolvedProjectDestination::{duckdb,postgres,parquet_filesystem}` constructors, and one run-matrix path calls removed `RestResource::compiled`. This directly validates DX4's catalog migration scope; production/source checks remain independent.
- 2026-07-11: Added the single conformance destination catalog and moved live-run, run-matrix, runtime-chaos, drift-quarantine, and acceptance-demo destination resolution through the same `DestinationRegistry`/driver authority as production. The removed project constructors remain deleted; no compatibility shim was restored. Static tests pin the three first-party registrations and prohibit concrete destination imports outside the explicit CLI composition/adapter diagnostics and temporary test-only project helper owner. Strict all-target Clippy and the four 100-rebuild package goldens pass. The live DuckDB golden now reaches execution and fails at the independent source-extension boundary because its fixture has not yet resolved the neutral source plan; `.10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md` owns that migration. Evidence: `.10x/evidence/2026-07-11-p0-dx4-conformance-catalog-milestone.md`.
- 2026-07-11: Corrected the conformance crate boundary after a downstream DuckDB test build proved that public live-run helpers cannot call `cfg(test)`-only catalogs. Destination/source fixture catalogs and their adapter dependencies are now ordinary conformance-harness library code; downstream destination suites compile the exact same public helper implementation. The 24-test DuckDB suite passes and strict all-target DuckDB Clippy plus normal `cdf-conformance` check are green.
- 2026-07-16: SX1 closure routed the remaining external-source law breadth here. DX4's data-driven catalog must cover retry/identity change, cancellation, memory failure, redaction/egress, projection/filter/limit, deep validation, and jobs invariance without reopening the source extension implementation ticket.
- 2026-07-18: Removed the final production-module exception instead of retaining a
  test compatibility surface. `ResolvedProjectDestination::{duckdb, parquet_filesystem,
  postgres}` no longer exists, even under `cfg(test)`; adapter construction lives in the
  explicitly test-only `test_destinations` module and all generic project call sites use
  the neutral `ResolvedProjectDestination::new` boundary. The conformance architecture
  gate now permits no concrete destination import anywhere under `cdf-project/src/runtime`.
- 2026-07-18: Proved the normal Cargo graph from an exact `git archive` of commit
  `6d3b6897`. The reverse normal dependency graph of `cdf-dest-duckdb` contains only
  `cdf-cli`, `cdf-conformance`, and `cdf-benchmarks`; neither `cdf-project` nor
  `cdf-runtime` is a dependent. Direct normal-tree scans for both neutral crates contain
  no `cdf-dest-*` package. A destination-only edit therefore cannot invalidate either
  neutral crate under Cargo's dependency graph.
- 2026-07-18: The first closure review rejected the synthetic destination because it was
  only a Parquet scheme alias, rejected concrete `LivePostgres` lifecycle threading through
  generic run/chaos functions, found metadata/count-only footprint oracles insufficient,
  and found the static source gates too narrow. All four findings were repaired together.
  Quasar is now a distinct driver with its own scheme, runtime identity, sheet, durable
  commit record, receipt, and observable payload. One `ConformanceEnvironment` owns optional
  external services behind the catalog boundary. Duplicate/crash footprints compare exact
  bytes or logical rows, and fresh artifact replay compares destination payload rather than
  only receipts/checkpoints. Static tests scan every project production source and the exact
  generic run/chaos engine files for concrete imports and destination-name dispatch.
- 2026-07-18: Renamed every live mock-destination use from ordinal language to the plausible
  `quasar` identity. Historical terminal records retain their original wording as immutable
  evidence; active specs, tickets, coverage, code, fixtures, schemes, and assertions use
  Quasar.
- 2026-07-18: The repaired-slice re-review found four remaining declaration/oracle gaps,
  again repaired without waiver. Quasar now honestly declares append-only disposition and
  unsupported corrections instead of claiming unimplemented replace/correction behavior;
  catalog provenance expectations are catalog data. All four destinations expose one typed
  logical conformance payload (`id`, `name`): DuckDB/Postgres query it, Parquet decodes it,
  and Quasar durably records it. Every chaos recovery now compares that payload to the exact
  prepared-package rows, including both pre-write windows. The dispatch gate covers
  `run_matrix/mod.rs` and derives both catalog and runtime identities from the catalog, so a
  new enrollment automatically extends the prohibition.

## Evidence

- `cargo fmt -p cdf-project -p cdf-conformance --all -- --check` against the isolated
  exact-tree copy passed after the helper deletion.
- `cargo test -p cdf-project --lib --locked -j 12 --no-run` passed. This compiles all
  208 project tests after migrating every removed helper call without executing unrelated
  integration fixtures.
- `cargo test -p cdf-project runtime_tests::generic_lock_plan_replay_and_recovery_drive_mock_runtime_without_destination_branch --locked -j 12 -- --exact --nocapture`
  passed. The quasar project driver inspects, supplies lock artifacts and health, plans,
  replays, deduplicates, and recovers through registry/runtime traits.
- `cargo test -p cdf-cli injected_quasar_destination --locked -j 12 -- --nocapture`
  passed three cases. They cover lock, plan, run, duplicate replay, resume from finalized
  package, resume from durable receipt, doctor, inspect, secret redaction, and checkpoint
  settlement for an injected quasar destination.
- `cargo test -p cdf-conformance destination_catalog --locked -j 12 -- --nocapture`
  passed five cases. The quasar driver inherits shared protocol and bulk-preflight laws,
  first-party entries publish measured bulk/provenance declarations, and the zero-exception
  project-runtime import gate passes.
- `cargo clippy -p cdf-project -p cdf-conformance --all-targets --locked -j 12 -- -D warnings`
  passed against the isolated exact-tree copy.
- `cargo clippy --workspace --all-targets --all-features --locked -j 12 -- -D warnings`
  passed against the isolated exact-tree copy after the review repairs. This is stricter
  than the fast-CI Clippy slice and covers every crate, target, and feature.
- `cargo fmt --all -- --check` passed against the same exact tree.
- `cargo test -p cdf-conformance run_matrix::tests::registered_source_catalog_cells_persist_output --locked -j 12 -- --nocapture`
  passed the generated matrix. Every applicable source/disposition cell executed for DuckDB,
  Parquet, Postgres, and Quasar; unsupported disposition cells were excluded only from
  inspected destination sheets. Every executed cell verified package, receipt, checkpoint gate,
  duplicate no-op with a content-sensitive footprint, and exact logical payload replay.
- `cargo test -p cdf-conformance runtime_chaos::tests::cross_destination_generic_runtime_stage_chaos_persists_output --locked -j 12 -- --nocapture`
  passed all 16 generated destination/crash-window cases, including distinct Quasar receipt
  identities, content-sensitive duplicate/recovery footprints, and exact typed payload
  equality against the three prepared package rows after every recovery window.
- `cargo test -p cdf-cli injected_quasar_destination --locked -j 12 -- --nocapture`
  passed three product-path cases covering lock, plan, run, replay, resume, doctor, inspect,
  redaction, and duplicate suppression through a caller-registered Quasar driver.
- The exact fast-quality workflow commands passed: locked metadata, workspace formatting,
  core Clippy, 368 core library tests (`90+70+64+144`, with 8 explicit performance ignores),
  35 CLI-core tests, 37 CLI-artifact tests, and CLI-core all-target/all-feature Clippy.
- Broad-suite limit: executing all project tests on the clean pre-change commit and on
  this tranche both reproduces the same default-test-thread stack overflow in the A8 drain
  settlement case; the unchanged pre-change case passes with `RUST_MIN_STACK=16777216`.
  It is not DX4 closure evidence and no assertion was weakened. Fast CI does not execute
  this integration suite; this tranche's full test target compiles and every governing
  DX4 scenario executes under the default stack.

## Review

First independent closure review verdict: **fail**, with four findings: the synthetic driver
was identity-equivalent to Parquet; generic orchestration leaked concrete Postgres lifecycle;
behavioral oracles did not compare payload values/bytes; and static import/dispatch gates were
incomplete. No finding was waived. The single repair batch is recorded above. Final independent
re-review then found two dishonest Quasar declarations and two remaining chaos/static-gate
holes. No finding was waived; the second repair batch is recorded above and received the
targeted verification below.

Final targeted independent verification verdict: **pass**. The reviewer confirmed Quasar's
append-only/default-capability contract is honest, every chaos window compares the exact typed
three-row payload, all destination payload adapters share one logical row model, the static gate
derives catalog and runtime identities (including `parquet_object_store`), and no guarded generic
source imports or branches on a concrete destination. No critical or significant finding remains.

## Retrospective

The production graph had already been inverted, but a `cfg(test)` method on a production
type kept concrete adapters and compatibility-shaped constructors in the runtime owner.
Static gates should protect directories with zero exceptions; fixture composition belongs
in an explicitly test-only module. Build timing is weaker than graph authority here: Cargo's
normal reverse-dependency graph proves which crates a destination edit can invalidate,
without host/cache noise.

The first synthetic-driver attempt also showed that a catalog label is not an extension law:
the driver must carry a distinct identity through inspection, resolution, mutation, receipt,
and replay, and the shared assertion must verify each join. Payload checks should compare
logical user data while excluding destination-allocated provenance such as load timestamps;
full footprints should remain byte-sensitive so duplicate and crash laws can detect mutation.
Capability claims and disposition semantics are part of an extension fixture's contract, not
decorative coverage metadata. A small synthetic driver should declare only what it implements;
applicable shared laws follow from that honest sheet rather than forcing every driver into the
same feature set.
