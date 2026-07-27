Status: done
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`

# Create the built-in driver catalog leaf

## Scope

Create `cdf-builtin-drivers` as the one construction authority for the shipped source, format,
transform, and destination catalogs. Move concrete construction out of `cdf-cli`; make product,
benchmark, conformance, and project integration tests consume the same catalog where applicable.

## Non-goals

- No dynamic registration, service locator, feature-selected partial product, or runtime plugin.
- No movement of CLI inspection/rendering context into the catalog crate.
- No change to catalog order, driver descriptors, generated schemas, plans, package identity, or
  shipped adapter surface.

## Acceptance criteria

- The new leaf constructs the exact current built-in catalogs and process-scoped source
  dependencies.
- `cdf-cli` installs the leaf and contains no concrete registration list.
- `cdf-project` production remains concrete-adapter-free; test dependency duplication is removed
  where the shared catalog or neutral fixtures suffice.
- Static graph tests forbid the leaf below CLI/product composition and prove neutral/core crates
  cannot reach it.
- `cdf-cli/bundled-duckdb` forwards through the catalog leaf to the destination, and the hosted
  release continues building the full static DuckDB binary without `DUCKDB_DOWNLOAD_LIB`.
- Built-in catalog artifacts and representative add/plan/run/doctor tests are byte/field
  equivalent.
- Adding a synthetic catalog entry changes only the catalog leaf and data-driven fixture.

## References

- `.10x/decisions/builtin-driver-catalog-composition.md`
- `.10x/specs/product-build-graph-boundaries.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/destination-extension-runtime-contract.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws8-release-engineering.md`

## Assumptions

- Record-backed: explicit deterministic static composition remains the product model.
- Source-backed: `cdf-transport-http` already owns the reusable HTTP provider.

## Journal

- 2026-07-26: Shaped from the two CLI registry modules and current Cargo graph. Concrete names in
  help/render fixtures are explicitly not in scope.
- 2026-07-26: Execution started from the active composition decision and build-graph specs.
  `graphify-out/graph.json` is present but the `graphify` executable is unavailable on PATH, so
  source/import/Cargo authority is being inspected directly. The working tree was clean at
  activation.
- 2026-07-26: Added `cdf-builtin-drivers` as the deterministic catalog leaf, moved the complete
  source/format/transform/destination construction out of CLI, forwarded the bundled DuckDB
  feature through the leaf, and changed benchmark/conformance/project test owners to consume or
  extend the shared catalog where their fixtures permit.
- 2026-07-26: The first executable catalog test exposed a stale benchmark Parquet destination
  artifact: clean baseline source already published compression-aware v6 paths while the fixture
  still described v5. Refreshed the fixture from current runtime authority and restored its exact
  inspection comparison.
- 2026-07-26: OCR review found three medium concerns: check-then-set catalog initialization,
  manifest-text graph checks, and membership-only fixtures. Repairs serialized initialization,
  moved graph enforcement to all-feature Cargo metadata package/dependency identities, and added
  full descriptor/schema/destination-inspection hashes. Re-review then found duplicated
  destination membership and default-feature-only resolution; one destination construction table
  now drives both registration and inspection, and metadata resolves `--all-features`.
- 2026-07-26: The required post-code `graphify update .` was attempted and again could not run
  because no `graphify` executable exists in the configured tool locations or PATH. The source and
  Cargo graph were verified directly; aggregate graph regeneration remains visible to Z1 rather
  than being claimed as completed evidence here.

## Blockers

None.

## Evidence

- Exact catalog/process scope: `env -u DUCKDB_DOWNLOAD_LIB cargo test -p
  cdf-builtin-drivers --features bundled-duckdb --locked` passed 3 tests. The data fixture maps
  every source descriptor and option schema, format descriptor, transform descriptor, and
  destination description/sheet/runtime/probe artifact to a canonical SHA-256; pointer checks
  prove the installed source/format/transform values are process scoped. The initialization path
  is double-checked under one mutex, so concurrent first access cannot construct discarded
  catalogs.
- CLI composition and behavior: `cargo test -p cdf-cli --features bundled-duckdb` passed the
  architecture fence and representative
  `add_local_ndjson_uses_the_registered_file_driver_without_cli_format_wiring`,
  `plan_json_exposes_pushdown_ddl_guarantee_and_state_advancement`,
  `run_command_commits_package_rows_mirrors_and_checkpoint`, and
  `doctor_registered_source_probe_fails_independently_before_network_or_writes`. These tests prove
  their assertions and representative field/byte outputs; they do not replace the aggregate full
  suite in Z1.
- Shared test/harness ownership: focused `cdf-project` normal-graph and registry-open source tests,
  `cdf-conformance::catalog_is_the_single_first_party_destination_enrollment_point`, and
  `cdf-benchmarks::first_party_destination_catalog_matches_runtime_inspection` passed with the
  leaf's bundled-DuckDB feature. Project production dependencies remain concrete-adapter-free;
  its dev helpers extend leaf format/transform catalogs only where unshipped Avro fixtures require
  it.
- Graph/feature authority: the leaf's Cargo-metadata test passed with `--all-features`, rejects any
  non-dev leaf edge outside `cdf-cli`, `cdf-conformance`, and `cdf-benchmarks`, and resolves aliases,
  optional, target, and build edges by package id. Direct `cargo tree` checks found no leaf path
  from `cdf-cli-core`, kernel, runtime, project, or package-contract.
  `cargo tree -p cdf-cli --features bundled-duckdb -e features -i libduckdb-sys` showed
  `cdf-cli → cdf-builtin-drivers → cdf-dest-duckdb → duckdb/bundled`; the release workflow still
  invokes that feature without `DUCKDB_DOWNLOAD_LIB`.
- Quality: focused all-target checks passed for every touched crate; strict all-target Clippy
  passed for the full touched package set and again for the repaired leaf; `cargo fmt --all
  -- --check` and `git diff --check` passed. An initial unbundled leaf test could not link because
  this host has no system `libduckdb`; the required static feature test passed with the variable
  explicitly unset.

## Review

Open-code-review delegation used OCR workspace preview and file-specific rule resolution. Initial
verdict: `concerns`, with three medium findings (initialization race, incomplete graph fence,
incomplete artifact fixture); first re-review found two remaining medium test-authority gaps
(duplicated destination membership and default-feature-only metadata). All five were repaired.
Final independent verdict: `pass`, no findings. Residual risk: full product/performance integration
is intentionally owned by Z1; the reviewer reran no tests and relied on the executor evidence
above.

## Retrospective

The extraction itself was mechanical; the hard part was proving there was still exactly one
authority. The useful technique was to make construction data drive both installation and
inspection, then snapshot canonical hashes of complete artifacts rather than a hand-selected
field subset.

Two dead ends cost time. `OnceLock::get_or_try_init` remains unstable on the repository toolchain,
so the implementation uses a narrow double-checked mutex while preserving original constructor
errors. Running tests without the product's bundled DuckDB feature failed at link time because
this host intentionally lacks a system library; explicitly testing the forwarded static feature
is the relevant release evidence.

Five whys on the repeated review findings: the first tests mirrored visible source structure;
that mirrored only current names; current names omitted disabled/aliased graph edges and future
destinations; therefore the tests could agree with themselves while authority drifted; the root
cause was deriving proof from duplicated representations rather than Cargo/runtime authority.
The durable lesson was added to
`.10x/knowledge/source-destination-extension-invariant.md`. No new skill is warranted: the
procedure is ordinary catalog/graph review, while DuckDB build/install toil is already covered by
the existing project build skill.
