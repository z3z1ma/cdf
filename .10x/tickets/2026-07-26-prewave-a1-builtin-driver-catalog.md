Status: open
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

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
