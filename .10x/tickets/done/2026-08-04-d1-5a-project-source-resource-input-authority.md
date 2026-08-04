Status: done
Created: 2026-08-04
Updated: 2026-08-04
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-d1-project-compilation-manifest-core.md`, `.10x/tickets/done/2026-08-03-d1-compile-cli-and-manifest-sql.md`

# D1.5a project source/resource input authority

## Scope

Implement the internal, typed Foundation D compiler-input boundary for the ratified current project
shape:

- exact `sources/<source>/<resource>.cdf.sql` discovery under the existing project path/stable-read
  fences;
- dedicated source/resource path-token types and canonical `<source>.<resource>` derivation;
- typed `[sources.<source>]` base configuration and sparse
  `[environments.<env>.sources.<source>]` overlays with immutable source type;
- deterministic joins among paths, source configurations, selected driver descriptors, authored
  SQL bytes/hashes, and manifest-ready provenance;
- source-option validation through the already registered driver's closed `source` schema without
  external I/O;
- one bounded downstream input model consumed by D3 for `upstream(...)` parsing and native
  lowering.

This child establishes compiler input authority only. It MUST NOT expose a second user-selectable
project mode. The D3 cutover consumes this boundary, makes SQL resources executable, and deletes
the retired resource map/declarative reader atomically.

## Non-goals

- parsing or lowering the SQL envelope, `upstream(...)`, projections, filters, casts, semantics, or
  execution policy;
- native scalar/relational IR expansion, owned by D2;
- changing manifest publication/recovery or `cdf sql` read-only behavior;
- scaffold/add/example cutover or deletion of the old front-end before D3 can replace it completely;
- source discovery, health, secret resolution, network/database/filesystem adapter I/O;
- a disabled/inactive/preconfigured source state;
- any compatibility reader, migration, feature flag, dual authoring mode, or fallback.

## Acceptance criteria

1. Dedicated project source/resource token types accept exactly
   `[a-z][a-z0-9_]{0,127}`, preserve accepted bytes, and reject invalid/overlength input with the
   exact path and remediation; general kernel ids and destination normalization remain unchanged.
2. Discovery admits only regular `sources/<source>/<resource>.cdf.sql` inputs under existing root,
   symlink, stable-read, UTF-8, and size/count fences; ordering is canonical source then resource
   and independent of directory iteration order.
3. Every source directory joins to exactly one `[sources.<source>]`, and every configured source
   joins to a directory with at least one valid resource. Missing, empty, orphaned, nested,
   duplicate/colliding, and malformed near-match cases fail `Contract` before SQL parsing or I/O.
4. Base source configuration requires one type. A selected environment may sparsely override only
   admitted source options and may not add/remove/rename a source or override type. The canonical
   effective configuration is deterministic and secret-redacted for display/artifact use.
5. Source type resolves through `SourceRegistry` only after path/config binding. Effective source
   options validate through that driver's closed source schema without validating resource options
   early and without invoking discovery, health, secret resolution, or execution.
6. One bounded typed inventory exposes authored relative path/hash, source/resource names and
   canonical id, selected environment, base/overlay/effective source configuration provenance and
   hashes, and driver descriptor/schema identities needed by D3/manifest assembly. It contains no
   resolved secret values, absolute host paths, generic unbounded JSON ownership, or SQL-derived
   identity.
7. Empty projects with no configured sources/resources are valid. `cdf.toml` containing any
   configured source without a valid resource is invalid; no inactive state or implicit source is
   introduced.
8. Existing product behavior remains buildable while this internal boundary is introduced, but no
   runtime/CLI branch can select it as a coequal authoring mode. The D3 cutover has one obvious
   consumption seam and no compatibility abstraction must later be removed.
9. Focused model/discovery/overlay/driver-schema tests, affected-crate checks, formatting,
   `git diff --check`, and strict affected-crate Clippy pass without a whole-workspace suite.

## References

- `.10x/decisions/filesystem-source-resource-and-configuration-authority.md`
- `.10x/decisions/project-path-tokens-and-upstream-relation-binding.md`
- `.10x/specs/project-source-resource-layout.md`
- `.10x/specs/sql-project-authoring.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/research/2026-08-03-project-compiler-authority-inventory.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`

## Assumptions

- Record-backed: existing project file stable reads, path fences, bounded manifest inputs,
  `SourceRegistry`, driver descriptors, and closed source/resource option schemas are the reusable
  authorities named by the references.
- User-ratified: exact path grammar, blocking empty configured sources, path-only identity,
  source/type/config resolution order, environment overlay constraints, `upstream(...)` relation
  ownership, and no compatibility machinery.
- Mechanical: the internal type/module names may follow surrounding `cdf-project` conventions as
  long as user-facing reports use the ratified vocabulary and no retired `ProjectResource.source`
  locator meaning leaks into the new boundary.

## Journal

- 2026-08-04: Opened after the user confirmed all remaining D1.5 path/config/relation choices.
  Source inspection established that kernel `ResourceId` is intentionally broad, destination
  normalization is a different domain, and current drivers already separate closed `source` and
  `resource` schemas. The child is intentionally limited to internal compiler input authority so
  D2 can land independently and D3 can perform one complete current-authoring cutover without a
  half-runnable or compatibility mode.
- 2026-08-04: Execution began from the complete referenced authority. The selected seam extends the
  existing typed `ProjectConfig` with source declarations/overlays, adds one internal bounded
  filesystem/configuration inventory, and adds one narrow `SourceRegistry` source-only validation
  method. SQL parsing/lowering remains wholly outside this child. `graphify` remains unavailable in
  this environment as previously recorded, so direct source/import/test inspection is the active
  authority fallback.
- 2026-08-04: Implemented dedicated exact-token types, canonical filesystem enumeration, stable
  no-follow reads, UTF-8/count/byte fences, path/config joins, exact selected-environment overlays,
  typed configuration/input hashes, driver descriptor provenance, and source-section-only schema
  validation. The first focused test invocation compiled the target but exposed a local linker
  environment omission (`-lduckdb`); pointing `LIBRARY_PATH` and `DYLD_LIBRARY_PATH` at the already
  built `target/debug/deps` library made the same bounded test target runnable without changing
  source or broadening validation.
- 2026-08-04: The primary-agent review found that the first implementation joined and validated
  driver configuration after source-directory admission but before resource-file admission. That
  could let an invalid driver configuration mask a malformed/empty resource directory, contrary to
  the ratified path -> configured source -> type -> registry order. Reordered the inventory so all
  source/resource shapes, identities, counts, stable bytes, and hashes are admitted first; added
  discovery-time source/resource count stops and a focused error-precedence assertion. No SQL is
  parsed and no driver is consulted until filesystem/configuration authority is complete.
- 2026-08-04: Final focused validation and the one independent D1.5a adversarial review passed.
  The implementation is not wired to any CLI/runtime branch; D3 remains the sole cutover owner.

## Blockers

None. The ticket is executable from its referenced authority.

## Evidence

- AC 1-8: `LIBRARY_PATH="$PWD/target/debug/deps" DYLD_LIBRARY_PATH="$PWD/target/debug/deps"
  cargo test -p cdf-project --lib project_inputs::` passed all 9 focused tests (278 filtered out).
  The assertions cover exact 1-128-byte token admission/remediation, canonical resource ordering
  and ids, selected overlay/hash provenance, source-only Postgres schema admission without a
  resource relation, unknown source-option rejection, empty/missing/orphaned sources, immutable
  overlay shape, nested/direct/malformed files, deterministic unrelated-file handling, and symlink
  rejection. Limit: deliberate concurrent filesystem mutation is enforced by metadata/open-handle
  identity checks but is not timing-tested.
- AC 5 and 8: `rg -n "inventory_project_source_resources" crates --glob '*.rs'` found the seam only
  in `cdf-project` definition/re-export and its focused tests; no runtime or CLI selection branch
  exists. The registry method directly validates only the registered driver's closed `source`
  schema and returns its exact descriptor; it does not call compatibility hooks, resource schema
  validation, discovery, health, secret resolution, or execution.
- AC 8-9: `cargo check -p cdf-runtime -p cdf-project --all-targets` passed. Limit: affected crates
  and their compile graph only; no whole-workspace suite was run.
- AC 9: `cargo clippy -p cdf-runtime -p cdf-project --all-targets -- -D warnings` passed; `cargo fmt
  --all -- --check` and `git diff --check` passed. Limit: affected crates only, intentionally.

## Review

One independent read-only red-team review inspected the governing records, full D1.5a diff, and
focused assertions after the ordering/count repair.

- Findings: none.
- Verdict: pass.
- Residual risk: deliberate concurrent filesystem mutation is guarded by directory metadata and
  no-follow open-handle identity checks but is not timing-tested. D3 must consume this inventory as
  its sole input seam and atomically delete the retired authoring path; that is existing D3 scope,
  not debt introduced here.

## Retrospective

- The difficult boundary was ordering, not parsing: path/resource admission must finish before a
  driver error can become authoritative. Writing the error-precedence test made the architectural
  rule executable rather than merely implied by happy-path coverage.
- Reusing the manifest's 64 MiB/100,000-input fences and existing no-follow/stable-read pattern
  avoided both a new policy and a filesystem helper abstraction with only one consumer.
- The narrow registry method was the correct cut: calling the existing combined validator would
  have invoked resource compatibility before `upstream(...)` existed and leaked relation concerns
  into project source authority.
- The local test-link failure was not a code failure; the already-built DuckDB library existed in
  `target/debug/deps`, and supplying that path restored the exact focused target. Future local
  validation should preserve that environment rather than recompiling or broadening test scope.
- No compatibility mode, legacy shim, CLI branch, external I/O, or follow-up repair was introduced.
  The remaining work is the already-owned D2/D3 dependency chain.
