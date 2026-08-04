Status: open
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

## Blockers

None. The ticket is executable from its referenced authority.

## Evidence

Pending execution.

## Review

Pending one independent adversarial review after implementation evidence is complete.

## Retrospective

Pending execution.
