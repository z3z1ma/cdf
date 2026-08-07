## Rust builds with DuckDB

Routine local Cargo builds, tests, Clippy, and benchmarks whose dependency graph includes DuckDB
must set `DUCKDB_DOWNLOAD_LIB=1` on the Cargo command. If a local command fails to link with
`-lduckdb`, rerun the exact focused command with this setting before classifying the failure as an
environment limitation. Follow `.10x/knowledge/developer-build-duckdb-linkage.md`; published release
builds use the repository's static/bundled path and must not set this developer-only variable.

## Current-only product development

CDF is unreleased, customer-zero software. Implement only the current ratified model.

- Do not add backwards compatibility, migration readers, legacy aliases, fallback parsers, dual
  authorities, deprecated fields, transition flags, or shims for code/config/state that has never
  shipped.
- Delete superseded code, fixtures, dependencies, and names outright. Do not retain code whose only
  purpose is detecting or explaining a removed internal model.
- Do not add tests that merely prove deleted legacy syntax is rejected. Test the current grammar and
  current trust boundaries.
- Do not mention `.10x` ticket ids, tranche names, implementation waves, or internal migration
  history in product errors, help, reports, serialized artifacts, or other user-facing strings.
  Product diagnostics use stable capability/domain language.

## Test authoring

Tests must exercise behavior or a real serialized/generated artifact contract.

- Never read Rust source or test files to assert tokens, function names, imports, module paths,
  source line counts, file length, test-name registries, ownership comments, or dependency
  direction. These checks are brittle text linting, not behavioral evidence.
- Do not replace source-text assertions with a more elaborate Rust parser. Use compiler boundaries,
  visibility, typed APIs, Cargo metadata, Clippy, `cargo machete`, or actual runtime behavior.
- Assertions over generated SQL, manifests, lockfiles, packages, receipts, CLI JSON, and rendered
  output are valid when those bytes are the public artifact under test.
- A passing test proves only its assertions. Record compile-only, link-only, ignored, filtered, and
  environment-backed results honestly.
- Do not weaken, delete, or rewrite a protective behavioral assertion merely to make a run green.

## Economical validation

Follow `QUALITY.md` and keep feedback proportional to the change.

- During implementation, prefer `cargo fmt`, affected-package `cargo check`, focused behavioral
  tests, and strict affected-package Clippy. Do not repeatedly run the workspace suite or product
  smoke matrix while iterating on a narrow repair.
- Run broader certificates only at a named integration/closure barrier or when CI identifies a
  concrete cross-workspace failure. Do not rerun an identical passing command for reassurance.
- Strict Clippy does not enable `clippy::cognitive_complexity`; run it explicitly as a diagnostic on
  changed production packages and treat findings as review input rather than a magic quality gate.
- Periodically run first-party `jscpd` with the scope and thresholds in `QUALITY.md`, and run
  `cargo machete --with-metadata` after dependency, module, fixture, or feature cleanup.
- Scope duplication analysis to first-party implementation/tooling. Do not refactor immutable
  evidence, generated artifacts, or vendored code to improve a repository-wide percentage.

## Architecture and authority

- Keep crate ownership literal. Neutral crates do not depend on product composition or concrete
  adapters; sources do not own shared SQL concepts or depend on sibling sources/destinations;
  destinations do not make runtime/project depend back on them. Composition belongs in the builtin
  driver catalog and CLI boundary.
- Prefer existing typed seams over adapter-specific copies. Do not introduce a generic abstraction
  until at least two real implementations require the same stable contract.
- DataFusion is compile-time SQL analysis authority only. Runtime and durable artifacts carry native
  typed CDF IR, never a DataFusion plan or an ambient SQL/default-resolution branch.
- The current project resource surface is `cdf/<namespace>/<resource>.cdf.sql` with shared typed
  `[sources.<name>]` configuration. Path resource identity, configured source identity, and logical
  destination target are independent authorities and must remain independent end to end.
- Secrets belong in typed source/destination configuration as secret references. Resource SQL,
  manifests, reports, diagnostics, and debug output must not contain or echo secret material.
- Project manifests must bind the exact bytes compiled. Multi-file publication must use the guarded
  project transaction/recovery primitives; never reconstruct a manifest from a later filesystem
  reread or publish a lock/manifest from mixed snapshots.

## Rust structure and naming

- Keep crate roots declarative: module declarations, intentional re-exports, crate documentation,
  and composition only. Put behavior in capability-named modules; split files when one file owns
  unrelated concepts, not to satisfy an arbitrary line count.
- Name modules for the domain concept they own, not the external product that first needed the
  concept. Shared SQL, catalog, transaction, position, and type machinery belongs in a neutral
  crate/module once multiple real adapters consume it.
- Avoid catch-all `utils`, `helpers`, `common`, `misc`, and `glue` modules. A module name must reveal
  its authority; a type name must distinguish configured identity, authored identity, compiled
  identity, and runtime authority when those differ.
- Keep test-only support beside the behavior it exercises or in a clearly named test-support
  module. Production modules must not survive solely because white-box tests import them.

## Correctness, performance, and operational bounds

- Correctness and throughput are both release criteria for connectors. Prefer native bulk,
  columnar, binary, pipelined, concurrent, or server-side protocols; do not introduce row-at-a-time
  network I/O or per-value allocation on a hot path without measured justification.
- Preserve exact source meaning through Arrow/CDF types. Textual mapping is valid when it is the
  declared lossless CDF representation; lossy coercion, silent truncation, locale-sensitive
  parsing, and destination-specific type invention are not.
- Keep memory, disk, concurrency, retry, and batch behavior explicitly bounded. Backpressure must
  propagate; an optimization must not bypass package, receipt, checkpoint, or recovery authority.
- A destination receipt gates checkpoint advancement. Never publish success, advance source
  authority, or garbage-collect replay/staging data before the durable downstream condition that
  owns that transition is proven.
- Treat deletes as first-class keyed package effects where the disposition admits them. Do not
  encode a delete as a nullable/upsert row or hide hard-versus-soft-delete behavior inside an
  adapter.
- Optimize from evidence: record the bottleneck, protocol, batch/concurrency settings, dataset, and
  limiting resource. Preserve a correctness oracle when changing a throughput path.

## Errors and trust boundaries

- Validate untrusted configuration, paths, SQL, external metadata, and wire data at the boundary
  that owns the contract. Fail with the narrow typed error kind and preserve external source/error
  provenance.
- Diagnostics must be stable, actionable, and safe to display. Name the invalid field/path and the
  corrective action when useful; never echo credentials, secret references, raw sensitive values,
  or internal implementation bookkeeping.
- Do not flatten contract/data/environment failures into `Internal`, stringify typed errors only to
  reparse them later, or silently substitute a default after validation fails.

## Working discipline

- The primary agent owns implementation. Do not delegate code changes to subagents; use a separate
  agent only for an explicitly requested independent red-team review.
- Preserve unrelated user changes and untracked files. In particular, do not stage personal Codex
  configuration such as `.codex/config.toml`.
- Make bounded coherent commits and push them as work closes. Check GitHub CI asynchronously after a
  push; repair concrete failures without starting an open-ended review/test loop.
- Use `rg`/`rg --files` for bulk discovery before opening files one by one. Search current and
  terminal `.10x` records before inventing a new owner or repeating an investigation.
