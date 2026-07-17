Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md

# Postgres source ownership isolation

## What was observed

Postgres table scanning and information-schema discovery now reside in `cdf-source-postgres`, which depends on the kernel, a leaf Postgres protocol crate, Arrow, and the Postgres client. It does not depend on the Postgres destination, project, CLI, runtime orchestration, or another source.

The destination and source share only `PostgresIdentifier` and `PostgresTarget` through `cdf-postgres`. Destination source modules and exports were removed. Existing declarative SQL execution and project catalog discovery import the source adapter, while destination-only planning/commit behavior remains in `cdf-dest-postgres`.

## Procedure

- `cargo test -p cdf-source-postgres --lib` — passed.
- `cargo test -p cdf-dest-postgres --lib` — passed.
- strict Clippy across `cdf-postgres`, source, destination, declarative, and project targets — passed.
- `cargo tree -p cdf-source-postgres --edges normal --depth 1` — shows no destination/project/CLI/source sibling dependency.
- `cargo tree -p cdf-dest-postgres --edges normal --depth 1` — shows the shared leaf but no source implementation dependency.

## What this supports

Postgres source changes can no longer enlarge or rebuild the destination's normal dependency graph, and source semantics are not exposed from a destination namespace.

## Limits

Registry-driven compilation/resolution and removal of the temporary project/declarative direct source dependency remain open on SX1. Live database tests remain opt-in according to the existing harness.
