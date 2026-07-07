Status: active
Created: 2026-07-05
Updated: 2026-07-07

# Conformance, governance, and roadmap

## Purpose and scope

This specification governs what "supported" means, release/version governance, MVP contents, fast-follow, and beyond-MVP scope. It derives from book Chapters 19, 21, 22, 23, 24, and 25 and decisions D-11, D-21, D-22, D-26, D-27, and D-28.

## Conformance

Resource conformance MUST test descriptor coherence, capability truth-telling, partition completeness, position replay for resources claiming it, and boundedness honesty.

Destination conformance MUST test every declared disposition, migration operation, idempotency mechanism, receipt verification clause, crash recovery behavior, replay identity, and exact type mappings.

The chaos layer MUST kill the process at every package/checkpoint lifecycle boundary and prove recovery terminates with no cursor ahead of durable data.

Golden-package tests MUST compare produced evidence hash-by-hash against committed fixtures.

Property/fuzz targets SHOULD cover schema inference fixed points, contract verdict totality, position serialization/migrations, and NDJSON/Singer/Airbyte parsers.

## Governance

The project is Apache-2.0, one repository, with crates published under semver. Before 1.0, Rust APIs may break in minor releases, but serialized artifacts MUST NOT break without a migration.

Checkpoint schema, package manifest version, capability-sheet format, WIT world, and declarative JSON Schema MUST be independently versioned specs with committed migration paths and fixtures.

Each cdf minor release MUST pin one load-bearing dependency tuple. Patch releases MUST NOT move those pins.

While CDF depends on Apache DataFusion from a git source, CDF MUST NOT publish crates.io releases unless a later active decision explicitly supersedes this constraint. Current crate manifests MUST carry `publish = false` while that git pin remains. The release path MUST first replace the DataFusion git pin with a crates.io dependency or record the superseding release decision with supply-chain evidence, then remove the manifest publication guard only as part of that governed release work.

## MVP contents

MVP MUST include the kernel, engine, contract compiler, package builder/replayer, SQLite ledger, authoring tiers 0/1/2/4 for Arrow IPC and NDJSON, HTTP toolkit, DuckDB/Parquet/Postgres destinations, HTTP-paginated API/Postgres snapshot-incremental/Parquet-CSV-JSON file sources, append/replace/merge dispositions, CLI except package archive, conformance suites, chaos layer, golden packages, and dlt shim preview.

MVP killer-demo acceptance MUST exercise Tier-0 GitHub issues, plan output, DuckDB load, `cdf sql`, contract freeze and drift quarantine, crash between destination commit and checkpoint commit, resume without source contact, replay into a second database, duplicate replay handling, and state history.

## Fast-follow and beyond MVP

Fast-follow MUST include Singer/Airbyte adapters, `cdf package archive`, dlt shim GA, vault-class secret providers, and the first warehouse destination.

Beyond-MVP scope MUST include distributed execution, streaming supervisor, WASM distribution/registry, and Iceberg/Delta lakehouse destinations. These features MUST reuse packages, partitions, checkpoint store seams, destination receipts, and conformance gates rather than introducing parallel artifact types.

No UI is in scope for the kernel. A future UI, if any, sits above CLI/library surfaces.

## Acceptance criteria

- No connector or destination is called supported until it passes the relevant conformance suite.
- Chaos recovery proves no source cursor advances ahead of durable committed data.
- Serialized artifact fixtures exist for each artifact version and migration.
- The MVP killer demo runs under five minutes on a laptop with no network beyond GitHub.

## Explicit exclusions

This spec does not assign implementation work; child tickets under the parent plan own execution.
