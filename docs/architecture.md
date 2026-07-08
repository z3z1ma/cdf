# Architecture

CDF is a local-first data movement framework built around explicit artifacts:
plans, packages, receipts, checkpoints, lockfiles, and conformance evidence.
The short version is in this page; the durable design is the book in
[VISION.md](../VISION.md).

## Layers

- Kernel: core identifiers, batches, resource traits, checkpoints, receipts,
  destinations, errors, and package-facing meaning. The kernel stays engine-free.
- Engine: planning and execution over Arrow batches, with DataFusion at the query
  boundary.
- Package, state, and destination crates: artifact persistence, SQLite state,
  DuckDB/Parquet/Postgres commit protocols, receipt verification, and replay.
- Project and CLI crates: `cdf.toml`, resource compilation, environment
  resolution, command execution, rendering, and operator workflows.

The active layering/runtime contract is
[`architecture-layering-runtime.md`](../.10x/specs/architecture-layering-runtime.md).
The CLI/project/security surface is
[`project-cli-observability-security.md`](../.10x/specs/project-cli-observability-security.md).

## Artifact Flow

1. `cdf plan` compiles the project resource into a plan and destination preview
   before data movement.
2. `cdf run` executes a resource through the run spine, writes a package,
   commits to a destination, verifies a receipt, then commits checkpoint state.
3. `cdf resume` uses run-ledger events, package artifacts, destination receipts,
   and checkpoint rows to drain interrupted work.
4. `cdf replay package` drives an existing package into a destination without
   contacting the source.
5. `cdf sql`, `cdf inspect`, `cdf state`, `cdf package`, `cdf doctor`, and
   `cdf status` expose local operational evidence.

The run/recovery behavior is governed by
[`run-orchestration-ledger.md`](../.10x/specs/run-orchestration-ledger.md), and
the destination receipt guarantees are governed by
[`destination-receipts-guarantees.md`](../.10x/specs/destination-receipts-guarantees.md).

## Current Product Boundary

The MVP scope is the Chapter 18 CLI minus future-only pieces, conformance suites,
chaos/golden testing, package replay, SQLite state, and the first destinations.
Chapter 23 of [VISION.md](../VISION.md#chapter-23-mvp) is the roadmap cutline.

Documentation and onboarding are governed by
[`docs-onboarding-surface.md`](../.10x/specs/docs-onboarding-surface.md).
Conformance and release governance are governed by
[`conformance-governance-roadmap.md`](../.10x/specs/conformance-governance-roadmap.md)
and
[`versioning-lts-release-policy.md`](../.10x/specs/versioning-lts-release-policy.md).

## What This Page Does Not Define

This page does not define new behavior, generated command syntax, error codes,
artifact schemas, or release promises. Generated command reference and error
catalog pages are owned by
[WS6B](../.10x/tickets/2026-07-08-p1-product-ws6b-generated-reference-freshness.md).
