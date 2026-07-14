Status: recorded
Created: 2026-07-13
Updated: 2026-07-13

# FX1 pinned-hints governance

## Observation

Schema hints remain the declared constraint after discovery is pinned. Lock hydration, ordinary plan/run preparation, `--no-pin` inspection, and explicit repinning preserve `SchemaSource::Hints { source, hints_hash, ... }`; none silently converts the resource to unconstrained discovery.

## Procedure

- Added kernel round-trip assertions for pinning and unpinning a hints source while preserving its source and hints hash.
- Extended the CLI Parquet hints scenario to plan and pin once, plan again unchanged, replace the observed file with a compatible wider schema, and plan a third time.
- Compared the lockfile and pinned snapshot bytes after both later commands.

## What it supports or challenges

The later commands report `unchanged`, keep the original baseline bytes, and compile observed drift against that baseline. This closes the aggregate-review critical finding that a second command could republish a hints snapshot.

## Limits

This evidence covers local Parquet hints and the shared schema-source transition primitive. Aggregate FX1 review remains failed until the remaining descriptor, plan-authority, build-domain, and remote external-codec findings are reconciled.
