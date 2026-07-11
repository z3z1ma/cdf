Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md

# Postgres neutral driver product path

## What was observed

Compiled Postgres table resources carry their neutral source plan alongside the compatibility plan. Discovery pinning and effective-schema reconciliation update the neutral plan's schema authority, preventing stale empty/discovered schemas from reaching execution.

The CLI has one standard source composition module. For executable Postgres resources it resolves the plan through `SourceRegistry` with project secrets and the injected execution host; production planning, preview, and run therefore receive a `dyn QueryableResource` without constructing a concrete SQL resource in generic command code. Invalid Postgres dialect and table shapes now fail during compilation instead of being deferred to runtime construction.

## Procedure

- declarative neutral-plan/capability test — passed.
- declarative query/dialect and malformed-table/empty-schema failure-path tests — passed.
- CLI Postgres discover/pin/plan/preview/run product test — passed (or used its existing environment-aware skip when Postgres was unavailable).
- strict Clippy for declarative and CLI targets — passed.

## What this supports

The first first-party source now traverses the neutral compile and runtime resolution boundary on the live product path, including schema discovery mutation and managed blocking execution.

## Limits

File and REST remain on compatibility plans, so generic CLI/declarative source-kind dispatch and the legacy SQL inspection wrapper cannot yet be deleted. Project-owned discovery still imports the Postgres source pending driver-hook migration.
