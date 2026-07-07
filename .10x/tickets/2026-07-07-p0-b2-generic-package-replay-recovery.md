Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-p0-workstream-b-open-orchestrator-world.md
Depends-On: .10x/tickets/2026-07-07-p0-b1-runtime-registry-foundation.md

# P0 B2: Generic package replay and recovery spine

## Scope

Replace destination-specialized package replay/recovery internals with one generic replay/recovery skeleton over `ProjectDestinationRuntime`, kernel `DestinationProtocol`, and segment-writing `CommitSession`.

Owns:

- generic package replay and durable-receipt recovery functions inside `cdf-project`;
- DuckDB, Parquet, and Postgres project destination runtime adapters for package replay/recovery;
- generic runtime stage hook/failpoint injection for replay/recovery;
- mock destination registration tests proving a new destination can use the generic replay/recovery path without orchestrator edits.

## Acceptance criteria

- One generic replay path handles package open/verify, checkpoint proposal, package loading status, destination commit session, receipt identity validation, trait-level receipt verification, checkpoint commit, and package checkpointed status.
- One generic recovery path handles durable receipt validation, trait-level receipt verification, checkpoint commit/reuse, and package checkpointed status without destination mutation.
- DuckDB, Parquet, and Postgres replay/recovery use project destination runtime adapters and preserve existing receipt content, idempotency, duplicate handling, package receipt reporting, and package identity behavior.
- Generic stage hooks cover all currently ratified crash windows. DuckDB-specific failpoint names may exist only as internal compatibility adapters for tests that have not yet migrated.
- A mock registered destination test exercises plan -> replay -> recover -> generic failpoint injection with zero generic orchestrator edits.
- `cdf-project` runtime tests for package replay/recovery pass through the generic path.

## Evidence expectations

Record focused `cdf-project` tests, mock destination registration test output, wrapper/internal duplication reduction notes, trait-level verification proof, and `rg` output showing destination-specific replay/recovery internals no longer own the commit gate.

## Explicit exclusions

No CLI caller migration, no run-project resource execution migration, no conformance matrix expansion, no new destination, and no public performance claim.

## Progress and notes

- 2026-07-07: Opened from Workstream B. Must preserve the verified-package-before-segment-write invariant from `.10x/reviews/2026-07-07-streaming-commit-session-api-review.md`.

## Blockers

Depends on B1 for adapter traits and module split foundation.
