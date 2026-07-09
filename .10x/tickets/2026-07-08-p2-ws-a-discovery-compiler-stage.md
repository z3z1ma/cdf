Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md

# P2 WS-A discovery as a compiler stage

## Scope

Implement discover and hints schema modes end to end as a bounded plan-time compiler stage with pinned schema snapshots, lockfile references, plan/package stamping, and CLI schema commands.

This workstream is broad. Split executable child tickets before code for kernel/source model changes, per-source probes, snapshot store/lockfile wiring, auto-pin behavior, and CLI commands.

## Acceptance criteria

- `Declared`, `Hints`, and `Discover` are representable without conflating unpinned discovery with pinned snapshots.
- Parquet, Arrow IPC, CSV/JSON/NDJSON, SQL, and REST have bounded discovery probes or explicit exclusions with rationale.
- Schema snapshots are written under `.cdf/schemas/<resource>@<hash>.json`, referenced from `cdf.lock`, and stamped into plans/packages.
- `cdf schema discover|pin|show|diff` exist with P1 rendering and additive JSON output where applicable.
- First-use auto-pin is visible in plan/run reports and deterministic for unchanged content.
- REST no longer fails solely with "requires a declared schema hash" when discover mode has a pinned snapshot.

## Evidence expectations

Focused tests for each probe, snapshot determinism, lockfile/package evidence, CLI command snapshots, redaction checks, and conformance scenarios for S1, S4, and S5 as they become available.

## Explicit exclusions

This ticket does not implement file manifest incrementality, remote transport credentials, or the full declarative type parser beyond what WS-B owns.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.

## Blockers

None for shaping. Executable child tickets may need dependencies on WS-D/E for remote ranged reads.
