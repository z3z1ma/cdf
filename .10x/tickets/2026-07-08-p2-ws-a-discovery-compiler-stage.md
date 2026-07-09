Status: open
Created: 2026-07-08
Updated: 2026-07-09
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
- 2026-07-08: Split first executable child `.10x/tickets/done/2026-07-08-p2-ws-a1-schema-source-model-snapshot-foundation.md` for the schema-source model and snapshot artifact foundation before per-source probes.
- 2026-07-09: Child A1 closed with evidence `.10x/evidence/2026-07-09-p2-ws-a1-schema-source-model-snapshot-foundation.md` and review `.10x/reviews/2026-07-09-p2-ws-a1-schema-source-model-snapshot-foundation-review.md`.
- 2026-07-09: Split executable child `.10x/tickets/done/2026-07-09-p2-ws-a2-local-parquet-discovery-probe.md` for the first concrete local Parquet footer/schema probe and schema snapshot handoff. Remote ranged discovery, CLI schema commands, auto-pin, and run/plan integration remain later children.
- 2026-07-09: Child A2 closed as `.10x/tickets/done/2026-07-09-p2-ws-a2-local-parquet-discovery-probe.md`, adding the first concrete local Parquet footer/schema discovery API and schema snapshot handoff helper. Evidence is `.10x/evidence/2026-07-09-p2-ws-a2-local-parquet-discovery-probe.md`; review is `.10x/reviews/2026-07-09-p2-ws-a2-local-parquet-discovery-probe-review.md`.
- 2026-07-09: Split executable child `.10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md` for the first operator-visible and first-use discovery flow: `cdf schema discover <resource>` for local single-file Parquet plus auto-pinning discovered snapshots before plan/run.
- 2026-07-09: User clarified discovery is a product-wide compiler capability, not a Parquet convenience. Future WS-A children must keep the abstraction source-archetype-neutral and extend discovery to declarative SQL/database resources, REST, future Avro-like file formats, Python generator resources, and WASM resource boundaries as those surfaces stabilize.
- 2026-07-09: Child A3 closed as `.10x/tickets/done/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md`. Evidence is `.10x/evidence/2026-07-09-p2-ws-a3-local-parquet-discover-autopin.md`; review is `.10x/reviews/2026-07-09-p2-ws-a3-local-parquet-discover-autopin-review.md`. This closes the local single-file Parquet `cdf schema discover` and plan/run auto-pin slice; remote, multi-file, SQL, REST, Python, WASM, `schema pin/show/diff`, `cdf add`, and conformance S1/S2/S8 remain later children.

## Blockers

None for shaping. Executable child tickets may need dependencies on WS-D/E for remote ranged reads.
