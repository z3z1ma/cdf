Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, .10x/specs/source-extension-runtime-contract.md

# P0 SX1: source driver registry and resource-plan boundary

## Scope

Implement neutral source driver/registry/plan/config schema contracts, extend capabilities, migrate file/REST/Postgres compilation/discovery/runtime/product hooks through drivers, extract Postgres source ownership from the destination crate, and prove a mock external source. Preserve existing TOML ergonomics/artifacts through an explicit migration.

## Acceptance criteria

- Generic declarative/project/CLI/discovery code contains no file/REST/SQL/Postgres source-kind match tree.
- Standard JSON Schema remains precise for common and driver fields; runtime parser is registry-open.
- Project/generic CLI have no `cdf-source-*` dependency/import; Postgres source is independent of its destination.
- Existing file/REST/Postgres add/discover/deep/preview/plan/run/doctor behavior passes compatibility/golden conformance.
- Extended capability declarations are live-falsified and sufficient for P3 scheduler admission without source ids.
- A mock source adds only driver crate/test, composition/schema catalog, and fixture entries and inherits every applicable law.
- Cargo graph/rebuild evidence proves source dependency isolation.

## Evidence expectations

Config/schema migration goldens, dependency/static tests, mock source, first-party parity matrix, discovery/package artifact hashes, redaction/egress/retry/memory/jobs tests, build graph evidence, and adversarial extension review.

## Explicit exclusions

No new source protocol, parser optimization, dynamic ABI, or distributed execution.

## Blockers

Depends on neutral runtime, memory, and execution-host contracts. P3 source scheduling and remote overlap must use this boundary.

## References

- `.10x/decisions/source-driver-registry-and-resource-plan-boundary.md`
- `.10x/research/2026-07-11-source-extension-boundary-audit.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/knowledge/source-destination-extension-invariant.md`

## Progress and notes

- 2026-07-11: Added the engine-neutral `SourceDriver`, `SourceRegistry`, compiled source plan, resolution context, and scheduler-facing execution capability contracts to `cdf-runtime`. Compiled plans bind driver/version/option-schema authority and canonical redacted-option/physical-plan hashes; registry resolution rejects authority drift. A mock driver proves deterministic registration, compilation, serialization, and resolution without source-id scheduling branches. First-party source migration and dependency isolation remain open.
