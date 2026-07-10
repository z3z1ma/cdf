Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-09-p2-ws-a10c-exhaustive-local-binary-discovery.md, .10x/tickets/done/2026-07-10-p2-rp3-correction-capabilities-row-provenance.md, .10x/tickets/done/2026-07-10-p2-rp4-schema-scope-lease-lock-cas.md
Verdict: pass

# P2 A10c/RP3/RP4 integration and extension-cost review

## Target

Adversarial review of exhaustive local binary discovery/pinning, destination correction capability and row-provenance vocabulary, fenced schema leases, and guarded lockfile publication. This review applies `.10x/knowledge/source-destination-extension-invariant.md` as a P0 closure criterion.

## Assumptions tested

- Multi-file discovery is one resource-set compiler stage rather than repeated single-file Parquet logic.
- Adding an equivalent binary format does not require source-specific CLI/run branches.
- Adding a destination does not require spelling every unsupported future capability or editing generic orchestration.
- Destination capability claims are independently falsifiable.
- Lease time and fencing authority belong to the store rather than the executor.
- Exact lockfile bytes plus rename are sufficient without serializing every CDF writer.
- Compatibility helpers and public option structs cannot silently become future extension traps.

## Findings

- Significant, repaired: discovery initially ignored the exact supplied baseline hash and recomputed authority. Baseline input is now an opaque, resource-bound token returned only after snapshot/manifest hydration and verification; cross-resource use fails before probing.
- Significant, repaired: the legacy singular Parquet helper accepted a multi-file set and returned only the first partition. It is deprecated and cardinality-one/fail-closed; all product paths use the exhaustive artifact API.
- Significant, repaired: the new public discovery options initially exposed fields. It is now non-exhaustive with private fields and builders, so sampling/executor/cache policy can extend without another struct-literal break.
- Significant, repaired: destination correction state was first added directly to `DestinationSheet`, forcing edits in every destination and breaking public struct literals. A defaulted `DestinationProtocolCapabilities` aggregate and `sheet_artifact()` seam now preserve `DestinationSheet`; only Postgres overrides the truthful unsupported default, generic conformance consumes the aggregate, and lock snapshots retain it.
- Significant, accepted one-time migration: persisted capability authority required a durable lock extension slot. `LockedDestination` is now non-exhaustive and constructed through its artifact constructor. This is the sole `cdf-project` semver finding and prevents repeated public field additions; legacy lock bytes and default sheet hashes remain exact. The tradeoff is explicit in `.10x/decisions/destination-protocol-capabilities-extension-seam.md` and is not a general compatibility waiver.
- Significant, repaired: lease callers originally supplied `now_ms`, allowing an executor to seize a live lease by advancing time. Stores now own an injectable `ScopeLeaseClock`; production uses system time and conformance uses a deterministic clock.
- Significant, repaired: promotion CAS had a final-check/rename TOCTOU against another CDF lock writer. All production CDF lock mutations now share a project advisory guard and exact prior authority; ordinary create/update and CAS use synchronized temp-write, sync, atomic install, cleanup, and parent sync.
- Pass: the discovery orchestrator owns scheduling/evidence while Parquet and IPC own only format probes. Runtime full-file identities and discovery bounded observations remain separate.
- Pass: destination strategy/planner models contain no driver or CLI types, and generic conformance can reject false adapter claims without destination-name branches.
- Pass: the lease kernel contains no filesystem, SQLite, scheduler, source, destination, CLI, Spark, or Flink dependencies. New stores implement one trait and must pass shared conformance.
- Pass: workspace tests, strict lint, docs, supply-chain gates, kernel/state semver, legacy serialization, deterministic goldens, and live Postgres evidence are green.

## Verdict

Pass. All material abstraction, authority, race, and partial-evidence findings were repaired. Extension cost is localized to the intended adapter/trait/artifact boundaries.

## Residual risk

POSIX/advisory locking cannot force an unrelated editor or external process to cooperate. CDF reports that boundary and rechecks exact authority immediately before publication; all CDF-owned writers serialize. The pre-1.0 `LockedDestination` constructor migration is intentional and bounded; future capability families must extend the aggregate rather than reopen the lock record.
