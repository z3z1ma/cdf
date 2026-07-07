Status: active
Created: 2026-07-07
Updated: 2026-07-07

# Arrow/DataFusion Tuple Policy

## Context

`VISION.md` makes DataFusion part of the day-zero architecture. `.10x/decisions/datafusion-tier-b-delegation-boundary.md` already ratifies that CDF will implement real DataFusion `TableProvider` execution for Tier B resources, while keeping kernel and authoring APIs Arrow-only.

The current blocked tuple ticket records a dependency mismatch: CDF first-party Arrow crates are on Arrow 59 while the inspected DataFusion 54 dependency graph uses Arrow 58. A direct production adapter cannot hand Arrow 59 batches into that DataFusion graph without either same-major alignment, a CDF Arrow repin, or a bridge.

The user ratified the recommended no-permanent-bridge policy on 2026-07-07 with one hard clarification: this policy MUST NOT be used to avoid DataFusion. DataFusion remains mandatory for the engine boundary, and CDF must jump through the necessary compatibility hoops to use it.

## Decision

CDF will keep DataFusion as a required day-zero engine dependency and architecture boundary.

The target dependency policy is one same-major Arrow/DataFusion tuple per CDF minor release. The preferred path is to use or upgrade to a DataFusion release whose Arrow major matches CDF's first-party Arrow major.

CDF will not introduce a permanent Arrow-major bridge in the engine hot path. A bridge may be considered only as a temporary, explicitly bounded implementation decision with expiry, benchmarks, golden-package proof, and supply-chain review.

If same-major DataFusion is not available when the TableProvider work is executed, the next acceptable compatibility path is a deliberate CDF first-party Arrow repin to DataFusion's Arrow major, but only after golden-package determinism and artifact-compatibility evidence prove the move safe.

Waiting for upstream DataFusion is acceptable only as a dependency-tuple alignment tactic, not as an excuse to leave DataFusion unused or replace it with a parallel engine.

## Alternatives considered

Keep DataFusion vocabulary but avoid real DataFusion execution.

Rejected. The user explicitly clarified that DataFusion is not optional, and the active boundary decision already rejects the thin metadata-only interpretation.

Adopt a permanent Arrow-major bridge.

Rejected. It would normalize dependency tuple drift and put conversion complexity on the engine hot path indefinitely.

Immediately repin CDF Arrow crates without artifact proof.

Rejected. Package hashes, Parquet/native Arrow paths, and golden fixtures make Arrow-major changes artifact-sensitive. Repinning remains available only behind evidence.

## Consequences

`.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md` is no longer blocked on user preference. Its next implementation step is to inspect the current registry/lockfile tuple and execute the smallest compatible path under this policy.

`.10x/tickets/2026-07-07-datafusion-tableprovider-adapter.md` must wait for tuple alignment work before execution.

This decision does not broaden `.10x/decisions/native-arrow-datafusion-parquet-policy.md` or its scoped `RUSTSEC-2024-0436` exception.
