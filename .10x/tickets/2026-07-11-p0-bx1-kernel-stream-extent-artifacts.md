Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/specs/stream-epochs-watermarks.md

# P0 BX1: kernel-owned execution extent and watermark artifacts

## Scope

Replace engine-local `PlanBoundedness` and free-form/optional live fields with versioned kernel `ExecutionExtent`, stream epoch policy, cadence, rotation, watermark, aggregation/idleness, late-data, drain termination, and frontier artifact types; adapt engine/project/CLI/conformance consumers and add compatibility/migration fixtures.

## Acceptance criteria

- Kernel artifacts contain no DataFusion, Tokio, source-driver, or CLI types.
- Every unbounded policy is structurally complete; invalid combinations cannot be represented or fail at deserialization/compile with precise errors.
- Existing bounded golden artifacts remain stable or follow an explicit artifact migration decision.
- Dependency/architecture tests prevent reintroduction of engine-owned stream semantics.

## Evidence expectations

Artifact JSON fixtures, migration/rejection tests, dependency graph output, all consumer compile/tests, and adversarial serialization review.

## Explicit exclusions

No epoch executor, concrete stream source, resident supervisor, or CLI UX.

## Blockers

None after the active spec.

## References

- `.10x/decisions/kernel-owned-stream-epoch-policy.md`
- `.10x/research/2026-07-11-bounded-unbounded-watermark-audit.md`
- `.10x/specs/stream-epochs-watermarks.md`
