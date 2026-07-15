Status: done
Created: 2026-07-11
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/specs/stream-epochs-watermarks.md

# P0 BX1: kernel-owned execution extent and watermark artifacts

## Scope

Replace engine-local `PlanBoundedness` and free-form/optional live fields with versioned kernel `ExecutionExtent`, stream epoch policy, cadence, rotation, watermark, aggregation/idleness, late-data, drain termination, and frontier artifact types; adapt engine/project/CLI/conformance consumers and add current-format serialization/rejection fixtures. The active pre-production format decision supersedes compatibility readers and migration fixtures: CDF writes and reads one current v1 artifact shape.

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
- `.10x/decisions/pre-production-current-format-only.md`

## Journal

- 2026-07-14: Activated after the prerequisite spec and ownership decision were confirmed. The existing engine enum admitted an empty `UnboundedDrain` and an optional/free-form `UnboundedLive`; every caller outside the engine hard-coded `Bounded`. The current-format-only decision supersedes this ticket's original compatibility/migration-fixture wording, so the implementation deletes the old shape and rejects missing or unsupported artifact versions rather than retaining a reader shim.
- 2026-07-14: Added versioned kernel execution extent, complete epoch policy, typed closure/termination, typed watermark claim/domain/authority/aggregation/late-data/operator behavior, canonical frontier, and separate nonidentity closure evidence. Replaced the engine-owned enum throughout engine/project/CLI/conformance/benchmarks. Planner, preview, and package execution all validate the extent; resident policy is recordable but rejected before source contact until the supervisor exists.
- 2026-07-14: Focused verification passed for all seven kernel artifact laws and all 45 kernel library tests. A broad engine library run had 125 passes, six ignored tests, and two pre-existing failures: the runtime-ownership static gate still detects active REST/subprocess `futures_executor::block_on`, and the standalone package rechunking identity test remains an existing unrelated failure. BX1-focused planner, execution-preflight, explain, missing-field, and architecture tests passed.
- 2026-07-14: The first adversarial review returned one critical, four significant, and two minor findings in one pass: drain could silently run through the bounded executor; nested versioned artifacts trusted invalid deserialization; watermark claims omitted policy version; elapsed/watermark closure observations were incomplete; top-level and explain extents could diverge; identity-channel enforcement and ownership gates were overstated/narrow. The complete repair added a bounded-only execution preflight, validated serde for every nested versioned artifact, policy identity, dimension-checked closure observations, pre-mutation extent-coherence validation, explicit A7 nonidentity ownership, and workspace-wide engine source scanning. Focused re-review passed with no critical or significant finding.

## Evidence

- `.10x/evidence/2026-07-14-p0-bx1-kernel-stream-extent-artifacts.md` maps the artifact, dependency, affected-graph, strict-lint, serialization, and execution-preflight observations to the acceptance criteria.
- Current-format bounded JSON is exactly `{"kind":"bounded","version":1}`; missing and unsupported versions fail. Complete drain JSON round-trips, while incomplete/invalid policy and nested versioned artifacts fail at deserialization or plan validation.
- Exact engine laws prove drain/resident and divergent recorded authority fail before source contact/package creation. The source-ownership gate scans every current engine Rust module and the kernel manifest gate excludes runtime/engine/CLI/source dependencies.

## Review

Initial verdict: `fail`. The reviewer found one critical, four significant, and two minor issues, all listed in the final journal entry. Re-review verdict after the complete repair: `pass`; no critical or significant finding remains. Residual risk is explicit and owned: A7 must route closure timing through a nonidentity channel, and A7/A8 must enforce capability-aware frontier scope, ordering, and monotonicity. The current architecture gate scans all current flat engine source files and must become recursive if the crate gains nested Rust source directories.

## Retrospective

Moving semantics into the kernel was not sufficient by itself: the executor had to fail closed until the semantic consumer exists, every nested versioned artifact needed validation at its actual serde trust boundary, and duplicated evidence views needed an equality invariant. The highest-value review technique was tracing each new enum from construction through deserialization and the first mutation boundary rather than reviewing the type definitions in isolation. Keeping frontier identity and trigger/control observations as separate types creates the right seam, but package integration—not comments—must enforce which one hashes; A7 now owns that explicit proof.
