Status: active
Created: 2026-07-11
Updated: 2026-07-16
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/done/2026-07-11-p0-bx1-kernel-stream-extent-artifacts.md, .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md, .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md

# P3 A7: stream policy compilation and explain

## Scope

Compile source capabilities and declarative/Rust authoring policy into complete kernel execution extents, epoch/frontier plans, operator watermark propagation, package/lock evidence, and command-correct plan/explain/deep-validation output.

## Acceptance criteria

- Unbounded plans missing any required policy fail before extraction with exact remediation.
- Source and operator capability claims determine legal safe frontiers/backpressure/watermark propagation without concrete-source branches in generic planning.
- Plan/lock/package evidence is canonical and secret-free.
- Mock source additions exercise the same compiler solely through registry descriptors/capabilities.

## Evidence expectations

Compiler matrices, invalid-combination/property tests, plan/golden fixtures, extension architecture gate, and explain/JSON rendering evidence.

## Explicit exclusions

No runtime epoch execution or resident lifecycle.

## Blockers

None. SX1, BX1, and A5 are done; this ticket is executable.

## Journal

- 2026-07-19: Activated immediately after WX1 closure on the A7 -> A8 -> C5 critical path. Implementation is confined to neutral kernel/runtime capability joins, declarative/project compilation, engine plan evidence, and CLI plan/deep-validation consumption; the concurrent object-access/Iceberg source lane remains out of scope.
- 2026-07-19: Commit `03b1b990` added a source-neutral `CompiledStreamPolicy`, explicit unbounded-source stream capabilities, typed declarative drain/watermark/frontier authoring, execution extents on every compiled physical graph node, watermark projection/operator validation, exact plan/explain/package and lockfile evidence, and deep-validation/plan rendering. Generic joins consume only source and destination capability artifacts; no source/driver id branch was added.
- 2026-07-19: The declarative schema advanced to `cdf-declarative-v4`. Source-frontier authoring covers every kernel `SourcePosition` family and freezes current versioned positions before planning. Bounded compiled-source and portable-worker identities remain stable because absent stream capabilities are omitted rather than hashed as a new null field.
- 2026-07-19: Focused verification passed: all 22 declarative tests; 101 runtime tests with one explicit benchmark ignored; the source-policy capability/termination matrix; exact engine source-binding, graph/package round-trip, drain preflight, coherent-tamper, and watermark-projection tests; lockfile canonical/tamper tests; and JSON/human deep-validation, plan, and explain tests. A broad engine run and broad project run exposed already-present unrelated failures; clean `ed9bb3de` reproduced the effective-schema fixture failure before A7, while the object-access lane owns the newly introduced project transport failures.

## References

- `.10x/specs/stream-epochs-watermarks.md`
- `.10x/specs/source-extension-runtime-contract.md`
