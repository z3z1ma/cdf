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

## References

- `.10x/specs/stream-epochs-watermarks.md`
- `.10x/specs/source-extension-runtime-contract.md`
