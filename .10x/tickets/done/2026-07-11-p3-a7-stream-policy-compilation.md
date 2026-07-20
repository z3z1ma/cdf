Status: done
Created: 2026-07-11
Updated: 2026-07-19
Parent: .10x/tickets/done/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
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
- 2026-07-19: One adversarial review returned `fail` with one critical and five significant findings: enabled watermark authority was not proved against typed preserved columns; source-frontier values were not structurally/source-capability validated; drain graphs were not bound to exact source/policy authority and could be omitted; bounded artifact compatibility was unproved; partial drain declarations and inspect remediation were weak; and intrinsic source-hash validation was incomplete. The repair is intentionally one compiler-boundary batch rather than iterative local patches.
- 2026-07-19: The repair added versioned structural `SourcePosition` validation and source-declared comparable position families; exact source/derived watermark-capability joins; typed source/output event-domain, redaction, projection, and transform proofs; graph-level extent plus per-node hash references; exact drain graph bindings to compiled source and stream-policy hashes; mandatory drain graphs at execution/package serialization; bounded-only omission of new graph/lock/policy fields; actionable missing-field diagnostics; and stream capabilities in `cdf inspect resource`.
- 2026-07-19: Repair verification passed with `cargo check` across kernel/runtime/declarative/engine/project/CLI, 104 runtime unit tests (one benchmark ignored), all 22 declarative tests, strict `-D warnings` clippy across all targets of the six affected packages, exact engine watermark/source/graph/package/preflight tests, project lock canonical/tamper coverage, and CLI explain/inspect coverage. The committed 100-run bounded golden remains stale, but this is not an A7 regression: the current tree and a detached pre-A7 `ed9bb3de` worktree produced the exact same actual package hash `sha256:e9b2846c...d115` and the same three mismatched artifact hashes. No golden was regenerated or weakened.
- 2026-07-19: The focused re-review still found one critical and two significant gaps rather than passing them through: source watermark `preserve` did not itself name/prove the event-time field/domain/authority; source-frontier compatibility stopped at the position-family discriminant; and nested missing policy fields plus the plural inspect remediation remained weak. The final repair replaced family-only frontier claims with exact typed cursor-field/log/protocol capabilities (including recursive composite admission), bound source watermarks to an exact field/domain/authority and the compiled Arrow schema, centralized Arrow-domain matching in the kernel, validates decimal cursor strings, points remediation to `cdf inspect resource <id>`, and adds missing-field remediation for nested TOML/YAML declarations. Runtime now passes 105 tests with one benchmark ignored, declarative passes all 22 tests including nested omissions, the engine watermark/operator-graph suites pass, and strict all-target clippy remains clean.
- 2026-07-19: Final reviewer verdict is `pass` with no critical/significant finding; the only residual risk is that generic nested-field remediation recognizes serde's current missing-field text. A final self-audit also bound the optional stream-capability hash into `CompiledSourceCompilerBinding`, so a coherently rehashed execution artifact cannot substitute stream claims while retaining the compiler authority; bounded bindings omit the field and preserve their prior identity shape.

## References

- `.10x/specs/stream-epochs-watermarks.md`
- `.10x/specs/source-extension-runtime-contract.md`

## Evidence

- **Complete policy before extraction:** declarative test `stream_policy_compiles_before_plan_and_missing_policy_names_the_fix` covers all six required top-level fields plus nested watermark aggregation/idleness omissions; runtime `capability_matrix_fails_before_extraction_with_specific_remediation` covers illegal boundedness, quiescence, idleness, watermark, and recapture joins without opening a source.
- **Capability-governed frontiers/backpressure/watermarks:** runtime's six-test stream-policy matrix plus `source_watermark_capability_is_bound_to_the_compiled_arrow_schema` and `source_frontier_requires_declared_comparison_kind_and_valid_position` prove exact source schema/domain/authority and cursor-field/log/protocol admission. Engine tests prove final projection, redaction, transform, exact source/policy graph binding, and no driver-id dispatch.
- **Canonical secret-free evidence:** plan/explain, lock, graph, and package tests validate intrinsic hashes and exact joins. Bounded graph/lock/policy fields are omitted. The 100-run golden is independently stale: detached pre-A7 `ed9bb3de` and final A7 produced the same actual package and three artifact hashes, proving A7 did not alter the current bounded identity.
- **Extension law:** declarative `streamish` is registered solely through a mock driver descriptor/capabilities and compiles the same policy path. Strict all-target clippy passed for kernel/runtime/declarative/engine/project/CLI; runtime passed 105 tests with one benchmark ignored; declarative passed 22; focused engine/project/CLI gates passed.

## Review

The first adversarial review failed with one critical and five significant findings. The repair closed exact graph authority, bounded serialization, position structure, diagnostics, lock/hash validation, and extent duplication. A focused re-review then found the source watermark and frontier capability descriptions still too coarse plus incomplete nested remediation; those were corrected at the capability artifact boundary. Final independent verdict: **pass**, no critical or significant finding. Residual risk: generic nested-field remediation recognizes serde's current missing-field message format; typed top-level errors remain independent of that text.

## Retrospective

The costly mistake was treating `Preserve` and a position-family enum as sufficient authority. They describe behavior classes, not the exact field/domain/comparator a compiler can safely admit. The durable rule is that capability artifacts must name every semantic dimension used in a plan-time proof, and the compiler must join those claims to independently typed schema/source authority. A second lesson is that compatibility must be measured against the actual pre-ticket tree: the committed bounded golden was already stale, and comparing exact hashes in a detached pre-A7 worktree prevented both an incorrect regeneration and an unnecessary rollback. Finally, graph nodes should carry compact references to one graph-level policy artifact; duplicating full frontiers multiplies artifact size without adding authority.
