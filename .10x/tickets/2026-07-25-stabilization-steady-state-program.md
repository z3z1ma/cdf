Status: active
Created: 2026-07-25
Updated: 2026-07-25

# Stabilization steady-state program

## Scope

Bring the existing CDF implementation and its P1/P3 program graph to an honest terminal state
before the next two major feature programs begin. This parent owns prioritization, dependency
order, status/reference coherence, and aggregate closure. It is not an executable implementation
ticket.

Future capability ambitions are indexed by
`.10x/knowledge/active-backlog-and-future-roadmap.md`; they are excluded from this program until
the user deliberately activates them.

## Workstreams and sequence

### WS-A — Source lifecycle closure

1. `.10x/tickets/done/2026-07-20-source-resume-aware-negotiation.md` — done.
2. `.10x/tickets/done/2026-07-13-p0-sa4-dynamic-producer-admission.md` — done.
3. `.10x/tickets/done/2026-07-13-p0-sa5-fixed-schema-admission-conformance.md` — done.
4. `.10x/tickets/done/2026-07-13-p0-fixed-schema-discovery-stream-admission.md` — done.

Resume-aware negotiation lands first because it prevents expensive source planning before the
committed frontier is known. SA4 then applies the bootstrap barrier to currently registered
dynamic producers only; future Lua/WASM runtimes do not block it. SA5 and the parent close from
cross-archetype evidence.

### WS-B — Evidence-driven statistics pruning

1. `.10x/tickets/2026-07-12-p3-j1-evidence-statistics-pruning.md`

Complete the implemented neutral pruning adapter, its memory/soundness boundary, and a concrete
consumer path. Other DataFusion bridges remain parked.

### WS-C — Daily-driver CLI and release readiness

1. `.10x/tickets/2026-07-11-p1-cx2-compact-renderer-errors.md`
2. `.10x/tickets/2026-07-11-p1-cx3-live-progress-activity.md`
3. `.10x/tickets/2026-07-11-p1-cx4-cli-conformance-performance.md`
4. `.10x/tickets/2026-07-11-p1-ws9-cli-experience-excellence.md`
5. `.10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md`
6. `.10x/tickets/2026-07-11-p1-z1-product-program-closeout.md`
7. `.10x/tickets/2026-07-08-p1-product-experience-program.md`

CX1's implemented terminal/help foundation is retained; its unratified pager remainder is
cancelled rather than blocking the compact renderer. A hosted release remains a real P1 closure
criterion and is sequenced after the daily-driver surface stabilizes.

### WS-D — Scale and implemented-interop proof

1. `.10x/tickets/2026-07-11-p3-f3-stress-generators-laws.md`
2. `.10x/tickets/2026-07-11-p3-f4-one-tb-memory-closeout.md`
3. `.10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md`
4. `.10x/tickets/2026-07-11-p3-h5-interop-envelope-closeout.md`
5. `.10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md`

This lane proves the already-built architecture. Any implementation defect discovered by a
stress or envelope run receives a separate bounded repair owner; proof tickets do not absorb
open-ended optimization rabbit holes. WASM cost modeling is not a P3 dependency.

### WS-E — P3 aggregate closure

1. `.10x/tickets/2026-07-11-p3-z1-envelope-evidence-reconciliation.md`
2. `.10x/tickets/2026-07-11-p3-z2-scale-demo-adversarial-review.md`
3. `.10x/tickets/2026-07-11-p3-z3-program-closure-retrospective.md`
4. `.10x/tickets/2026-07-10-p3-terabyte-scale-program.md`

P3 closes against its core performance architecture, implemented interop modes, fixed-schema
admission, J1 pruning, and scale evidence. Enterprise codec breadth, broad DataFusion adoption,
WASM, distributed execution, CDC, and new connectors are not P3 closure dependencies.

## Acceptance criteria

- Every active executable child is terminal with evidence and review.
- No active ticket exists solely as a reminder for an unprioritized future capability.
- P1 and P3 parent criteria either map to evidence or carry an explicit user-ratified scope
  removal preserved in terminal history and the roadmap.
- Parent/dependency/reference paths resolve and status relationships are coherent.
- P1 and P3 can be selected in the sequence above without hidden cross-program blockers.
- The active backlog reaches zero before the next feature program begins, except for a newly
  authorized feature owner deliberately opened by the user.

## Non-goals

- No implementation under this parent.
- No weakening of correctness, performance, evidence, or package identity.
- No activation of parked features merely to make a terminal count look complete.

## References

- `.10x/knowledge/active-backlog-and-future-roadmap.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/specs/cli-interaction-excellence.md`

## Journal

- 2026-07-25: Opened after a complete 45-ticket active-backlog inventory. The user ratified
  removing speculative capabilities from the executable backlog, removing enterprise codecs,
  broad DataFusion bridges other than J1, and WASM from P3 closure, and parking Athena while
  preserving its source research. Stale umbrella tickets and completed observability/conformance
  parents move terminal; future ambitions move to the roadmap rather than remaining open
  implementation tickets.
- 2026-07-25: WS-A SA4 closed with the neutral foreign-producer schema-acquisition capability,
  single-invocation Python bootstrap retention, configured-reference pin/hydration repair, and
  491-test CLI/project integration evidence. SA5 is now the only source-lifecycle child.
- 2026-07-25: WS-A is closed. SA5's assertion audit and complete source/conformance suite proved
  the fixed-schema lifecycle across inventory, discovery, retained payloads, jobs, preview/run,
  retry/replay, residuals, and quarantine; the fixed-schema parent and all six children are done.

## Blockers

None at the aggregate level. Child tickets own technical blockers and evidence.
