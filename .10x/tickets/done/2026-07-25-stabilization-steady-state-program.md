Status: done
Created: 2026-07-25
Updated: 2026-07-26

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

1. `.10x/tickets/done/2026-07-12-p3-j1-evidence-statistics-pruning.md` — done.

Complete the implemented neutral pruning adapter, its memory/soundness boundary, and a concrete
consumer path. Other DataFusion bridges remain parked.

### WS-C — Daily-driver CLI and release readiness

1. `.10x/tickets/done/2026-07-11-p1-cx2-compact-renderer-errors.md` — done.
2. `.10x/tickets/done/2026-07-11-p1-cx3-live-progress-activity.md` — done.
3. `.10x/tickets/done/2026-07-11-p1-cx4-cli-conformance-performance.md` — done.
4. `.10x/tickets/done/2026-07-11-p1-ws9-cli-experience-excellence.md` — done.
5. `.10x/tickets/done/2026-07-08-p1-product-ws8-release-engineering.md`
6. `.10x/tickets/done/2026-07-11-p1-z1-product-program-closeout.md`
7. `.10x/tickets/done/2026-07-08-p1-product-experience-program.md`

CX1's implemented terminal/help foundation is retained; its unratified pager remainder is
cancelled rather than blocking the compact renderer. A hosted release remains a real P1 closure
criterion and is sequenced after the daily-driver surface stabilizes.

### WS-D — Scale and implemented-interop proof

1. `.10x/tickets/done/2026-07-25-p0-canonical-segment-memory-admission.md`
2. `.10x/tickets/done/2026-07-25-p0-staged-writer-memory-headroom.md`
3. `.10x/tickets/done/2026-07-11-p3-f3-stress-generators-laws.md`
4. `.10x/tickets/done/2026-07-11-p3-f4-one-tb-memory-closeout.md` — done.
5. `.10x/tickets/done/2026-07-10-p3-ws-f-constant-memory-guarantee.md` — done.
6. `.10x/tickets/done/2026-07-11-p3-h5-interop-envelope-closeout.md` — done.
7. `.10x/tickets/done/2026-07-10-p3-ws-h-interop-boundaries.md` — done.

This lane proves the already-built architecture. Any implementation defect discovered by a
stress or envelope run receives a separate bounded repair owner; proof tickets do not absorb
open-ended optimization rabbit holes. WASM cost modeling is not a P3 dependency.

### WS-E — P3 aggregate closure

1. `.10x/tickets/done/2026-07-11-p3-z1-envelope-evidence-reconciliation.md`
2. `.10x/tickets/done/2026-07-11-p3-z2-scale-demo-adversarial-review.md`
3. `.10x/tickets/done/2026-07-11-p3-z3-program-closure-retrospective.md`
4. `.10x/tickets/done/2026-07-10-p3-terabyte-scale-program.md`

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
- `.10x/decisions/cdf-system-authority-steady-state.md`

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
- 2026-07-25: WS-B is closed. J1 now streams manifest-aligned pruning decisions from a completely
  verified statistics profile under one caller-sized shared-memory reservation, exposes payload
  capabilities only for retained segments, records skipped/retained rows and bytes, and retains
  every segment conservatively when evidence is absent or unsupported. The current package/engine
  integration surface passed 280 tests and strict all-target Clippy.
- 2026-07-25: WS-C's holistic implementation tranche is complete. CX2/CX3 closed after a
  renderer-wide information-architecture rewrite, bounded nonblocking live progress, 320 passing
  CLI/core tests, strict Clippy, 19.26 million governed events/second, and a real five-partition
  public HTTPS-to-DuckDB smoke. CX4 now owns the hosted overhead and recording closure only.
- 2026-07-25: WS-C's daily-driver CLI lane is closed. Hosted FineWeb evidence measured progress
  enabled within ordinary variance of disabled (`-0.4407%` median delta), exact default execution
  completed after a generic staged-singleton repair, and 100×40 canonical recordings cover normal,
  verbose, redirected, JSON, replay, and failure surfaces. WS8 release engineering is next.
- 2026-07-25: WS-D crossed its final P0 memory boundary. Generic topology admission now joins
  source, canonical construction, encoding, staged handoff, and destination floors; mimalloc at the
  executable boundary removes glibc arena retention without native-library interposition. The
  untuned exact 100 GiB / 2 GiB EC2 law passed in 263.493 seconds at 1.658 GiB peak RSS with
  verified package, receipt, and checkpoint semantics. F3 now owns only its independent
  geometric, spill, and failure-mode acceptance matrix.
- 2026-07-25: F3 is closed. The same clean optimized binary completed 5, 20, and 100 GiB
  product-shaped runs under an enforced 2 GiB cgroup with flat RSS, and a repeated 5 GiB run
  showed no allocator/handle drift. Typed impossible-budget failure plus focused spill,
  spill-exhaustion, backpressure, compression, metadata, dedup, foreign-child, and staged-writer
  laws complete the matrix. F4 now owns only the 1 TiB scale run, device saturation, generated
  owner matrix, and permanent slow-tier gate.
- 2026-07-25: WS-F is closed. The exact dedicated-host default-policy cell completed 1.0086 TiB
  and 5.436 billion rows at 1.896 GiB peak process RSS under CDF's unoverridden 4 GiB policy,
  with verified package, receipt, checkpoint, and zero cgroup OOM. A separate all-file Parquet
  discovery pass closed the 1,024-file evidence boundary, the allocation-owner matrix has no open
  row, and dedicated-host procedures are permanent without inflating hosted CI.
- 2026-07-25: WS-D is closed. H5 promoted source-neutral planned and actual foreign-boundary
  evidence through the product path, proved exact Arrow ownership and bounded control retention,
  published honest Python/subprocess release cells, and left prospective WASM outside the
  implemented envelope. WS-H is terminal; P3 aggregate reconciliation is next.
- 2026-07-25: P3 Z1 is closed. The final generated envelope reconciles every target across its
  actual host/mode/reference, preserves the immutable pre-optimization baseline, retains partial
  and unproven ambitions visibly non-green, and rejects dead evidence sources or unaccepted
  residuals. Z2 now owns aggregate demo/adversarial synthesis without rerunning the 1 TiB cell.
- 2026-07-25: WS-E and P3 are closed. Z2 joined the exact full-year TLC and 1.0086 TiB
  demonstrations with a ten-category adversarial review; Z3 audited all 24 direct descendants,
  runtime/extension topology, references, coverage, and terminal limits. The generated envelope
  retains every non-green cell. No benchmark host was reprovisioned; the EC2 tranche remains
  terminated. WS8 is now the sole executable program workstream.
- 2026-07-26: WS8 is closed. Hosted run `30196650532` built and verified all five static-DuckDB
  targets and published `v0.2.0-alpha.1`; the public installer then passed against the published
  aarch64 macOS archive. No EC2 host was reprovisioned, and a read-only FQ12 inventory found no
  running CDF-tagged instance. P1 Z1 is now the sole active child before aggregate stabilization
  closure.
- 2026-07-26: P1 Z1 is closed. The aggregate matrix joins all WS1-WS9 evidence, the hosted
  prerelease, direct renderer/registry topology inspection, and a newly executed Chapter 23
  crash/resume/replay/drift session. The P1 parent is ready to move terminal.
- 2026-07-26: P1 is closed. All nine workstreams and Z1 are terminal; the coverage matrix records
  P1 and Chapter 23 done. Stabilization is now the only active ticket.
- 2026-07-26: Aggregate closure found and repaired one stale live recovery link and coverage claim
  for the removed pre-production `state migrate` command. The current recovery surface is strict
  schema-v1 state plus receipt-gated package recovery, with a migration-ready seam but no upgrade
  product before a real predecessor exists.
- 2026-07-26: Stabilization is closed. Terminal-status and repository-wide reference audits pass;
  P1/P3 and every bounded child are terminal; the hosted five-target prerelease remains public;
  the FQ12 inventory contains no CDF benchmark host; and future capability rows are parked behind
  an explicit activation rule rather than open reminder tickets.

## Blockers

None.

## Evidence

| Acceptance criterion | Evidence |
|---|---|
| Every active executable child is terminal | All workstream paths above are terminal; `.10x/evidence/2026-07-26-stabilization-steady-state-closure.md` records the zero-child and terminal-status audits. |
| No reminder-only active ticket remains | The final active-ticket inventory is empty; `.10x/knowledge/active-backlog-and-future-roadmap.md` owns inactive capabilities. |
| P1 and P3 are honestly reconciled | `.10x/evidence/2026-07-26-p1-z1-program-closure.md` and `.10x/evidence/2026-07-25-p3-z3-program-closure.md`. |
| Parent, dependency, reference, and status coherence | Repository-wide reference and terminal-directory status audits in the aggregate closure evidence. |
| No hidden P1/P3 sequencing blocker | Both program parents and all bounded descendants are terminal; their aggregate reviews pass. |
| Active backlog reaches zero | Final `.10x/tickets/` depth-one inventory contains zero Markdown tickets. |

## Review

The final fresh-hat review traced the terminal graph, active authority, roadmap, coverage matrix,
operator recovery docs, hosted release, current Chapter 23 session, and benchmark-host inventory.
It specifically challenged closure through stale current paths, reminder-ticket laundering,
unsupported completion claims, removed-command docs, and cloud-resource leakage.

One significant record-coherence finding was repaired before closure: current documentation still
claimed a `state migrate` command that the pre-production compatibility cleanup intentionally
deleted, and several coverage rows described already-closed work as active. One minor
temporal-authority finding was also repaired by superseding the decision that called the
now-terminal stabilization graph current. No critical or significant finding remains. Verdict:
pass.

## Retrospective

The backlog became trustworthy only after separating executable ownership from long-horizon
vision. Open reminder tickets hid the true critical path, while monolithic proof tickets invited
optimization rabbit holes. The successful pattern was a bounded owner, measured evidence, an
explicit keep/kill decision, terminal history, and a roadmap activation trigger for work that is
real but not current.

Aggregate closure also showed why reference and live-documentation audits belong at the program
boundary. Tests cannot detect a removed command still promised by operator docs, or an active
decision whose temporal statement expired when its program closed. Future program closeouts
should always audit executable status, record paths, live docs, external cost-bearing resources,
and the distinction between implemented coverage and parked ambition.
