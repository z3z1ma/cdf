Status: done
Created: 2026-07-10
Updated: 2026-07-25
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/done/2026-07-07-interop-boundary-overhead-triage.md

# P3 WS-H: measured interop boundaries

## Scope

Measure and document Python PyCapsule/C Data Interface, subprocess Arrow IPC framing, row-shaped fallback, and the prospective WASM stream cost model. Preserve the rule that foreign rows become Arrow batches at the boundary and do not enter the engine runtime model.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md`
- `.10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md`
- `.10x/tickets/done/2026-07-11-p3-h3-subprocess-stream-supervision.md`
- `.10x/tickets/cancelled/2026-07-11-p3-h4-wasm-cost-interface-model.md`
- `.10x/tickets/done/2026-07-11-p3-h5-interop-envelope-closeout.md`

## Acceptance criteria

- Python zero-copy is verified at batches of at least 1 MiB and its startup/per-batch costs are recorded.
- Subprocess Arrow IPC throughput and copy count are measured against the native path.
- Row-shaped compatibility costs are explicit rather than blended into Arrow-native claims.
- WASM's projected stream/sandbox cost model is recorded without pretending Tier 3 exists.
- Python and subprocess production boundaries are incremental, ledger-accounted, cancellable implementations of one neutral foreign-stream contract rather than eager private runtimes.

## Blockers

None. Implemented Python and subprocess modes are measured and closed; prospective WASM remains
parked outside P3 closure.

## References

- `.10x/decisions/neutral-foreign-stream-boundary.md`
- `.10x/research/2026-07-11-foreign-interop-boundary-audit.md`
- `.10x/specs/foreign-stream-interop.md`

## Journal

- 2026-07-19 — H2 closed after real GIL/free-threaded and PyArrow matrices, constant-memory/backpressure evidence, neutral `ForeignProducer` production integration, explicit runtime-resolved lane admission, and a final independent pass. H3/H4/H5 remain the active children; H5 owns the calibrated native-memory/copy/release envelope rather than H2 overstating it.
- 2026-07-18 — H3 closed after incremental Arrow IPC/NDJSON/Singer/Airbyte production paths, cancellation-aware bounded supervision, process-tree cleanup, explicit Linux child-memory fencing, protocol-state preservation, release envelopes, a full adversarial repair pass, and workspace-wide static/fast checks. H4 and H5 remain; H5 owns exact copy accounting, aggregate process RSS/cgroup evidence, and the residual control-event retention law.
- 2026-07-25 — Backlog grooming parked H4 with the unimplemented WASM runtime. H5 can close the measured interop envelope over the implemented Python and subprocess modes without manufacturing speculative measurements.
- 2026-07-25 — H5 closed the implemented envelope. Python and subprocess implement the same
  neutral foreign-event lifecycle; planned capabilities and EOF-bound actual transfer/copy/control
  evidence now survive the ordinary runtime and CLI. Exact owner-release and control-retention
  laws are permanent, mode-specific release observations are host-labelled, production does not
  infer zero-copy from Arrow compatibility, and WASM remains explicitly prospective.

## Evidence

- H1 measurement/copy-proof harness:
  `.10x/tickets/done/2026-07-11-p3-h1-interop-measurement-copy-proof.md`.
- Python incremental Arrow boundary:
  `.10x/tickets/done/2026-07-11-p3-h2-python-incremental-arrow-boundary.md`.
- Supervised subprocess boundary:
  `.10x/tickets/done/2026-07-11-p3-h3-subprocess-stream-supervision.md`.
- Implemented-mode closeout:
  `.10x/tickets/done/2026-07-11-p3-h5-interop-envelope-closeout.md` and
  `.10x/evidence/2026-07-25-p3-h5-interop-envelope.md`.
- Public operator/contributor guidance: `docs/interop-boundaries.md`.

## Review

The workstream closure audit found every implemented mode incremental, ledger-accounted,
cancellable, redacted, and expressed through one kernel-facing contract. Performance evidence
keeps Arrow C, Arrow IPC, and row compatibility separate. No concrete runtime type enters generic
engine orchestration, and actual telemetry remains outside package identity. Verdict: **pass**.

## Retrospective

The durable interop abstraction is a stream of ordinary physical Arrow outcomes plus typed
control/terminal facts—not a Python, process, or future sandbox runtime in the engine. Copy
classification is useful precisely because it permits honest unknowns. Prospective runtimes should
join this boundary only when executable; architectural diagrams and cost guesses are not runtime
evidence.
