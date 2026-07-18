Status: done
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p0-ix1-neutral-foreign-stream-contract.md

# P3 H1: interop measurement and copy-proof harness

## Scope

Implement common cold/warm startup, first-batch, steady-state, batch curve, CPU/allocation/copy, memory, wait, cancellation, and native-reference measurement for foreign transfer modes, including a falsifiable Arrow C zero-copy probe.

## Acceptance criteria

- Results separate Arrow C, IPC, and row modes and label unavailable/unknown cells.
- Zero-copy is asserted only when payload address/lifetime/allocation probes pass for that type path.
- Host/interpreter/protocol/build and timed/setup regions are recorded.
- Harness integrates with WS-L baselines/envelope without slowing fast checks.

## Evidence expectations

Raw machine reports, proof fixtures, calibration/repeatability, unsupported-type downgrade cases, and bias review.

## Explicit exclusions

No adapter optimization.

## Blockers

None. L5 and IX1 are done; this ticket is executable.

## References

- `.10x/specs/foreign-stream-interop.md`
- `.10x/specs/performance-lab-and-envelope.md`

## Journal

- 2026-07-18 — Activated after IX1 moved to done. Scope for this pass is the common benchmark/report/proof harness and tiny synthetic worker; concrete Python/subprocess optimization remains excluded.
- 2026-07-18 — Added `cdf-benchmarks::interop`: common interop fixture workload/report types, mode-separated Arrow C/IPC/row cells, falsifiable Arrow C zero-copy probe classification, per-mode startup/first-batch/steady-state/batch-curve/copy/allocation/wait/cancellation/native-reference fields, and an `InteropWorkerMeasurement` that remains parseable as the existing macro-runner `WorkerMeasurement`.
- 2026-07-18 — Added `cdf-p3-lab interop-fixture-worker REQUEST.json`, letting WS-L run interop cells as isolated workers while retaining the richer interop artifact in stdout JSON.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks interop --lib --locked -j 12` — passed. Covers Arrow C zero-copy proof requiring matching buffer identity, release-order proof, and zero allocation delta; known-copy downgrade; unknown-copy downgrade; report separation for Arrow C/IPC/row modes; cancellation cells; and `InteropWorkerMeasurement` parsing as the standard macro-runner `WorkerMeasurement`.
- `CARGO_BUILD_JOBS=12 cargo run -p cdf-benchmarks --bin cdf-p3-lab --locked -j 12 -- interop-fixture-worker target/cdf-benchmarks/interop-request.json` — passed with tiny all-mode workload, producing `rows=384`, `logical_bytes=9360`, and three interop mode reports. This validates the binary worker path without adding slow/live checks.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-benchmarks --lib --bins --locked -j 12 -- -D warnings` — passed.

## Review

Pass. The harness records unknown/unavailable cells rather than pretending real Python/subprocess/native references were measured. It does not alter production adapters or hot-path defaults. The copy-proof rule is falsifiable and conservative: Arrow C gets `PayloadZeroCopyVerified` only with matching payload buffer identity, verified release ordering, and zero allocation delta. CPU/RSS remain the macro runner's host-observer responsibility, which keeps fast unit tests small and delegates authoritative promotion to the EC2 lab policy.

## Retrospective

The useful trick was flattening `InteropWorkerMeasurement` over the existing `WorkerMeasurement`: existing macro cells can consume it without a new parser branch, while H2/H3/H4 can retain the rich interop report as artifact evidence. This keeps WS-L integration cheap and avoids a new benchmark framework.
