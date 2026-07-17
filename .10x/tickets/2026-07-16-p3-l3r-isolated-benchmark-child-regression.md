Status: open
Created: 2026-07-16
Updated: 2026-07-16
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l3-macro-roofline-runners.md

# P3 L3R: repair isolated benchmark child execution

## Scope

Repair the performance lab's isolated `baseline-run` execution on the current macOS host. The runner currently reports exit code 1 for every generated child, including the raw Arrow reference, while the exact generated raw and CDF workers succeed when invoked directly. Preserve process isolation and typed host/resource observation; do not mask the defect by falling back to in-process timing.

## Non-goals

- No codec, package, or destination optimization.
- No CI polling or broad CI stabilization.
- No weakening of worker timeout, RSS, CPU, cache-mode, or failure evidence.

## Acceptance Criteria

- A generated raw Arrow reference and a generated CDF case both complete through `cdf-p3-lab baseline-run` on the reproducing host.
- Failed children retain actionable command-safe stderr/status evidence in the report rather than only exit code 1.
- The root cause is covered at the host-capability/process-runner boundary, including a focused macOS regression test where the defect is platform-specific.
- Process isolation, median-of-N sampling, warmup, and raw-sample retention remain unchanged in meaning.
- Strict focused Clippy and the L3 runner tests pass.

## References

- `.10x/tickets/done/2026-07-10-p3-ws-l3-macro-roofline-runners.md`
- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/decisions/performance-lab-host-capability-boundary.md`

## Assumptions

- Record-backed: the generated fixtures and requests under `/tmp/cdf-b5-current` are valid because the exact generated worker requests completed when invoked directly.
- Record-backed: the failure belongs to runner/child orchestration rather than B5's codec because raw Arrow and unrelated generated cases fail identically.

## Journal

- 2026-07-16: B5 closure attempted the authoritative lab path after its codec-local gates. `cdf-p3-lab baseline-run` generated the ordinary fixture/request catalog but every isolated case terminated with exit code 1, including `raw-ndjson`; no report was produced. Running the exact generated raw and CDF workers directly five times each succeeded and produced valid timing payloads. This ticket owns the lab regression so B5 does not misclassify runner failure as codec throughput.

## Blockers

None.

## Evidence

Pending implementation.

## Review

Pending implementation.

## Retrospective

Pending implementation.
