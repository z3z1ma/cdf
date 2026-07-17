Status: done
Created: 2026-07-16
Updated: 2026-07-17
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
- 2026-07-17: Current reproduction on this host no longer produced the original "all generated children fail" condition: `raw_arrow_ndjson`, `json_ndjson_to_package`, and `legacy_tiny_startup_e2e` observed successfully through `baseline-run`. The remaining failed generated legacy cases were genuine stale-worker errors, but `baseline-run` reported only exit code 2 because the host observer discarded child stderr. Direct `legacy-case-worker` invocation showed the actionable error (`executable engine plan requires compiled source and partition-schedule authority`). The fix keeps process isolation and `/usr/bin/time` observation, drains stderr concurrently so children cannot block on it, retains a bounded 64 KiB sanitized failure snippet, and surfaces it in `ObservationStatus::Failed`.

## Blockers

None.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks failed_child_retains_bounded_stderr_evidence --locked -j 12` — passed. Covers the portable child-process observer boundary and verifies failed children retain stderr with exit code.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks macos_time_wrapped_failed_child_retains_stderr_evidence --locked -j 12` — passed. Covers the macOS `/usr/bin/time -l -o` wrapper path and verifies stderr survives the timing wrapper.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-benchmarks isolated_macro_runner_retains_samples_and_derives_distribution --locked -j 12` — passed. Confirms the macro runner still retains samples and derives distributions under isolated execution.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-benchmarks --all-targets --locked -j 12 -- -D warnings` — passed.
- `CARGO_BUILD_JOBS=12 cargo build -p cdf-benchmarks --bin cdf-p3-lab --release --locked -j 12` — passed.
- Live smoke: `target/release/cdf-p3-lab baseline-run /tmp/cdf-l3r-after.nNb6ok/report dev deps toolchain 3` exited 0 with 12 observations. `raw_arrow_ndjson` and `json_ndjson_to_package` were `observed`; failed legacy cases now include sanitized stderr, for example `cdf-p3-lab: Data: executable engine plan requires compiled source and partition-schedule authority`.

## Review

Verdict: pass.

- The change preserves process isolation, warmup, median-of-N sampling, timeout behavior, stdout measurement limit, `/usr/bin/time` CPU/RSS observation, and cache-mode semantics.
- The host observer now drains stderr concurrently for both success and failure, avoiding deadlock on noisy children. Successful child measurements still parse stdout as the authority; failed children retain bounded diagnostic evidence only.
- Failure text is sanitized before entering benchmark reports, preserving the report validator's no-path/no-user/host-identity rule.
- Remaining failed legacy observations are not runner failures; their now-visible errors identify stale legacy workload paths that should be owned by the relevant benchmark compatibility ticket, not L3R.

## Retrospective

The misleading symptom was "baseline-run failed" when the runner was actually hiding child stderr. The fast way out was not to infer from exit codes but to run the generated child command directly and compare it with the macro runner's retained evidence. A performance lab that cannot explain failed children is worse than unavailable; it makes optimizer work chase ghosts. Future process-runner changes should treat stderr as bounded evidence, not noise, while keeping report sanitization strict.
