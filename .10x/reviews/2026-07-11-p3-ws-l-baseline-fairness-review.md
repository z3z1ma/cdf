Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md
Verdict: pass

# P3 WS-L baseline fairness and architecture review

## Target

The complete WS-L lab, immutable pre-optimization baseline, generated envelope, and stop-line release.

## Assumptions tested

- A missing enterprise fixture was not allowed to become an omitted or passing row.
- Raw reference and CDF ratios were computed only for matching rows and physical input bytes.
- Logical Arrow allocation differences remained visible without invalidating a physically comparable decode workload.
- Tiny compatibility cases did not acquire large-file or steady-state claims.
- Baseline installation could not silently overwrite history or proceed without an evidence record.
- Profiling failure did not produce a fabricated flamegraph.
- No runtime, decoder, destination, hashing, parallelism, or memory optimization entered the before-picture changes.

## Findings

### Significant, explicitly owned: the baseline cannot yet exercise the workloads that matter most

Full-year TLC Parquet, TPC-H CSV, Postgres, the 100 GiB stressor, and the vector validation kernel are unavailable or failed. Consequently the baseline does not measure multicore scaling, remote overlap, steady-state columnar throughput, binary COPY, or constant memory. The generated envelope renders every one of these gaps as failed/unavailable, and the P3 workstream graph already owns each implementation and closeout. This is acceptable for the pre-optimization stop-line because hiding or synthesizing those observations would be materially less honest.

### Significant, explicitly owned: observed destination figures have tiny-fixture setup bias

DuckDB and Parquet numbers include compatibility-path fixture/setup work. They are useful as regression anchors for the exact current path, not as destination roofline measurements. D2/D4/D5 own prepared large-fixture replacements and must not use these values to claim target attainment.

### Minor: phase telemetry does not explain all process wall time

The NDJSON phase sum is smaller than process wall time. Uninstrumented orchestration/setup remains visible as a gap; A1-A5 and L2 telemetry consumers own decomposition as the pipeline is replaced. No overhead attribution is inferred from the missing interval.

### Minor: profile evidence is platform-limited

The macOS sample call tree identifies durability work in the captured tiny workload. A flamegraph was unavailable without full Xcode. E1-E4 must repeat profiles on the lab's Linux/perf host before generalizing cost percentages.

## Verdict

Pass. The baseline is deliberately embarrassing, immutable, host-labeled, bias-labeled, and complete in outcome coverage. Its missing enterprise measurements are visible failures with existing implementation owners, not weakened acceptance criteria. The before picture predates WS-A through WS-H implementation and safely releases the P3 optimization stop-line.

## Residual risk

The strongest risk is optimizing fixed costs exposed by tiny fixtures while missing large-file bottlenecks. The active P3 target-specific children and final adversarial workload review own that risk. Every future green claim still requires same-host large-workload evidence; this review authorizes workstream activation, not target attainment.
