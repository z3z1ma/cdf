Status: active
Created: 2026-07-07
Updated: 2026-07-17

# Runtime Conformance Throughput Rule

Runtime velocity is only acceptable when the conformance harness keeps pace.

A parent ticket that adds or materially changes a runtime path MUST NOT close until conformance owns coverage of that path or an active record explicitly excludes the path with rationale.

Runtime path changes include:

- new or changed resource execution paths;
- new or changed destination commit-session, replay, recovery, resume, or receipt-verification paths;
- new or changed checkpoint-gated orchestration paths;
- new or changed package lifecycle, golden-package, chaos, or run-ledger paths;
- new or changed streaming, CDC, distributed, or worker execution paths.

Coverage may be a conformance scenario, chaos scenario, golden fixture, property/fuzz target, or recorded exclusion, depending on the path. Ordinary unit tests are not enough when the changed path is part of the system spine.

Parent closure reviews MUST treat missing conformance ownership as a closure blocker, not a backlog nicety.

## Performance-affecting defaults

Performance and correctness are joint first priorities for runtime work. A change that can plausibly affect throughput, latency, memory, disk spill, network saturation, batching, concurrency, destination bulk paths, or hot-path allocation MUST NOT change the default behavior on intuition alone.

The acceptable paths are:

- measure the relevant benchmark, smoke, or live-envelope workload and show the hot path preserves or improves performance;
- make the behavior an explicit operator/configuration knob, leaving the measured fastest safe default in place;
- defer or cancel the ticket with a recorded no-action rationale when the only available default risks regression;
- apply the change without prior benchmark evidence only when it fixes a correctness bug that can corrupt data, violate package/checkpoint/receipt invariants, or make execution fail closed incorrectly, then record follow-up measurement.

Hidden hard caps, fixed throttles, reduced concurrency defaults, extra pre-scans, compatibility shims, and new buffering stages are presumed performance risks until measured otherwise. Adaptive controllers are acceptable only when their bounds, telemetry, and non-regression evidence are recorded; otherwise they remain opt-in experiments.
