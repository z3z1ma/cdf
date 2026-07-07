Status: active
Created: 2026-07-07
Updated: 2026-07-07

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
