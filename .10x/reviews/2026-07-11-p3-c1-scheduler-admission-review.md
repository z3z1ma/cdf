Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md, commits 5eb4c166 through the C1 closure commit
Verdict: pass

# P3 C1 scheduler admission adversarial review

## Target

Canonical partition authority, capability validation, effective-jobs resolution, fair hierarchical admission, cancellation, runtime observability, static executor ownership, and admission overhead.

## Findings

No critical or significant finding remains.

- The review found that removing an active permit before validating its public payload could let a malformed release return an error after permanently leaking scheduler capacity. Release now validates against active authority before mutation, and a permanent regression test proves the original permit remains releasable.
- The review rejected using a single-writer destination as a global jobs ceiling. Writer serialization is a distinct lane; upstream extraction/decode concurrency remains governed by source/CPU/memory and later bounded-channel backpressure. The report exposes both values.
- Effective host/memory values are command-time evidence only and cannot affect `EnginePlan` or package hashes. Canonical schedule authority remains identity-participating because it derives solely from source/scan plans.
- Round-robin queues are work-conserving around ineligible heads. Jobs, bytes, CPU slots, I/O permits, connections, quota authorities, and scope leases cannot oversubscribe their declared limits in the focused transition tests.
- Cancellation is deterministic, rejects subsequent enqueue/admit, and preserves accounting for already-active work until join/release. There is no detached cancellation path.
- Scheduler source contains no first-party source, destination, executor-library, or format identifiers. Static production gates reject private thread/runtime pools outside the injected host.

## Verdict

Pass. C1 establishes the neutral admission and observability contract without prematurely implementing production fan-out.

## Residual risk

Production task fan-out, canonical frontier/reorder buffering, live lease acquisition, retry/rate timers, and jobs-invariance packages are deliberately owned by C2/A5/C3/C4. The C1 benchmark excludes those future execution costs and makes no end-to-end throughput claim.
