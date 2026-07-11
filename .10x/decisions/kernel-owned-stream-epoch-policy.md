Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Kernel-owned stream epoch policy

## Context

P3 must complete VISION 6.5–6.6 while preserving the kernel/engine boundary and package calculus. The current engine-local boundedness enum cannot express the required semantics safely and would make runtime timing an artifact authority.

## Decision

The kernel owns a versioned `ExecutionExtent` artifact:

- `Bounded`;
- `Drain(StreamEpochPolicy, DrainTermination)`; and
- a reserved `Resident(StreamEpochPolicy)` variant that is plan-invalid until the later supervisor is enabled.

`StreamEpochPolicy` contains typed checkpoint cadence, package rotation, watermark strategy, late-data action, partition aggregation/idleness policy, and safe-frontier requirements. Required fields are non-optional for unbounded extents. Engine/runtime types adapt this artifact but cannot redefine it.

An unbounded execution is an ordered sequence of finite **epochs**. Each epoch closes only at a canonical admitted source-position frontier, creates one independently verified package, obtains/verifies its destination receipt, and commits exactly that epoch's typed checkpoint scope. Failure leaves later positions uncommitted. Rotation does not create a bypass around the package or commit gate.

Cadence and rotation may request closure using batches, rows, bytes, elapsed time, or watermark advance. Time is never the cut identity. The scheduler captures the next legal canonical frontier after the request and records the trigger observation, frontier, overshoot, and policy version. For fixed plan and captured input interval, `--jobs` cannot change the frontier/package sequence.

Watermarks are typed monotone completeness claims with event-time domain/field, position, source or derived provenance, partition scope, and policy version. Global aggregation uses the minimum eligible partition claim. Partition idleness/exclusion must be explicit, capability-backed, and evidenced. Operators declare preserve/transform/drop behavior; an operator may not silently retain an invalidated watermark.

Late data is a total verdict-bearing action selected at plan time: `recapture_next_epoch`, `quarantine`, or `admit_with_annotation`. It never silently disappears and does not mutate a finalized package. Cursor lag/window-close remains processing-completeness policy and must not be mislabeled event-time completeness.

P3 implements bounded and drain execution plus reserved resident artifacts/conformance seams. The later resident supervisor adds continuous lifecycle over the same epoch executor. General event-time windows, triggers, retractions, and incremental aggregates remain out of scope.

## Alternatives considered

- Keep boundedness in `cdf-engine`: rejected because engine swaps would reinterpret artifact/state semantics.
- Make time directly determine package contents: rejected because scheduling and host load would change identity.
- Treat any batch max timestamp as a watermark: rejected because observation is not a source completeness claim.
- Implement a full Flink-style window engine now: rejected because CDF needs evidence epochs and late-data discipline, not arbitrary triggers/retractions.
- Defer all unbounded work to the resident supervisor: rejected because it leaves P3 runtime/artifact APIs bounded-only and guarantees later rework.

## Consequences

BX1 replaces the engine-local enum with kernel artifacts and migration fixtures. A7 compiles policies. A8 executes deterministic drain epochs. A9 supplies watermark/chaos/jobs conformance. The old CDC/supervisor parent remains the owner for concrete CDC sources, `cdc_apply`, and resident lifecycle, consuming rather than replacing these P3 foundations.
