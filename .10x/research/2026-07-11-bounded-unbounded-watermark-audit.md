Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Bounded, unbounded, and watermark architecture audit

## Question

What must P3 implement for `VISION.md` 6.5–6.6 so the high-throughput bounded runtime is also the correct foundation for drain-mode CDC/queues and a later resident supervisor, without turning CDF into a windowing engine or leaking DataFusion/runtime details into artifacts?

## Sources and methods

Inspected `VISION.md` 6.1–6.6, 8.3, 10, 12, and 25.3; active runtime, resource, checkpoint, package, scheduler, memory, and source-extension specifications; the existing broad CDC/supervisor ticket; `cdf-kernel` batch/watermark types; and `cdf-engine` plan boundedness validation and consumers.

## Findings

The current `PlanBoundedness` lives in `cdf-engine`. `UnboundedLive` contains optional `checkpoint_cadence_ms`, optional `package_rotation_rows`, and an optional free-form watermark string. Planning rejects that variant wholesale; `UnboundedDrain` carries none of the three policies that VISION requires for every unbounded plan. Most product callers hard-code `Bounded`. This is a semantic ownership leak and cannot become the long-term artifact shape.

The kernel already owns `Batch`, `Watermark { name, position }`, typed source positions, checkpoint scopes including window/stream, and receipt-gated state transitions. These are the right lower-layer nouns, but watermark meaning/provenance, late-data action, cadence, rotation, drain termination, partition aggregation, idle behavior, and operator propagation are not yet typed.

Time-based triggers are control-plane observations, not deterministic data cut points. Closing immediately on a timer racing worker scheduling would make package contents depend on scheduling. A timer may request closure; the actual cut must be a deterministic admitted source-position/frontier barrier recorded in the package/checkpoint evidence. Replay consumes the resulting package and never re-times the source.

Watermarks are claims, not timestamps found in a batch. A usable claim needs event-time field/domain, position, provenance/authority, partition, monotonicity, and late-data policy. A global watermark is the minimum eligible partition watermark. Idle partition exclusion is safe only under an explicit source capability/timeout policy and must be evidence, because silently ignoring an idle partition can assert completeness falsely.

CDF does not need general windows/triggers/updates. It needs finite evidence-bearing epochs over an unbounded source: admit data until a closure request reaches a canonical safe frontier, finalize one package, settle destination, commit that epoch's typed position, then continue. This is the same package/receipt/gate calculus repeated. A later resident supervisor adds lifecycle and recurrence, not a second executor or artifact model.

Drain mode is the P3 execution cut. It requires explicit termination (`quiescent`, bounded duration, bounded records/bytes, or source frontier), all unbounded policies, bounded memory/spill, and one or more independently gated rotated packages. Resident pause/resume-from-head and log CDC source implementations remain in the later supervisor/CDC program, but their artifact/runtime seams are implemented and conformance-tested in P3.

## Conclusion

Move boundedness/window policy to versioned kernel artifacts, compile it through source/declarative/plan/package/lock surfaces, and execute drain mode as a sequence of deterministic frontier-closed epochs on the same fused graph. Treat watermarks as typed monotone claims with explicit aggregation, idleness, propagation, and late-data verdicts. Keep resident lifecycle out of P3 while proving a mock unbounded source can drain, rotate, crash/recover, and replay without a new semantic path.

## Limits

This audit does not select a Kafka/Postgres replication protocol, implement resident supervision, define general event-time aggregation windows, or promise identical package boundaries across two independently timed live captures. It requires jobs invariance for a fixed captured source interval and plan.
