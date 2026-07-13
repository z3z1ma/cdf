Status: done
Created: 2026-07-12
Updated: 2026-07-12

# C2 open-and-poll frontier and retry architecture

## Question

What is the smallest scheduler-owned architecture that keeps multiple partition opens and batch streams live, releases only canonical outcomes, remains bounded behind a stalled head, preserves the exact jobs=1 limit/position/package semantics, and has an honest cancellation boundary? Which retry semantics remain unratified before C2 can execute retries?

## Sources and methods

Local authority and evidence inspected:

- `.10x/specs/deterministic-parallel-scheduler.md`
- `.10x/decisions/canonical-frontier-parallel-scheduling.md`
- `.10x/decisions/scheduler-source-boundary-readiness.md`
- `.10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md`
- `.10x/evidence/2026-07-11-p3-c1-scheduler-core.md`
- `.10x/evidence/2026-07-11-p3-c1-engine-plan-schedule.md`
- `.10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md`
- `.10x/evidence/2026-07-11-p3-a3-canonical-byte-boundary-correction.md`
- `.10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md`
- `.10x/evidence/2026-07-11-p3-a5-streaming-graph-closeout.md`
- `.10x/evidence/2026-07-11-p3-a5e-parallel-segment-frontier.md`
- `.10x/evidence/2026-07-11-p3-c2-canonical-open-frontier.md`
- `.10x/reviews/2026-07-11-p3-c2-canonical-open-frontier-review.md`
- `.10x/tickets/2026-07-11-p3-c2-parallel-frontier-execution.md`

Local source inspected included `cdf-runtime` scheduler/source/host/graph contracts, `cdf-kernel` batch/resource/stream types, `cdf-engine` partition execution/segmentation/standalone host, first-party source capability declarations, generation checks, accounted format batches, and the existing HTTP retry implementation.

Primary upstream comparison used the exact locally pinned versions and upstream source/docs:

- Apache DataFusion 54 at pinned revision `7ff7278edc1bf7446303bff51e5883a38414bbdf`: [`ExecutionPlan` streaming and cancellation contract](https://github.com/apache/datafusion/blob/7ff7278edc1bf7446303bff51e5883a38414bbdf/datafusion/physical-plan/src/execution_plan.rs), [`ReceiverStreamBuilder`](https://github.com/apache/datafusion/blob/7ff7278edc1bf7446303bff51e5883a38414bbdf/datafusion/physical-plan/src/stream.rs), and [`SpawnedTask`](https://github.com/apache/datafusion/blob/7ff7278edc1bf7446303bff51e5883a38414bbdf/datafusion/common-runtime/src/common.rs).
- Futures 0.3.32: [`FuturesOrdered`](https://docs.rs/futures-util/0.3.32/futures_util/stream/struct.FuturesOrdered.html), [`FuturesUnordered`](https://docs.rs/futures-util/0.3.32/futures_util/stream/struct.FuturesUnordered.html), and [`Buffered`](https://docs.rs/futures-util/0.3.32/futures_util/stream/struct.Buffered.html).
- Tokio 1.52.3: [`JoinSet`](https://docs.rs/tokio/1.52.3/tokio/task/struct.JoinSet.html), [task cancellation](https://docs.rs/tokio/1.52.3/tokio/task/index.html), and [bounded `mpsc`](https://docs.rs/tokio/1.52.3/tokio/sync/mpsc/index.html).

No builds or tests were run. The work was read-only except for this research record and the requested C2 shaping note.

## Findings

### Existing local boundary

The engine currently fills a `FuturesOrdered` with partition-open futures, awaits its canonical head, and then stops polling that collection while it drains the returned head stream. This overlaps opens only while the collection itself is polled. It cannot guarantee that later open futures establish their producer streams, and it never polls later batches concurrently with the head. The stalled-first prototype recorded in C2 correctly falsified the stronger concurrency claim.

`FuturesOrdered` is behaving as documented: it races contained futures and retains later completed outputs, but only while its own `poll_next` is called; it returns no later output before the earliest submitted future. A whole-wave open barrier would add a dependency on every later open and therefore worsen head-of-line behavior.

C1 already owns effective-jobs and admission policy. A3 already owns deterministic partition-local segment assembly and conservative position joins. A5 already owns accounted graph edges, durable segment publication, canonical segment registration, and structured encode-task cleanup. C2 should not duplicate any of those policies or replace the canonical writer.

`Batch` can carry payload retention, and native format paths produce accounted physical batches. The scheduler capability declares minimum/maximum poll and decode working sets. However, not every compatibility source batch is yet guaranteed to carry a payload retention lease, so a frontier cannot claim byte-boundedness merely from item count; it must reject over-declaration, retain existing leases, and have an explicit adoption/migration rule for unaccounted outcomes.

The present `BatchStream` is an opaque boxed `Stream`. Some first-party streams contain injected-host task scopes. Natural end-of-stream polls and joins those scopes, but early `Drop` only cancels/aborts; it does not expose an awaitable join to the engine. Therefore dropping a `FuturesUnordered`, bounded channel, or stream is not by itself evidence for C2's required cancel-and-join law.

### Upstream patterns and applicability

DataFusion's fundamental execution boundary is pull-based: each partition returns a stream and work advances when streams are continually polled. Its partitioned executor constructs one stream per partition; its collecting helper uses completion-order tasks and sorts by partition afterward. That collect-and-sort pattern is valid for bounded query results but is unsuitable here because CDF must release a canonical prefix incrementally and remain bounded behind a stalled head.

DataFusion's `ReceiverStreamBuilder` demonstrates the complete producer-channel pattern: a bounded channel supplies item backpressure, the returned stream owns the producer `JoinSet`, producer errors/panics are surfaced, and dropping the receiver aborts the owned tasks. This is useful precedent for lifecycle ownership. It does not solve CDF's byte accounting, canonical partition frontier, exact limit authority, or awaitable shutdown of blocking work. DataFusion itself warns that spawned work must be tied to the returned stream and cancelled when the stream drops.

`FuturesUnordered` is the closest primitive for the scheduler core: it polls only futures that wake and yields completion order, allowing the scheduler to record readiness separately from release order. `FuturesOrdered`/`Buffered` combine execution and FIFO release, which hides the point where CDF must enforce byte/item/ordinal budgets, retry attempt authority, and canonical acknowledgements. Neither collection is bounded by itself.

Tokio bounded `mpsc` provides item backpressure, but capacity does not account payload bytes and one channel per producer permits up to one buffered payload per admitted producer plus in-flight allocations. `JoinSet` gives completion-order ownership and abort-on-drop; `shutdown` aborts and awaits async tasks. Tokio explicitly notes that aborting is cooperative and running `spawn_blocking` work cannot be aborted. CDF must therefore continue to use injected cancellation checks and join declared blocking lanes rather than equating `abort_all` with completion.

### Retry authority already present but insufficient

`SourceExecutionCapabilities` declares granularity, typed retryable errors, idempotence, reopenability, and attestation strength. It does not declare an attempt budget, elapsed deadline, delay policy, jitter authority, override precedence, or duplicate-attempt state machine.

`cdf-http` has a separate default of three retry decisions, 30,000 milliseconds of accumulated delay budget, 100-millisecond exponential base, 5,000-millisecond cap, and deterministic arithmetic jitter. The REST loop currently ignores the returned delay and immediately retries. That behavior is driver-local, counts retry decisions rather than total attempts, excludes attempt execution time from its budget, and is not scheduler authority. It is evidence of prior candidate values, not ratification for C2.

File sources already bind reads to planned ETag/version/checksum/local-generation evidence and fail generation mismatches. Effective-schema execution already reattests physical schema and processed position in selected paths. Those mechanisms should be reused by a retry acceptance gate; retry must not invent a second identity model or auto-replan inside a run.

## Frontier options

### Option A: detached producer per partition plus bounded channel

Each admitted partition opens and drains in its own injected-host task, sending batches to a one-item channel; a coordinator reads channels in canonical order.

Advantages: straightforward live polling; Tokio/DataFusion have mature variants; channel send naturally backpressures each producer.

Rejected for the smallest C2 step. It adds a task and channel per partition, still needs a separate global byte/ordinal budget, allows every producer to allocate one in-flight plus one queued payload unless permits are acquired before polling, and recreates the lifecycle problem exposed by the prototype. The current opaque stream contract also cannot prove that cancelling the wrapper joined driver-owned producer scopes.

### Option B: whole-partition outcome futures in `FuturesUnordered`, reorder complete bundles

Each future opens and drains a whole partition, returning a complete outcome bundle; the scheduler stores completed bundles by ordinal until the head releases.

Rejected. A single partition can be arbitrarily large, so a stalled first partition makes later retained outcomes input-sized. Spooling them would add deterministic spill policy and I/O beyond C2's smallest architecture. It also delays file/unit failure and backpressure until a whole partition completes.

### Option C: scheduler-owned one-step open-and-poll frontier

One scheduler object owns a bounded `FuturesUnordered` of partition step futures. A step owns the partition attempt state and either opens the stream and polls one batch, or polls one next batch from an already-open stream. It returns `(canonical ordinal, attempt id, stream state, Batch | EOF | typed error)` to the scheduler. The scheduler stores at most one ready step per admitted ordinal, exposes only the current canonical ordinal, and rearms an ordinal only after the engine acknowledges consuming or discarding that step.

Recommended. It creates no new detached task or channel, continually polls every admitted open/stream that wakes, keeps release policy explicit, and makes the stopped-head bound structural:

- at most `effective_jobs` ordinals are admitted;
- each ordinal has exactly one of `polling`, `ready-one-step`, `released`, or `terminal` state;
- at most one batch outcome per non-head ordinal is retained;
- the maximum admitted ordinal is at most `frontier + effective_jobs - 1`;
- a ready later batch is not polled again until it becomes canonical, so a stalled head naturally halts later producers;
- a completed head admits one next ordinal, preserving a sliding window without a wave barrier.

The frontier should be a scheduler-owned state machine in `cdf-runtime`, with an engine adapter supplying the existing canonical batch processor/writer. It should consume the already-resolved effective-jobs result and canonical schedule; it must not recompute ceilings or know driver names.

This option has one prerequisite: replace or wrap opaque `BatchStream` with a neutral lifecycle-bearing opened-attempt handle that supports `cancel` and awaitable `join`, with a no-op implementation for same-task streams. The frontier owns that handle until EOF or shutdown. On terminal failure it stops admission, cancels all handles, awaits every join, releases all payload/admission permits, then reports errors in canonical unit order. Without this seam, Option C is bounded but cannot satisfy C2 cancellation evidence.

## Recommended candidate frontier contract

This is a draft for confirm-or-correct shaping, not an active specification.

1. The runtime scheduler MUST be the sole owner of admitted partition attempt state, open futures, next-batch poll futures, ready outcomes, retry timers, and lifecycle joins. It MUST use the canonical schedule and already-resolved effective jobs without driver-name dispatch or duplicate ceiling arithmetic.
2. Each admitted ordinal MUST have at most one live attempt and at most one outstanding poll. A poll step MUST retain the opened stream and return exactly one batch, EOF, or typed error. Completion order MUST be runtime-only; only the next canonical ordinal may be acknowledged into the existing engine batch processor and canonical segment writer.
3. The live window MUST be bounded simultaneously by jobs, ordinal distance, ready item count, and accounted bytes. A non-head ordinal MUST NOT be re-polled while it has a ready batch. An outcome exceeding its declared maximum working set MUST fail as a capability-contract violation. Unaccounted compatibility batches MUST be migrated to a scheduler-adopted retention lease or remain serial; item bounds alone MUST NOT substantiate memory safety.
4. Engine acknowledgement MUST be transactional: consuming a canonical batch transfers its payload lease into the existing transform/assembler path before the frontier rearms that ordinal. EOF advances the partition frontier and admits at most one next ordinal. Error or cancellation transfers no data, position, evidence, file completion, or state authority.
5. `--jobs 1` MUST use the same frontier state machine with a one-ordinal window. Fixed plan/input runs at jobs 1/N MUST feed the same batches to the same canonical processor in the same `(partition ordinal, local batch sequence)` order; runtime waits and attempt timing remain nonidentity evidence.
6. Until exact speculative limit and position-slice authority is separately present, any global limit MUST set admission/ordinal lookahead to one. The next partition MUST NOT be opened or attested until the canonical processor proves the limit remains unsatisfied. Within the head batch, limit stays at the compiled jobs=1 position after residual filters and before projection/contract evaluation. A crossing batch may advance a sliced position only when typed exact slice authority exists; otherwise the existing conservative boundary rule applies. Discarded work MUST transfer no position or observation authority.
7. File/source completion MUST be an EOF acknowledgement over every selected canonical unit. A file `FileManifest` position MUST remain pending until all selected units are canonically acknowledged; one unit success or speculative EOF cannot mark the file processed.
8. Cancellation MUST stop admission and retry timers immediately, cancel every lifecycle-bearing opened attempt, await every async/blocking-lane join, then release admission and memory authority. Merely dropping futures, channels, or join handles MUST NOT count as joined cleanup.

## Retry semantics requiring ratification

The following candidate is intentionally exact so it can be corrected. None of these defaults is authorized yet.

### Eligibility and granularity

Candidate: retry only when all are true: the planned unit is idempotent and reopenable; its declared granularity permits this exact partition/unit; and the error kind is in the source's declared `retryable_errors`. Initial eligible kinds are `Transient` and `RateLimited` only. `Auth`, `Contract`, `Data`, `Destination`, and `Internal` are terminal for extraction retry. Credential refresh remains a separate source protocol step and does not make `Auth` a scheduler-retryable class.

### Attempts and elapsed deadline

Candidate: `max_total_attempts = 3`, including the original attempt, and `max_elapsed = 30s`, measured by the injected monotonic clock from first attempt start through opens, work, and sleeps. The first exhausted bound wins. A server delay that cannot begin and leave positive time before the deadline terminates without another attempt.

### Backoff and jitter

Candidate: exponential delay `min(100ms * 2^(failed_attempt-1), 5s)`, with full jitter selected from `0..=cap`; a valid typed `retry_after_ms` is a minimum delay, still bounded by the elapsed deadline. Production jitter is runtime entropy and never identity evidence; deterministic tests inject clock and RNG seeds. Retry history records attempt ordinal, typed cause, selected redacted delay, and exhaustion reason, then renders histories in canonical unit order.

An MVP no-jitter alternative is simpler and reproducible but risks synchronized retries across runs. Deterministic jitter keyed only by plan/unit identity was rejected as a default because separate workers processing the same authority could synchronize.

### Reattestation and replan

Candidate: every retry uses the same planned unit identity and a new runtime attempt id. Before reopening, and again before accepting a successful retried EOF/outcome when the source has mutable external identity, the scheduler invokes the source's existing generation/snapshot/schema attestation. Planned immutable identity hash, generation/snapshot position, and physical schema attestation must match the plan and the first accepted evidence. Any mismatch is a terminal typed identity/schema error with `replan required`; the scheduler MUST NOT auto-replan or mix generations inside the run.

### Duplicate-attempt authority

Candidate: C2 does not hedge attempts. A retry starts only after the preceding attempt has terminated and joined. The scheduler maintains one state machine keyed by planned `(partition ordinal, unit ordinal)` and is the sole authority that can transition `running -> succeeded -> released`. A second success is never allowed to cross the frontier: byte-identical duplicate success is discarded and reported as an internal invariant breach; conflicting identity/schema is terminal data drift. No source or engine callback may independently choose the winning attempt.

### Operator and source overrides

Candidate precedence: source capability is the hard safety ceiling; compiled source policy may narrow eligible kinds/granularity and lower budgets; run/operator policy may disable retry or lower attempts/deadline, but cannot add an error class, widen granularity, or exceed source policy. No CLI retry widening is added in C2. Source-provided `retry_after_ms` may lengthen a particular delay only within the elapsed deadline. Existing REST `RetryPolicy::default` is removed from execution authority or adapted to the scheduler policy rather than composed as a nested second budget.

## Confirm-or-correct questions

1. Should C2 adopt the one-step, same-task `FuturesUnordered` frontier and add a neutral lifecycle-bearing stream handle so cancellation can be awaited? Decision unlocked: scheduler ownership, memory/backpressure shape, and cancel/join acceptance. I recommend yes; retain serial lookahead for all limited runs until exact speculation/slicing authority exists.
2. Confirm or correct the retry budget/timing candidate: three total attempts, 30 seconds wall elapsed, 100ms exponential/full-jitter backoff capped at 5 seconds, with typed `retry_after_ms` as a minimum subject to the deadline. Decision unlocked: executable retry policy and deterministic timer/RNG tests. I recommend these values as a small bounded baseline, not reuse of the current driver-local retry counter.
3. Confirm or correct retry safety/precedence: only declared `Transient`/`RateLimited`; retry only after prior attempt joins; pre-open plus pre-accept reattestation; generation/schema change requires replan; scheduler alone accepts one success; source is the hard ceiling and operator policy may only narrow/disable. Decision unlocked: identity, duplicate-attempt, and override semantics. I recommend this fail-closed policy and no automatic in-run replan or CLI widening.

## Conclusion

Option C is the smallest architecture that addresses the failed prototype without adding another producer layer. Its central move is to separate completion-order polling from canonical release while allowing only one ready batch per admitted ordinal. That yields bounded live concurrency and structural stalled-head backpressure while reusing C1 admission, A3 segmentation/positions, and A5 canonical persistence.

Implementation is not yet authorized. Retry defaults remain semantic blockers pending ratification, and cancel-and-join requires the neutral lifecycle-bearing stream seam before C2 can claim structured cleanup.

## Limits

This research does not prove the design with code, tests, memory traces, or scaling measurements. It does not select unit-level Parquet/ORC/Avro decomposition, define a new exact position-slice algebra, authorize speculative limited reads, or activate/supersede the scheduler spec. The current REST retry loop and compatibility unaccounted batches need migration, but this record does not assign or implement that work. DataFusion patterns are analogies for polling and lifecycle ownership, not semantic authority for CDF order, retry, limit, position, or package identity.
