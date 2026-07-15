Status: active
Created: 2026-07-14
Updated: 2026-07-14

# Deterministic source retry policy

## Context

CDF's source capabilities already declare whether reads are idempotent and reopenable, the safe retry granularity, typed retryable error classes, resumability, and attestation strength. They do not define an execution budget, deadline, backoff, jitter, override precedence, or the authority that admits a retried outcome. `cdf-http` also contains a transport-local retry counter whose delay is not consistently executed. Composing that loop with scheduler retries would create two independent budgets and make retry behavior dependent on adapter implementation.

The deterministic parallel scheduler requires retries to preserve canonical output, package identity, source generation, schema epoch, constant memory, cancellation, and exact file completion. A streaming partition that has already released batches is not an atomic retry boundary unless the source proves exact resumable continuation or execution stages all attempt effects until EOF.

The user ratified all three candidate decisions in `.10x/research/2026-07-12-c2-frontier-retry-architecture.md`: the one-step same-task frontier, the exact budget/backoff values, and the fail-closed safety/precedence model.

## Decision

The runtime scheduler owns one compiled retry policy and one attempt state machine per planned retry unit. Source capabilities are the hard safety ceiling. Compiled source policy may only narrow that ceiling, and run/operator policy may only disable retry or lower attempts/deadline. No operator or adapter may add an error class, widen granularity, or exceed source policy. C2 adds no CLI widening surface.

The default policy is:

- at most three total attempts, including the original;
- at most 30 seconds of monotonic wall elapsed from the first attempt start, including work and delays;
- exponential backoff with a 100 millisecond base and 5 second cap;
- full jitter chosen uniformly from zero through the exponential cap;
- a typed `retry_after_ms` is a minimum delay, still subject to the elapsed deadline.

The elapsed deadline governs admission of another attempt; it is not a source-operation timeout. Time spent in completed attempts counts against the remaining retry budget, but an attempt that began within budget may finish successfully after the deadline. This distinction prevents a retry default from becoming an implicit 30-second limit on legitimate large scans. Explicit source/run timeouts, where configured, remain separate cancellation policies.

Production jitter uses runtime entropy and never enters artifact identity. Deterministic tests inject clock and entropy. Retry history records attempt ordinal, typed cause, selected redacted delay, and exhaustion reason as runtime evidence.

An attempt is eligible only when the exact planned unit is idempotent and reopenable, its declared retry granularity covers the attempted boundary, and the error kind is both source-declared and one of `Transient | RateLimited`. `Auth`, `Contract`, `Data`, `Destination`, and `Internal` are terminal for source execution retry. Credential refresh remains a distinct protocol operation.

A retry begins only after the prior attempt has terminated and joined. Before reopening and before accepting a successful retried outcome, CDF reattests the same planned immutable/snapshot identity and schema evidence. Generation, snapshot, or schema change is terminal and requires replan; CDF never auto-replans within a run. The scheduler is the sole success authority, and exactly one successful attempt may cross the canonical frontier.

Retries may occur only at an atomic attempt boundary. Before any output has crossed the canonical frontier, the complete planned unit may restart. After output has crossed, retry requires either exact resume authority at the acknowledged source position or attempt-local durable staging whose data, verdict, lineage, position, and completion effects remain invisible until EOF. In the absence of either proof, the error is terminal rather than duplicating or rolling back visible work heuristically.

The existing `cdf-http` policy ceases to be execution authority. Transport protocol helpers may classify a response and expose typed `retry_after_ms`, but scheduler policy owns attempts, elapsed budget, delay, and final outcome admission.

## Alternatives considered

- Keep adapter-local retry loops. Rejected because budgets compose unpredictably, delays and cancellation differ by adapter, and duplicate success has no single authority.
- Retry every reopenable partition after any stream error. Rejected because batches and evidence may already be visible; replaying them would duplicate or require unbounded in-memory rollback.
- Deterministic jitter keyed by plan/unit identity. Rejected because independent workers processing the same identity would synchronize.
- No jitter. Rejected because fleets would synchronize retries during shared outages or rate limits.
- Automatic in-run replan after identity drift. Rejected because one run would mix schema/generation epochs and invalidate package identity.

## Consequences

Runtime needs injected monotonic-clock and entropy authorities, a compiled narrowing operation, typed attempt history, and a cancellation-aware delay. Sources expose capabilities and attestations but do not run independent execution budgets. The canonical frontier can retry opens and other atomic steps immediately; mid-stream retry remains fail-closed until exact resume or attempt-local staging proves atomicity. Tests can deterministically falsify budget, jitter bounds, cancellation, reattestation, duplicate acceptance, and jobs invariance without making timing part of identity.
