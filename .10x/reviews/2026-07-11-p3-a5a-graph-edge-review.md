Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-a5a-graph-edge-contracts.md
Verdict: pass

# P3 A5a graph and accounted-edge adversarial review

## Target

The engine-neutral graph artifact/compiler, accounted data/outcome envelopes, bounded edge, cancellation primitive, structured execution scope, destination affinity declarations, plan/package integration, tests, and benchmark evidence through the working tree that closes A5a.

## Findings

### Resolved — significant: submission-order join could deadlock first failure

The host awaited child tasks in submission order. A permanently blocked first task could prevent observation of a later failure, so cancellation never fired. Join now supervises I/O, CPU, and blocking completions in one completion-ordered set. First failure cancels or aborts siblings, and the scope consumes every completion before returning. A regression proves a never-completing task releases its memory lease when a sibling fails.

### Resolved — significant: source position was mandatory and would force invention

Not every source batch has a position. Requiring one would make future adapters fabricate authority. The envelope now carries `Option<SourcePosition>`: presence and absence are both explicit, typed facts.

### Resolved — significant: runtime-owned outcome summary constrained extensions

A fixed `GraphOutcome` vector would force custom/Python/WASM operators to convert typed facts into a runtime-specific summary or add branches. `GraphDataEnvelope<O>` and `AccountedGraphOutcomes<O>` are now generic; the neutral runtime owns lifetime/accounting while each operator owns its outcome type.

### Resolved — significant: first blocking lane was accidental routing authority

Selecting `blocking_lanes.first()` would make destination behavior depend on declaration order and break destinations with maintenance plus commit lanes. Capability sheets now name staged-ingress and final-binding lanes explicitly, validate references, and preserve backward deserialization defaults. The graph test declares two lanes and proves final binding selects the named second lane.

### Resolved — minor: graph identity could depend on caller collection order

Construction now canonicalizes nodes by deterministic topological order and edges by id. Reversed mock source/operator/destination inputs produce the identical graph and semantic hash. Runtime queue capacity and timing remain outside identity.

## Verdict

Pass. The child ticket's contract boundary is complete, source/destination/operator extension remains name-agnostic, the data plane cannot use naked edge payloads, cancellation/backpressure ownership is proven, and measured edge overhead is below 100 ns per envelope on the recorded host. Business-stage migration correctly remains in A5b/A5c/A5e rather than leaking into this child.

## Residual risk

`futures-channel` may accept an item into its internal queue before the sender future reports readiness; cancellation therefore relies on structured receiver teardown to release an already-transferred lease. The full-channel regression proves scope teardown returns the ledger to zero. A future specialized ring buffer may reduce the measured ~97 ns incremental cost, but current cost is small relative to 1–32 MiB payload work and does not justify a custom unsafe queue.
