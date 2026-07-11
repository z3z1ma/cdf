Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-c1-scheduler-admission-contract.md

# Deterministic scheduler core

## What was observed

The neutral runtime can compile a scan/source plan into contiguous canonical partition ordinals with immutable identity hashes and explicit working-set, executor, retry, speculation, and ordering facts. Speculation is rejected unless reads are idempotent, reopenable, and attested.

Auto jobs resolves from effective container CPU (including fractional cgroup quotas), managed memory, source maximum/useful concurrency, configured jobs, transport, destination, lane, scope, and partition ceilings. The result records all limiting factors and remains runtime evidence rather than package identity.

The deterministic admission controller uses round-robin resource queues and typed release permits while enforcing global jobs/memory/CPU/I/O/connection capacity, shared quota authorities, and exclusive checkpoint scopes. A blocked head in one resource does not prevent an independent resource from admission.

## Procedure

- `cargo test -p cdf-runtime scheduler::tests -- --nocapture` — 4 passed.
- strict Clippy across runtime and all first-party source adapters — passed.

## What this supports

C1 has a source/destination-neutral admission calculus suitable for the later A5 production graph and C2 fan-out. No scheduler branch names a source, destination, transport, or native library.

## Limits

The controller is not yet bound into engine plan/explain artifacts or host/memory leases. Deterministic cancellation scenarios, static private-pool gates, and a recorded context-switch benchmark remain before C1 closure.
