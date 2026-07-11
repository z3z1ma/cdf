Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-c1-scheduler-admission-contract.md

# Deterministic scheduler core

## What was observed

The neutral runtime can compile a scan/source plan into contiguous canonical partition ordinals with immutable identity hashes and explicit working-set, executor, retry, speculation, and ordering facts. Speculation is rejected unless reads are idempotent, reopenable, and attested.

Auto jobs resolves from effective container CPU (including fractional cgroup quotas), managed memory, source maximum/useful concurrency, configured jobs, transport, destination, lane, scope, and partition ceilings. The result records all limiting factors and remains runtime evidence rather than package identity.

The deterministic admission controller uses round-robin resource queues and typed release permits while enforcing global jobs/memory/CPU/I/O/connection capacity, shared quota authorities, and exclusive checkpoint scopes. A blocked head in one resource does not prevent an independent resource from admission. Cancellation drains queued requests in canonical resource/ordinal order, prevents new admission, and retains active accounting until joined work releases its permit. Invalid permit payloads fail transactionally without leaking capacity.

The CLI resolves effective jobs from the injected host's logical CPU authority and currently available managed-memory budget plus partition, source maximum/useful, configured, transport, lane, destination-stage, and scope inputs. The resulting runtime resolution is emitted in JSON and human plan reports only; it is deliberately absent from `EnginePlan` and package identity. Destination writer concurrency is reported as its own bounded lane and does not collapse upstream extraction/decode concurrency for single-writer destinations.

## Procedure

- `cargo test -p cdf-runtime scheduler::tests -- --nocapture` — 7 passed.
- `cargo test -p cdf-cli --lib plan_json_exposes_pushdown_ddl_guarantee_and_state_advancement -- --nocapture` — passed; JSON contains effective jobs, available managed memory, and destination writer concurrency without writes.
- `cargo test -p cdf-cli --lib plan_human_headless_render_uses_operator_panels -- --nocapture` — passed; the same resolution is human-readable.
- `cargo test -p cdf-engine production_runtime_ownership_is_centralized -- --nocapture` — passed; production source contains no private runtime/pool construction outside the standalone host.
- strict Clippy across runtime, CLI, and benchmark targets — passed.

## What this supports

C1 has a source/destination-neutral admission calculus suitable for the later A5 production graph and C2 fan-out. No scheduler branch names a source, destination, transport, or native library. Capability validation rejects missing/zero working sets, unsafe speculation/retry combinations, and invalid concurrency before admission.

## Limits

This is the admission contract, not production partition fan-out. Actual task execution, canonical frontier/reorder, jobs-invariance packages, rate timers, and live memory leases belong to C2/A5/C3/C4. Loom is not useful for this single-owner controller because it exposes no shared concurrent mutation; deterministic transition-sequence tests cover its state machine, while the host's concurrent slot behavior has separate synchronization tests.
