Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Driver and session concurrency canon

## Context

CDF's trait bounds already encode a coherent concurrency model, but the model is distributed
across Rust signatures:

- source and destination drivers are `Send + Sync` factories;
- destination runtimes are exclusively owned per run and are not required to be `Send` or
  `Sync`;
- finalized-package commit sessions borrow their runtime and are not `Send`;
- staged-segment ingress sessions are `Send`;
- source compilation is synchronous and contact-free while execution is asynchronous through
  the source frontier and injected execution host;
- source portability fails closed unless a driver explicitly validates its compiled plan.

An adapter author can infer the law, but the fourth destination or catalog source should not have
to reverse-engineer it from bounds.

## Decision

The following is the canonical extension law:

1. A driver is a thread-safe, stateless or internally synchronized factory and descriptor.
2. A resolved runtime is exclusively owned by one logical run. It may hold non-`Send` native
   handles when its declared blocking lane or ingress protocol confines them safely.
3. Finalized-package ingress is a borrowed, serial lifecycle over a verified package unless the
   destination's own implementation internally schedules bounded work through injected host
   services.
4. Staged-segment ingress is the primary high-throughput category. Its owned session is `Send`
   because generic orchestration may deliver durable segments across task boundaries. Concurrency
   remains bounded by sheet declarations, memory, and stage-local pressure; `Send` does not grant
   an unbounded private executor.
5. A destination exposes exactly one prepared ingress category. Unsupported methods do not exist
   and no destination identity selects the generic branch.
6. Source compilation and portable-plan validation perform no source contact or heavy work.
   Discovery is an explicit bounded compiler stage; execution and retry use injected asynchronous
   host services.
7. Portability, retry safety, staged concurrency, and ingress mode fail closed. A new driver must
   explicitly claim and pass conformance for each capability.

Conformance MUST compile and exercise one synthetic source and one synthetic destination against
this law. Documentation MUST answer where a native handle lives, which operations may cross
threads, where concurrency is admitted, and which ingress category the adapter implements.

## Alternatives considered

- Require every runtime/session to be `Send + Sync`: rejected because it would force unsafe or
  mutex-heavy wrappers around legitimate thread-affine database/interpreter handles.
- Make every session non-`Send`: rejected because staged high-throughput delivery and remote
  workers need owned, movable ingress sessions.
- Support both ingress modes on every destination: rejected because it creates unused commit
  engines and unsupported-method stubs.
- Infer portability from serialization success: rejected because external resources, credentials,
  and host-bound runtimes require explicit semantic validation.

## Consequences

The Rust bounds remain minimal and performance-neutral. The new obligation is compiler-enforced
documentation/conformance around those bounds. Future warehouse adapters may choose finalized
ingress when atomic bulk APIs require a complete package, but staged ingress is the preferred
high-throughput design where the destination can durably acknowledge independent segments.
