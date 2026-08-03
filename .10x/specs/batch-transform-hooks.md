Status: draft
Created: 2026-08-03
Updated: 2026-08-03

# Plan-declared batch transform hooks

## Status and authority conflict

The user wants inline transformation hooks in Python, WASM, or another embedded language. This
draft defines the safe data-plane contract and separates it from lifecycle side effects.

Implementation is blocked by an explicit active-authority conflict: VISION D-23 and existing
records state that Python is authoring/interchange only and never the execution substrate. An
execution-time Python transform host would supersede that rule. The user must ratify a narrower
replacement, including trust and sandbox expectations, before Python hook execution is authorized.

WASM is also not implementation-ready: `cdf-wasm` is a placeholder, and the canceled WIT
foundation found no ratified acyclic wire projection for recursive scope/source-position types.

## Purpose

Permit bounded, vectorized, plan-declared transformation of Arrow batches without weakening:

- compiled schema/contract/semantic identity;
- constant-memory and backpressure guarantees;
- deterministic package construction and replay;
- source position, watermark, CDC control-field, and provenance authority;
- error ownership, quarantine, and cancellation;
- dependency isolation and source/destination extension boundaries.

## Two distinct capabilities

### Data transform hook

A data transform hook consumes and returns accounted Arrow `RecordBatch` streams. It may change
values/schema only as declared at compile time. It is identity-bearing and executes before package
segment encoding.

### Lifecycle observer/action

A lifecycle observer/action receives package, receipt, checkpoint, or run events and may perform an
external side effect only under a separately ratified idempotency/retry/failure/security contract.
It never mutates Arrow data or package/checkpoint identity.

These capabilities MUST use separate specs, traits, plan declarations, permissions, and error
policies. Current in-process `RuntimeStageHook`/`ReceiptVerifiedHook` test callbacks are not product
authority for either one.

This specification covers data transform hooks only. Lifecycle actions are excluded.

## Initial attach point

The recommended first version supports exactly one attach point:

```text
source decode and schema reconciliation
→ pre-contract transform hook
→ native expressions
→ contract validation/quarantine
→ normalization
→ package encoding
```

Reasons:

- hook output schema and semantics can be compiled into the ordinary contract;
- invalid output is checked before package identity;
- one point avoids ambiguous ordering among hooks, contracts, and normalization;
- post-contract mutation could invalidate an already-issued verdict.

Future post-normalization hooks are excluded until they either re-enter full schema/contract
validation or prove they cannot change governed fields/values. “Post-decode,” “pre-contract,” and
“post-normalize” are not interchangeable aliases.

## Hook declaration

Each hook MUST be fully resolved in the project compilation manifest and include:

- stable hook id and declaration version;
- runtime kind/version (`wasm-component`, a ratified Python host, etc.);
- project-relative artifact/module reference;
- exact content/module hash and dependency/environment identity;
- attach point and deterministic order;
- expected input Arrow schema/hash;
- declared output Arrow schema/hash;
- field-level lineage or explicit opaque-lineage markers;
- semantic additions/removals/changes and resolved definition hashes;
- source-position behavior (`preserve` only in the first version);
- watermark behavior (`preserve`, `transform` with mapping id, or `drop`);
- row-count behavior (preserve/filter/expand) with declared bounds;
- memory/output expansion bounds;
- allowed host capabilities and resources;
- timeout/fuel/cancellation policy;
- error policy and stable hook error namespace;
- deterministic/purity declaration and verification class.

Unpinned “latest,” import-path lookup at runtime, mutable virtual environments, or network-fetched
code are forbidden.

## Data contract

- Input and output are Arrow C Data/PyCapsule/component-model batches with exact schema identity.
- No row-at-a-time callback API is offered.
- Hook output buffers enter CDF memory accounting before downstream retention.
- Shared zero-copy buffers retain one ownership/lease authority; new allocations reserve and
  reconcile bytes.
- Output batch row/byte expansion cannot exceed the compiled bound.
- Empty batches and zero-column/zero-row edge cases have explicit Arrow behavior.
- Dictionary, nested, extension metadata, semantic/source/physical metadata, and nullability are
  preserved or changed only as declared.
- The hook cannot retain a borrowed batch beyond the call/stream ownership contract.
- Cancellation closes the foreign boundary and releases every lease exactly once.

## Schema and semantic effect

At compile time the hook MUST provide enough authority to derive an exact output schema. Acceptable
first-version models are:

- exact declared output schema checked against the module's exported declaration; or
- a closed schema-transform description (add/drop/rename/cast fields) validated and hashed by CDF.

Runtime output must match exactly. “Infer from the first batch” is forbidden for governed
execution.

Semantic changes resolve through `.10x/specs/semantic-type-registry.md`. A hook cannot emit an
unknown semantic tag, forge `cdf:physical_type`, weaken PII classification, or reinterpret an exact-
value encoding without a declared semantic cast.

## Control authority

The first version MUST preserve:

- batch/resource/partition identity outside the payload;
- source positions and source continuation;
- CDC `_cdf_op`, key fields, and transaction order;
- fields used by cursor/checkpoint/window planning;
- schema-evidence control metadata;
- contract-required/control-critical fields.

A pre-contract hook MAY filter/derive ordinary data only when its declared row/schema effect allows
it. It MUST NOT alter source positions or synthesize watermark completeness.

Watermark behavior defaults to `drop` for arbitrary transforms unless CDF can prove preservation
from the declared transform. A claimed `preserve` requires that the event-time field survives with
identical values/type/order; `transform` requires a versioned monotone mapping authority. This uses
the existing operator-watermark vocabulary.

## Determinism and replay

Packages record post-hook batches, so replay of a finalized package MUST NOT execute the hook again.
That protects replay from future runtime/code availability and nondeterminism.

Original execution and pre-finalization retry still require determinism:

- same hook identity, input batch, declared environment, and configuration MUST produce byte-
  equivalent Arrow output and the same error/verdict behavior;
- time, randomness, network, ambient environment, and undeclared filesystem access are denied;
- dependency/runtime versions are part of identity;
- hook output is subject to golden/repeatability tests;
- a nondeterministic hook mode, if ever allowed, requires explicit package evidence and disables
  cache/retry claims; it is excluded initially.

Side effects are forbidden in data transform hooks. This includes HTTP calls, database writes,
email/notifications, arbitrary file writes, and subprocesses.

## Python-specific proposal

If D-23 is superseded, a Python host MUST NOT execute arbitrary in-process project code by default.
The activation decision must choose and evidence:

- isolated worker process versus in-process interpreter;
- supported CPython/free-threaded versions and dependency lock format;
- PyCapsule/Arrow ownership and GIL behavior;
- import/filesystem/network/secret capability policy;
- cancellation/timeout and worker-crash classification;
- stdout/stderr/log redaction and bounded capture;
- process reuse without cross-project state leakage;
- deterministic environment/content identity;
- performance floor against a native Arrow pass-through roofline.

In-process Python has a larger crash/security/allocator blast radius and is not the recommended
default for untrusted hooks. An isolated, capability-limited worker is the recommended starting
research option, not yet a ratified design.

## WASM-specific proposal

WASM SHOULD be the preferred portable sandbox target after current ecosystem research, but only
after:

- one ratified acyclic wire projection for recursive `ScopeKey`/`SourcePosition` values;
- a stable component/WIT stream state machine for batches, terminal status, errors, cancellation,
  and host quotas;
- Arrow interchange and ownership costs are measured against copy/PyCapsule alternatives;
- Wasmtime/WASI version/capability behavior is freshly verified;
- module signature/registry distribution is kept separate from local module execution.

The initial transform interface need not expose scope/position values if the hook is forbidden to
change them; omitting them may avoid the recursive WIT blocker. That narrower interface must be
proven sufficient and MUST NOT later smuggle mutable control state through opaque JSON.

## Error and quarantine behavior

- declaration/schema/capability mismatch: Contract before execution;
- missing/changed hook artifact or dependency lock: Data drift before execution;
- output schema/control-field mismatch: Data if produced by hook, then fail the batch/run before
  package publication;
- deterministic per-row validation error returned through a declared result channel: may enter the
  ordinary contract/quarantine pipeline only if source row mapping and redaction evidence remain
  exact;
- worker crash, timeout, OOM, protocol violation, or malformed Arrow: typed Environment/Data error
  according to ownership, with hook/runtime provenance;
- CDF host/lease/protocol invariant violation: Internal;
- hook failure advances no checkpoint and produces no partial destination receipt.

The hook MAY return typed row-level rejection evidence only through a future explicitly specified
side channel. The first version SHOULD fail the batch on hook error and let ordinary contracts own
quarantine after successful output.

## Performance contract

- batch-level Arrow interchange only;
- useful batch sizes and maximum expansion are compiler/host bounded;
- hook time, input/output bytes, allocation/copy bytes, and queue wait are measured separately;
- no private runtime/thread pool; execution uses an injected foreign/blocking lane with declared
  useful concurrency;
- parallelism is bounded and cannot reorder batches where cursor/CDC semantics require order;
- pass-through and representative transform rooflines compare the hook host to direct native
  Arrow kernels on the same host;
- a hook language/runtime cannot be described as high-throughput without measured copy and
  serialization overhead.

## Retired `records_transform` field

REST currently accepts `records_transform` but does not execute it. Until this spec is active and a
runtime kind is implemented, CDF MUST fail configuration that supplies the option or remove the
option from the active schema. Silently accepting it is forbidden.

The repair is owned by `.10x/tickets/2026-08-03-rest-records-transform-contract-repair.md` and must
not implement an ad hoc REST-only Python hook.

## Acceptance scenarios

1. Given a declared pass-through hook, output schema/data/metadata/positions are identical, memory
   is accounted, and two executions are byte-equivalent.
2. Given a schema-changing hook, compilation records exact output schema/lineage/semantics and the
   contract validates that output; runtime drift fails before package publication.
3. Given a hook tries to remove `_cdf_op`, a key, cursor, or event-time field under preserve
   authority, compilation or execution fails and no checkpoint advances.
4. Given a hook allocates beyond its bound, backpressure/failure is deterministic and all leases
   release.
5. Given cancellation or worker crash, no partial output batch/package/destination mutation becomes
   authoritative.
6. Given a finalized package replay, no hook runtime or source is contacted.
7. Given the same input/code/environment twice, golden output and error behavior match.
8. Given a Python hook without D-23 supersession, compilation rejects it with the governing
   authority conflict.
9. Given a WASM hook before WIT/runtime activation, compilation rejects it rather than treating the
   placeholder crate as support.
10. Given `records_transform` in REST before hooks activate, config fails early and never pretends a
    transform ran.

## Explicit exclusions

- lifecycle notifications/actions;
- row callbacks;
- source/destination connector implementation inside a hook;
- arbitrary DataFusion/Python/WASM expressions in identity-bearing runtime plans;
- ambient network/filesystem/secrets;
- runtime code download or mutable dependencies;
- hooks that mutate positions, checkpoint state, package manifests, receipts, or destination
  plans;
- post-load warehouse transformation graphs.

## Open blockers

1. User ratification to supersede or retain D-23 for Python execution.
2. First runtime kind and sandbox boundary.
3. Exact first attach point and allowed row-count behavior.
4. Hook declaration/file syntax in the SQL/project front-end.
5. Exact schema-effect declaration and lineage model.
6. WIT recursive-value resolution or proof the narrow transform interface excludes it safely.
7. Performance/admission thresholds.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/project-compilation-manifest.md`
- `.10x/specs/semantic-type-registry.md`
- `.10x/specs/foreign-stream-interop.md`
- `.10x/decisions/neutral-foreign-stream-boundary.md`
- `.10x/tickets/cancelled/2026-07-08-wasm-wit-interface-foundation.md`
- `.10x/research/2026-07-18-wasi03-stream-cost-interface-model.md`
- `.10x/decisions/datafusion-analysis-scheduling-identity-boundary.md`
- `VISION.md` D-2, D-23, D-25, D-26
