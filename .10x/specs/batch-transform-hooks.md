Status: active
Created: 2026-08-03
Updated: 2026-08-08

# Plan-declared batch transform hooks

## Status and authority

This specification defines the safe data-plane contract for inline transformation hooks and
separates it from lifecycle side effects. It is active.

The former VISION D-23 authority conflict is resolved by
`.10x/decisions/python-in-process-batch-transform-hooks.md`, which narrowly supersedes D-23's
downstream clause for plan-declared batch transform hooks only. That decision ratifies four
parameters governing every Python-specific rule in this specification:

- hooks are **first-party project code only**, carrying the same trust as the repository's
  `cdf/<namespace>/<resource>.cdf.sql` resources; registry distribution, third-party modules, and
  any untrusted authoring tier are out of scope;
- the substrate is **in-process PyO3 on the D-25 Python pool**, reusing `cdf-python`'s existing
  PyCapsule zero-copy path under D-25's identical-semantics rule for GIL and free-threaded builds;
- environment identity is a **fully resolved, hash-pinned `uv.lock` plus an explicit CPython
  version**;
- the performance floor is **measured against a native Arrow pass-through roofline, then ratified**
  from observed data and enforced as a closure gate.

D-23 otherwise stands: Python remains an authoring and interchange surface everywhere else, and the
kernel's dependency graph contains no Python.

WASM remains out of scope for this version: `cdf-wasm` is a one-line placeholder, and the cancelled
WIT foundation found no ratified acyclic wire projection for recursive scope/source-position types.
This specification does not prejudge D-26; WASM remains the preferred portable sandbox if hooks are
ever opened to untrusted authors.

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
- time, randomness, network, ambient environment, and undeclared filesystem access are **declared
  and compile-time audited, not runtime-enforced**. The hook declares purity; CDF audits its
  declarations and imports at compile time and MUST reject on mismatch. Because the ratified
  substrate is in-process, the runtime does not confine a hook that violates its own declaration.
  This is accepted residual risk recorded in
  `.10x/decisions/python-in-process-batch-transform-hooks.md`, and it is the reason replay is
  protected structurally — packages record post-hook batches, so replaying a finalized package never
  re-executes the hook — rather than by confinement;
- dependency/runtime versions are part of identity;
- hook output is subject to golden/repeatability tests;
- a nondeterministic hook mode, if ever allowed, requires explicit package evidence and disables
  cache/retry claims; it is excluded initially.

Side effects are forbidden in data transform hooks. This includes HTTP calls, database writes,
email/notifications, arbitrary file writes, and subprocesses.

## Python host contract

The ratified substrate is an in-process PyO3 host on the D-25 Python pool, executing first-party
project code only. The following are requirements, not open options.

**Interpreter and pool.** The host MUST reuse the existing `cdf-python` interpreter and dedicated
Python pool rather than introducing a second Python entry point. Per D-25, semantics MUST be
identical on GIL and free-threaded builds; the design MUST NOT depend on free-threading and MUST NOT
waste it. Supported interpreter versions MUST be declared and pinned as part of environment
identity.

**Arrow ownership.** Batches cross via the existing PyCapsule zero-copy path
(`crates/cdf-python/src/arrow_capsule.rs`). Existing release-exactly-once and lease-ownership
behavior is authority; a hook MUST NOT retain a borrowed batch beyond the call contract, and
cancellation MUST release every lease exactly once. Hook output buffers enter CDF memory accounting
before downstream retention.

**Cancellation and timeout.** The host MUST reuse `ForeignCancellation`, budget timeouts, and the
`Cancelled` terminal status already proven in the Python producer direction
(`crates/cdf-python/src/driver.rs`), not invent a parallel mechanism. The non-yielding C-extension
limit recorded in the error section applies.

**Environment identity.** A fully resolved, hash-pinned `uv.lock` plus an explicit CPython version
hash into the hook's environment identity and enter the compilation manifest. Unpinned `latest`,
mutable virtualenvs, runtime import-path lookup, and network-fetched code remain forbidden. `uv`'s
lock-format stability and cross-platform resolution reproducibility are **unverified** and MUST be
confirmed by recorded evidence before the environment hash is treated as authoritative.

**Capability policy.** Declared and compile-time audited, per the determinism section. CDF MUST NOT
resolve, inject, render, or place secret material into hook inputs or hook identity. Because the
ratified host is in-process, CDF cannot truthfully prevent trusted project code from reading ambient
process state; doing so violates the hook's declared purity and is the accepted residual risk in the
governing decision, not an isolation guarantee.

**Output capture.** `stdout`/`stderr` MUST be captured with a bounded buffer and pass through the
existing redaction path; unbounded capture is forbidden.

**State isolation.** Interpreter reuse across resources and runs MUST NOT leak module-level or
cross-project state into hook execution in a way that could change output for identical declared
inputs.

**Performance.** A hook pass-through roofline MUST be measured against native Arrow pass-through on
the same host with dispersion bounds, following the discipline used by the existing source
rooflines. The observed ratio is ratified and then enforced as a closure gate.

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
- recoverable timeout, protocol violation, or malformed Arrow: typed Environment/Data error
  according to ownership, with hook/runtime provenance;
- hard in-process fault (segfault, allocator abort, OOM kill): the process terminates and no typed
  error is produced. This is a decided case, not a gap — VISION §20.2's chaos layer exercises every
  lifecycle boundary on every merge, and the five-row crash matrix already resolves it: no receipt
  and no finalized package means the transition never happened and is re-planned. A hook MUST NOT be
  able to produce a partial receipt or advance a checkpoint by crashing;
- a hook blocked inside a C extension that never yields to the interpreter is not preemptible; the
  declared timeout does not fire until control returns. Conformance MUST cover the yielding case and
  MUST record the non-yielding case as a known limit rather than asserting a bound it cannot hold;
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
8. Given a Python hook that is not plan-declared first-party project code, compilation rejects it
   before importing or executing the module.
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

## Implementation closure prerequisites

- E1 must choose and compile one concrete SQL/project declaration syntax for the hook fields above.
- Recorded evidence must verify `uv.lock` identity stability for supported hosts before that hash is
  authoritative.
- The measured native/Python pass-through ratio must be ratified before it becomes the performance
  gate. No numeric floor is invented by this specification.

WIT recursive-value work is not an E1 blocker because WASM is explicitly outside the first runtime.

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
