Status: done
Created: 2026-07-18
Updated: 2026-07-18

# WASI 0.3 stream cost and CDF interface model

## Question

What can CDF assert today about a future WASI 0.3 Component Model resource boundary, which WIT revisions are required for semantic parity with the neutral foreign-stream contract, and which performance/security cells must remain unknown until an authorized Wasmtime prototype runs?

## Sources and methods

Inspected the current CDF foreign-stream specification, neutral-boundary decision, failed WIT-foundation ticket and its two reviews, recursive-value projection research, kernel scope/position types, H1/H2/H3 evidence, VISION D-26 and sections 9.5/17/20/25, and the empty `cdf-wasm` crate.

Primary upstream sources inspected on 2026-07-18:

- WASI 0.3 release announcement: <https://bytecodealliance.org/articles/WASI-0.3>
- Component Model WIT reference: <https://component-model.bytecodealliance.org/design/wit.html>
- Component Model Canonical ABI reference: <https://component-model.bytecodealliance.org/advanced/canonical-abi.html>
- Wasmtime Component Model guide: <https://component-model.bytecodealliance.org/running-components/wasmtime.html>
- Wasmtime 46.0.0 release notes: <https://github.com/bytecodealliance/wasmtime/releases/tag/v46.0.0>
- Wasmtime `Config`, `Engine`, `ResourceLimiter`, pooling allocator, component function, stream reader, join handle, and component API documentation as rendered on 2026-07-18: <https://docs.wasmtime.dev/api/wasmtime/struct.Config.html>, <https://docs.wasmtime.dev/api/wasmtime/struct.Engine.html>, <https://docs.wasmtime.dev/api/wasmtime/trait.ResourceLimiter.html>, <https://docs.wasmtime.dev/api/wasmtime/struct.PoolingAllocationConfig.html>, <https://docs.wasmtime.dev/api/wasmtime/component/struct.Func.html>, <https://docs.wasmtime.dev/api/wasmtime/component/struct.StreamReader.html>, <https://docs.wasmtime.dev/api/wasmtime/component/struct.JoinHandle.html>, and <https://docs.wasmtime.dev/api/wasmtime/component/struct.Component.html>. These API URLs float; claims requiring implementation must be rechecked against the selected D-28 tuple.
- Component Model WIT grammar and acyclic-type rule: <https://github.com/WebAssembly/component-model/blob/main/design/mvp/WIT.md>
- Component Model discussion of separate-memory copy limits: <https://github.com/WebAssembly/component-model/issues/398>

No Wasmtime dependency, guest toolchain, WIT artifact, component, generated binding, benchmark, or executable prototype was introduced. The cost worksheet therefore separates architecture facts from measurements and leaves every unmeasured numeric cell unknown.

## Findings

### Upstream maturity boundary

WASI 0.3.0 was ratified on 2026-06-11. Native `async func`, `stream<T>`, and `future<T>` are Canonical ABI primitives; the host runtime owns scheduling. Wasmtime 45 ran a release-candidate ABI; Wasmtime 46 is the first cited release that ships the ratified WASI 0.3.0 tuple and enables component-model async by default. Draft-ABI execution before 46 is not evidence of stable-0.3 compatibility. The current Rust API still labels portions of component-model async support incomplete. CDF therefore has a credible target but not a stable-enough basis for an unmeasured product claim or an independently frozen WIT artifact.

The 0.3 stream terminal model matters directly to CDF. Upstream WASI interfaces commonly pair a stream with a future that resolves independently of how much data the consumer reads. A bare `stream<u8>` does not by itself communicate CDF's final success/failure and checkpoint authority to a consumer that stops early. The WIT foundation's original `open(partition) -> stream<u8>` shape is therefore plausible for bulk bytes but insufficient as the whole neutral producer outcome.

### Established requirements and unresolved interface choices

The future `cdf:resource` package must express one semantic boundary, not a WASM-only runtime. The following requirements are established by active authority:

1. **Resolvable, grammar-valid package.** Reserved identifiers use WIT escaping; every imported package/interface resolves under one pinned WIT/WASI/Wasmtime tuple; real parser and binding-generation gates replace substring tests.
2. **Faithful descriptor and plan.** `describe` exposes the current resource descriptor and neutral producer capabilities. `negotiate` accepts the governed scan request and returns the governed scan plan. Both return the shared typed resource error rather than inventing a mandatory connector code. Capability claims, schema source, scope, ordering, delivery guarantee, estimates, retry timing, deduplication, and contract positions cannot be omitted silently.
3. **Typed recursive control values.** Composite scope and source position need a separately ratified lossless projection. Current research recommends a canonical rooted arena because WIT value definitions are acyclic. Its canonical form, limits, allocation accounting, and error semantics remain unresolved.
4. **Arbitrary transfer chunks are not semantic chunks.** Guest writes and host reads may split or coalesce bytes arbitrarily. Batch identity comes only from the Arrow IPC decoder. No WIT chunk, Canonical ABI transfer, or host poll becomes package identity.
5. **Host imports are real broker contracts.** Empty `cdf:host/http`, `secrets`, and `log` interfaces are not a capability system. Concrete interfaces must govern request/response limits, streaming, redirects, timeouts, cancellation, secret-use without disclosure, errors, redaction, and rate/egress policy before publication.
6. **Schema and reconciliation stay outside the guest.** The guest declares/observes physical data and emits IPC/control facts. CDF's compiled stream-admission plan remains the sole authority for normalization, coercion, residual capture, quarantine, and checkpoint gating.

The active authorities do not settle how one high-throughput byte stream also exposes ordered typed control and an independently observable terminal status. D-26 and VISION section 9.5 explicitly sketch `open(partition) -> stream<u8>`; replacing that result with a record or event stream would supersede that shape. Three candidates remain:

- keep the bare byte stream and add a separately keyed lifecycle/control API, which introduces session identity and cross-call races;
- return distinct IPC, control, and completion handles, which preserves the specialized bulk path but requires a complete cross-stream ordering, boundedness, fairness, completion, abandonment, and cancellation state machine; or
- return one typed event stream, which supplies total order but may add list/variant allocations and copies to the bulk path.

The second candidate remains the performance-oriented recommendation for prototyping, not semantic authority:

```text
open(partition) -> {
  ipc: stream<u8>,
  control: stream<sequenced-control-event>,
  completion: future<result<open-completion, resource-error>>,
}
```

It is not exact until authority settles initial sequence, gaps, equal offsets, future-offset bounds, IPC/control EOS order, completion-before/after-drain, partial-consumer abandonment, fairness, and checkpoint meaning. `open-completion` may report the guest's final observed physical position/state but cannot itself make that state governed or committed.

### Current cancellation reality

Current Wasmtime does not make ordinary Rust drop equivalent to structured component cancellation. Dropping a component call future does not cancel the guest; documented hard cancellation may require dropping the Store. A `StreamReader` must be explicitly closed or its store-side representation can remain live and its writer can hang. A component `JoinHandle` requires explicit abort; dropping it alone does not abort the task. Therefore CDF cannot yet claim that dropping any returned stream/future cancels `open`.

The future host needs an explicit lifecycle authority that concurrently drains or closes every handle, explicitly aborts/cancels the component task where the selected tuple supports it, bounds the grace interval, and tears down the Store if hard cancellation is required. Intentional early consumer stop, successful EOS, protocol failure, host cancellation, guest trap/OOM, blocked host call, and partial handle abandonment are distinct states. Exactly-once release and no successful receipt/checkpoint after failure are executable conformance laws, not WIT prose.

### Cost worksheet

| Cell | Current status | Supported statement | Required future measurement |
|---|---|---|---|
| Dependency/build footprint | unknown | Wasmtime is optional/post-MVP and must remain outside default builds until admitted | clean build time, incremental build time, binary size, dependency/audit delta |
| Cold validation/compile | unknown | Wasmtime can compile components; compilation must not occur per partition | source component to compiled artifact wall/CPU/RSS, artifact size, concurrency scaling |
| AOT load | unknown | `Engine::precompile_component` can move translation/codegen out of the hot path; compatibility is engine-config/host dependent and unsafe deserialization requires trusted artifact provenance | safe compile path, authenticated cache provenance, tamper/incompatibility rejection, verified-load latency/RSS, cache hit/miss behavior |
| Instance creation | unknown | on-demand is default; pooling can improve repeated/high-concurrency instantiation but carries reserved-memory and platform tradeoffs | on-demand versus pooling p50/p95/p99, cold/warm, concurrency curve, virtual/RSS footprint |
| TLS first-call setup | known qualitative, numeric unknown | Wasmtime documents one-time per-OS-thread setup that may be hundreds of microseconds; it can be eagerly initialized | first-call versus warmed-call distribution on every admitted host class |
| Small host calls | unknown | canonical lifting/lowering and async scheduling are not free | calls/s and latency by payload shape/size for log, secret-use, HTTP control calls |
| Byte-stream crossing | one-copy lower bound; exact cost unknown | separate guest/host memories preclude a general zero-copy claim; Wasmtime 46 added `Bytes`/`BytesMut` component lowering, but a stream-specific allocation/copy advantage is unproved | direct pinned stream prototype, copy bytes/allocations/cycles and throughput across 1 KiB–32 MiB transfer windows |
| Guest IPC encode | unknown | guest must produce one valid Arrow IPC stream; no native-equivalent claim exists | per-language encoder throughput, compression modes, allocation/RSS, batch curve |
| Host IPC decode | unknown for WASM path | CDF's native incremental decoder is reusable after the boundary | same decoder benchmark with arbitrary WASM transfer fragmentation and control offsets |
| End-to-end stream | unknown | compare separately from native, Python C Data, subprocess IPC, and row compatibility | first batch, steady bytes/rows per second/core, package hash, managed/native peak |
| Epoch interruption | known qualitative, numeric unknown | Wasmtime documents epoch interruption as lighter than fuel and safe against malicious guest avoidance; it is nondeterministic | throughput tax, cancellation latency, timer cadence sensitivity |
| Fuel | known qualitative, numeric unknown | deterministic instruction accounting is explicitly more expensive and incompatible with Winch | tax by workload and fuel interval; retain only as an explicit policy knob |
| Component memory | partially bounded | `ResourceLimiter` limits guest memories/tables/instances but explicitly not all Store or embedder memory | guest linear peak, Store metadata/native peak, lifted control values, host buffers, async stacks, task/handle quotas, ledger reconciliation |
| Pool reservation/isolation | configurable, numeric unknown | pooling exposes instance/memory/table/stack ceilings and resident-memory knobs; retained/reused pages require tenant-isolation evidence | throughput/RSS/virtual-memory, zeroing cost, fresh/reused instances, cross-tenant remanence on every platform |
| Capability mediation | unknown | no ambient capabilities is the default; broker call costs, limits, deadlines, cancellation, and HTTP overlap are unmeasured | request/stream throughput, outstanding-call/body/log quotas, redirect/deadline behavior, connection reuse, secret/log overhead, denied-call latency |
| Cancellation/drop | unknown and currently non-structural | current Wasmtime requires explicit close/abort and may require Store teardown for hard cancellation | cancellation at compile/instantiate/host-call/stream/guest-loop boundaries, every partial-handle state, exact release and teardown latency |

### Performance posture

The future default should be measured, not doctrinal:

- Precompiled component artifacts and a reused engine are the intended production path; compilation is an admission/cache operation.
- Epoch interruption is the candidate normal safety path because upstream explicitly describes fuel as more expensive. Fuel remains an opt-in deterministic quota knob until evidence justifies another default.
- On-demand versus pooling allocation is a host/profile knob selected from the measured concurrency and resident-memory envelope; neither is hard-coded as universally faster.
- Transfer-window, guest batch, instance concurrency, pool, memory, epoch, and fuel values are named configuration authorities with hardware-aware defaults. No unexplained performance cap belongs in the WIT.
- `stream<u8>` remains the bulk-path candidate. Typed control cannot force a per-row host call, and every per-batch/cross-stream alternative must first measure its actual lowering and copy behavior.
- WASM is the untrusted distribution tier. It need not beat the native or Python C Data path, but every avoidable copy, allocation, compile, and scheduling tax must be measured and removed before a performance claim.

### Later executable benchmark matrix

An authorized Wasmtime implementation ticket is not ready to claim support until it runs one pinned tuple on the EC2 benchmark host and records:

1. cold component validation/compile and AOT artifact production;
2. warm AOT load and on-demand/pooling instance creation at concurrency 1, physical cores, and oversubscribed;
3. time to `open`, first IPC batch, and terminal completion;
4. 1 KiB, 64 KiB, 1 MiB, 8 MiB, and 32 MiB transfer windows over identical uncompressed and LZ4 IPC payloads;
5. throughput/cycles/allocations/copies for native Arrow production, H3 subprocess IPC, and WASM IPC on the same rows and batches;
6. guest linear memory, Store/native metadata, host IPC buffers, CDF ledger peak, and process RSS separately;
7. epoch-disabled/epoch-enabled and fuel-disabled/fuel-enabled curves rather than one opaque sandbox number;
8. brokered HTTP streaming overlap, secret-use, and structured logging with body/call/log quotas, redirects, deadlines, cancellation, and denied-capability controls;
9. package/manifest equality under arbitrary 1-byte, prime-sized, randomized, and coalesced transport chunks;
10. the ratified ordering state machine: initial/duplicate/gapped/equal/future offsets, malformed controls, each IPC/control EOS and completion ordering, slow/flooded control versus IPC, and fairness without deadlock;
11. cancellation during guest CPU loops, host calls, blocked stream writes, intentional early stop, partial handle abandonment, explicit stream close, explicit task abort, Store teardown, trap, and OOM;
12. authenticated AOT-cache load plus corrupt, tampered, wrong-version, wrong-config, and wrong-host rejection before unsafe deserialization;
13. fresh/reused pooled instances with stack/linear-memory zeroing and cross-tenant secret/data remanence probes; and
14. no surviving task/handle/memory lease and no receipt/checkpoint advancement after every failure injection.

These are measurement criteria, not preselected throughput thresholds. Retention requires that the chosen defaults are on the local Pareto frontier for throughput, latency, and bounded memory, and that every more conservative security policy is an explicit knob with its measured cost.

## Conclusions

H4 does not need an implemented WIT file to produce its cost-model result. The failed foundation plus current WASI 0.3/Wasmtime evidence is sufficient to identify the semantic questions and an honest measurement matrix. Requiring the incomplete artifact as an upstream dependency inverted the design flow and encouraged freezing an interface before its lifecycle and costs were understood.

WASI 0.3 is now real enough to preserve the seam and immature enough that all numeric performance cells remain unknown. H4 is not ready to claim exact WIT revisions: D-26's bare-stream shape conflicts with the recommended neutral lifecycle, current Wasmtime cancellation is not drop-structured, recursive control values and broker calls remain unresolved, and the ordering/completion state machine is unratified. A later executable host must use the already-existing neutral foreign producer boundary rather than create a second runtime API.

## Limits

- No generated bindings or WIT parser validated the explanatory shape.
- No Wasmtime/guest binary was built or run, so every numeric cell remains unknown.
- The cross-stream byte-offset ordering scheme is an unratified performance-oriented candidate, not a required revision or serialized artifact contract.
- Wasmtime and guest-toolchain support is moving quickly; the exact tuple must be reverified at implementation time under D-28.
