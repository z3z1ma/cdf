Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Foreign stream interop

## Purpose and scope

This specification governs neutral foreign producer capabilities/outcomes, Python C Data/row crossing, subprocess IPC/row crossing, supervision, state/control, memory/cancellation, copy taxonomy, measurement, schema reconciliation, diagnostics, and the prospective WASM cost/interface model.

## Neutral contract

The contract MUST contain no PyO3, Tokio, OS-process, Wasmtime, DataFusion, or CLI type. A producer descriptor declares transfer modes, pause/backpressure support, schema/position/watermark/state behavior, lane/internal concurrency, startup model, cancellation/interrupt safety, native/child memory, protocol/version, and security capabilities. A producer yields ordinary accounted physical outcomes compatible with the source/format graph.

The boundary MUST be incremental. Public production APIs cannot return `Vec<Batch>`, `Vec<RecordBatch>`, whole stdout/stderr, or whole foreign streams. A compatibility collector MAY exist only outside production paths/tests under a declared cap.

Schema changes pass through shared discovery/reconciliation/contract policy; the boundary MUST NOT invent a competing “one schema per read” rule. Fatal protocol/window errors publish no successful checkpoint; row-local recoverable errors use ordinary quarantine/residual verdicts.

## Ownership, memory, and backpressure

Every admitted payload and decoder window holds a global memory lease until consumer ownership transfers/releases. A foreign C Data candidate whose retained size is not knowable before import remains producer-native and pre-admission: it MUST emit no outcome unless it fits the reserved payload window, and oversized candidates are dropped. Foreign-native scratch and transient pre-admission retention are calibrated headroom; children inherit/enforce process-tree budgets. Control/state/stderr/progress queues are separately bounded and cannot deadlock payload draining.

Cancellation is structured: stop admission/polling, request cooperative termination, drain/reap within a bounded grace period, then force termination where authorized. Subprocess process groups/descendants cannot survive the run. Python exceptions/panics and child signals/exits produce redacted typed errors and no receipt/checkpoint advancement.

## Python

Arrow C Array/Stream MUST be pulled incrementally and preserve producer-owned buffer lifetime according to the Arrow C Data Interface. Conformance covers real PyArrow arrays/tables/readers when available, slices/nulls/nested/dictionary/decimal/timezone/large buffers, release ordering, exceptions between batches, mixed schemas, GIL and free-threaded hosts, and cancellation/backpressure.

Dict/row yields MUST accumulate only one adaptive/accounted conversion window and convert directly to Arrow without serializing the full resource. Row fallback is labeled compatibility and its cost is reported separately. Arbitrary Python objects are rejected at the boundary.

## Subprocess and protocol adapters

The host MUST concurrently read incremental stdout frames, bounded/redacted stderr lines, and typed control/state where the protocol provides it while observing exit/timeout/cancellation. Arrow IPC stream is preferred. NDJSON/Singer/Airbyte compatibility decodes bounded windows and emits batches immediately; schema/state messages remain ordered control facts.

The OS pipe supplies backpressure but the host still accounts read/framing/decoder buffers and sets maximum frame/record/stderr/line sizes. Startup handshake identifies protocol/version/schema/capabilities when required. Partial/truncated IPC, malformed records, stderr floods, child stalls, forked descendants, nonzero exit after data, and state-after-data are conformance cases.

## Copy and performance evidence

Each batch records transfer mode and measured/known copy classification: `payload_zero_copy_verified`, `payload_copy_known(bytes)`, or `copy_unknown`. Zero-copy verification requires supported underlying-buffer identity/lifetime probes and allocation/copy instrumentation; wrapper/schema allocation is separately reported. Marketing/docs may aggregate only verified cells.

The lab measures cold/warm startup, handshake, time-to-first-batch, steady-state bytes/rows per second and per core, batch-size curve, CPU cycles, allocations/copies, boundary queue waits, RSS/native/child peak, cancellation latency, and slowdown versus equivalent native Arrow production. Arrow C, IPC, and row modes are separate benchmark cells with host/interpreter/protocol versions.

## WASM prospective model

P3 MUST validate that the WIT stream can express arbitrary-chunk incremental Arrow IPC, typed descriptor/plan/errors, cancellation, host-mediated capabilities, and bounded control/state. It MUST model compile/instantiate/startup, host-call, memory copy, IPC encode/decode, fuel/epoch interruption, and sandbox memory costs from named Wasmtime/reference evidence or a clearly labeled local prototype only when authorized. Unknown cells remain unknown. No Tier-3 throughput claim is permitted before executable conformance.

## Conformance and diagnostics

All implemented foreign hosts pass one shared mock-producer suite for streaming, schema drift, positions/watermarks/state, backpressure, cancellation, resource limits, redaction, fatal/row errors, jobs determinism, and package/receipt/checkpoint behavior. `plan`/`explain` renders transfer/copy and lane capabilities; run reports actual mode/copies/throughput/limits without secrets or raw command environments.

## Explicit exclusions

No general plugin ABI, dynamic Rust loading, Wasmtime host implementation, registry/signing, ambient guest I/O, engine rows, or native-equivalent claim for row compatibility is specified here.
