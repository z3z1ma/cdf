Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Foreign interop boundary audit

## Question

What is implemented at the Python/subprocess/WASM boundaries, where are copies/materializations/runtime leaks, and what neutral contract lets interpreted/custom code participate in the same terabyte-scale pipeline without misleading native-speed claims?

## Sources and methods

Inspected the P3 WS-H/triage tickets, authoring/runtime/memory/source-extension specs, Python bridge/channel/evidence, subprocess runner/protocols, WASM crate/WIT tickets, and package/format streaming plans. Traced producer-to-Arrow ownership, buffering, supervision, state, and failure paths.

## Findings

Python recognizes `__arrow_c_array__`/`__arrow_c_stream__` and imports through `pyo3-arrow`, which is the correct ABI direction. However `import_arrow_stream` returns `Vec<RecordBatch>`, `PythonBatchRead` stores `Vec<Batch>`, dict rows collect/serialize through JSON, mixed schemas hard-fail locally, and the standalone `BoundaryChannel` is exercised in tests but is not the live bridge transport. Existing evidence explicitly did not run a real PyArrow capsule locally. `zero_copy_intent: true` is intent, not measured proof.

Subprocess uses Tokio `Command` but immediately `wait_with_output`, fully buffering stdout and stderr. Arrow IPC/NDJSON decoding begins only after successful exit and returns materialized `FormatRead`. Timeout/error supervision exists, but process-group termination, memory-budget placement, streaming backpressure, bounded stderr draining, startup/handshake, incremental foreign state, and mid-stream failure semantics are absent. Singer/Airbyte parsers are also byte-slice/row-shaped compatibility paths.

The WASM crate is documentation-only. A WIT foundation ticket proposes async `stream<u8>` Arrow IPC, which is directionally compatible, but no executable cost can be claimed. Arbitrary byte chunks require a bounded incremental IPC framer, and host-call/copy/fuel/memory/compile/startup costs need a modeled benchmark scaffold rather than fabricated measurements.

All three boundaries need the same semantic output: incremental accounted Arrow outcomes with source/partition/sequence/schema/state provenance, cancellation, backpressure, and terminal status. They differ in physical crossing: in-process Arrow C Data Interface ownership transfer, framed Arrow IPC bytes across process/sandbox, or row-shaped compatibility that must convert immediately at the boundary. This belongs under the neutral source/runtime contracts, not in engine match trees.

Zero-copy must have a falsifiable meaning. For Python it means Arrow payload buffers cross by borrowed/transferred C Data Interface ownership without payload memcpy; schema wrappers/reference-count operations do not disqualify it. Tests must hold source objects alive/then release them, verify buffers remain valid, compare underlying buffer addresses where supported, and use allocation/copy instrumentation plus throughput. Unsupported types/conversions must report a copy rather than retain the label.

Foreign producers are memory/runtime authorities too. Python pull iteration, subprocess pipe reads, and future WASM stream polling must stop when the accounted downstream lease is unavailable. Child processes run under the process-tree budget. GIL/free-threaded/internal-thread behavior is an execution-host lane capability, not Python-name branching in generic orchestration.

## Conclusion

Define one neutral foreign-stream boundary contract and measurement taxonomy. Implement Python as an incremental C-stream/row-fallback producer and subprocess as concurrently supervised streaming stdout/stderr/control. Measure startup, steady-state, copy count/bytes, CPU, RSS, and boundary efficiency separately by physical mode. Produce only a prospective WASM cost-model harness/interface review in P3; Tier 3 execution remains later.

## Limits

No real PyArrow/free-threaded timing or WASM runtime measurement was run during shaping. Exact protocol/version choices and thresholds are evidence-selected under WS-L/H1.
