Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Neutral foreign stream boundary

## Context

Python, subprocess, and future WASM custom code must join CDF's ordinary source/runtime calculus without private batch collections, unbounded child output, engine-specific branches, or inflated zero-copy/native-speed claims.

## Decision

CDF defines an executor-neutral foreign producer contract beneath source drivers and above physical language/process hosts. It yields an asynchronous/pullable stream of accounted Arrow outcome envelopes plus bounded control events and one terminal status. Outcomes carry canonical source/partition/local sequence, schema/position/watermark/state authority, payload lease/ownership, physical transfer mode, and copy telemetry. Generic planning/runtime sees source capabilities and ordinary outcomes, never `Python`, `subprocess`, or `WASM` variants.

Physical transfer modes are:

- `arrow_c_data`: in-process C Data Interface/C Stream ownership transfer; zero-copy eligible;
- `arrow_ipc_stream`: framed incremental IPC across process/sandbox; serialization/copying explicit;
- `row_compat`: dict/NDJSON/protocol rows converted in bounded windows at the boundary; never a downstream row runtime.

“Zero-copy” is asserted only per yielded batch when payload buffer ownership crosses without a payload memcpy and a conformance probe proves lifetime/address/allocation behavior for that type path. Copy count and bytes are telemetry; unsupported conversions downgrade honestly. IPC and row modes are never labeled zero-copy end to end.

Backpressure is pull/pipe/stream-poll based and shares global memory permits. Each host declares execution lane, internal concurrency/scratch, cancellation/interrupt safety, startup/handshake, and child-budget behavior through neutral capabilities. Subprocess stdout, stderr, control/state, and lifecycle are drained concurrently; bounded diagnostic rings never gate stdout. Nonzero exit or protocol failure prevents the current epoch/package from gating even if prior batches were staged.

Python and subprocess implement the contract in P3. WASM P3 work defines/validates the cost/interface model only; later Wasmtime execution must implement the same contract. Foreign state is typed/control-plane data and cannot hide in stderr or payload metadata.

## Alternatives considered

- Separate bespoke runtime APIs per tier: rejected because memory, cancellation, schema, and conformance would diverge.
- Force every tier through IPC: rejected because it would discard Python C Data Interface zero-copy.
- Expose rows as a general engine input: rejected because it destroys vectorization and multiplies semantics.
- Claim zero-copy from use of `pyo3-arrow`: rejected because adapters may convert/copy and current evidence did not execute real PyArrow.
- Implement Wasmtime in P3 to get a number: rejected by program scope and because an interface/cost model is sufficient to preserve the seam.

## Consequences

IX1 creates the neutral boundary in the structural layer. H1 adds comparable measurement. H2 and H3 replace eager Python/subprocess paths. H4 records the WASM model. H5 closes the matrix and claims. Source/format reconciliation remains the sole schema truth after crossing.
