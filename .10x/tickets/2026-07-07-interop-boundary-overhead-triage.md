Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage Python, WASM, and subprocess interop overhead

## Scope

Investigate the performance and memory overhead of CDF's non-native authoring and adapter boundaries: Python SDK/interchange, WASM component seam, subprocess adapters, Singer/Airbyte compatibility, Arrow IPC exchange, NDJSON fallback, and foreign state handling.

This ticket is triage only. It does not authorize implementing or changing interop protocols, adding benchmarks, changing Python/WASM/subprocess APIs, or optimizing adapters.

## Current hypothesis

CDF's best performance story depends on crossing into Rust/Arrow batches quickly and avoiding per-row Python or JSON work in the hot path. Python authoring, WASM components, and subprocess adapters are valuable for ecosystem compatibility, but their overhead must be measured and documented honestly so CDF does not promise native-speed execution through row-shaped foreign boundaries.

## Investigation questions

- Which current interop paths are Arrow IPC/native Arrow and which are NDJSON or row-shaped?
- What copy/serialization boundaries exist for Python, subprocess, and future WASM components?
- Does `cdf-python` use PyCapsule/C Data Interface paths in the current code or primarily Arrow IPC serialization?
- What are the expected costs for startup, schema exchange, batch transfer, and foreign state conversion?
- When should CDF recommend native Rust/declarative resources over Python/subprocess adapters for performance-sensitive workloads?
- What performance assertions should be added to docs or destination/resource sheets to avoid misleading claims?

## Candidate validation scenarios

- Python produces Arrow batches to Rust package execution.
- Subprocess adapter streams Arrow IPC batches.
- Subprocess adapter streams NDJSON rows for compatibility.
- Singer/Airbyte adapter-style extraction with foreign state.
- Future WASM component exchange, treated as design review unless an implementation exists.

## Acceptance criteria

- Inventory implemented interop boundaries and classify each as Arrow-native, Arrow IPC, JSON/NDJSON row-shaped, or not implemented.
- Identify startup overhead, per-batch overhead, and per-row overhead risks.
- Recommend no action, documentation, benchmark harness, protocol change, or separate implementation ticket.
- If implementation is recommended, split by boundary: Python, subprocess, Singer/Airbyte, or WASM.
- Preserve the active rule that row-shaped authoring crosses into batches at the boundary and rows do not become an engine runtime concept.

## Evidence expectations

- Source inspection of `crates/cdf-python/**`, `crates/cdf-subprocess/**`, `crates/cdf-formats/**`, and any Singer/Airbyte adapter code.
- Protocol notes identifying where copies or serialization occur.
- Optional small local timing sketches if deterministic fixtures already exist.

## Explicit exclusions

No Python API change, no WASM implementation, no subprocess protocol change, no Singer/Airbyte feature work, no dependency additions, no benchmark harness, and no optimization before triage closes.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/tickets/done/2026-07-05-python-sdk-bridge.md`
- `.10x/tickets/done/2026-07-05-formats-and-subprocess.md`
- `.10x/tickets/done/2026-07-06-singer-airbyte-protocol-adapters.md`
- `crates/cdf-python/**`
- `crates/cdf-subprocess/**`
- `crates/cdf-formats/**`

## Progress and notes

- 2026-07-07: Opened from performance discussion. CDF should be able to say clearly where it is Arrow-native fast and where compatibility boundaries impose overhead.
- 2026-07-11: Source audit completed in `.10x/research/2026-07-11-foreign-interop-boundary-audit.md`. Python and subprocess are eager/materialized despite partial Arrow/bounded primitives; WASM is unimplemented. IX1 and P3 H1–H5 now absorb contract, measurement, Python, subprocess, prospective WASM, and closeout work. This triage remains open until H5 records final evidence and moves it terminal; it owns no implementation.
- 2026-07-11: WS-L has no foreign-boundary copy-proof cell; the absence is explicit in `.10x/evidence/2026-07-11-p3-l5-preoptimization-baseline.md` and makes H1-H5 measurement work mandatory. No zero-copy or throughput claim is inferred.

## Blockers

None for investigation. Any protocol or implementation change is blocked on boundary-specific triage evidence.
