Status: active
Created: 2026-07-05
Updated: 2026-07-05

# Architecture, layering, and runtime

## Purpose and scope

This specification governs firn's crate/layer boundaries and runtime behavior. It derives from the book's Chapters 4 and 5 and decisions D-1, D-12, D-13, D-20, and D-28.

## Layer rules

firn MUST preserve strict lower-to-upper dependency direction.

Layer 1, the kernel, MUST define core types, traits, state-machine meaning, artifact schemas, receipts, checkpoints, and contract-facing values using arrow-rs types. It MUST NOT depend on DataFusion, DuckDB, Python, network clients, destination drivers, or project/CLI code.

Layer 2, the engine, MUST own planning and execution through DataFusion: table-provider adaptation, expression evaluation, physical operators, projection/filter/limit negotiation, and explain output. It MUST enforce kernel decisions but MUST NOT redefine run meaning.

Layer 3, extensions, MUST contain authoring tiers, destinations, formats, HTTP tooling, and secret providers. Extension crates MAY depend on kernel and engine where appropriate, but MUST NOT be required by the kernel.

Layer 4, project and product, MUST contain `firn.toml`, environments, lockfile handling, CLI, `doctor`, `status`, and user-facing orchestration. Nothing with a screen is load-bearing.

## Crate map

The workspace SHOULD include crates corresponding to these responsibilities: `firn-kernel`, `firn-engine`, `firn-contract`, `firn-package`, `firn-state-sqlite`, `firn-http`, `firn-formats`, `firn-declarative`, `firn-python`, `firn-wasm`, `firn-subprocess`, `firn-dest-duckdb`, `firn-dest-parquet`, `firn-dest-postgres`, `firn-project`, `firn-cli`, and `firn-conformance`.

MVP implementation MAY stub or feature-gate post-MVP crates when a ticket explicitly scopes them, but public boundaries MUST leave the designed seam.

## Runtime behavior

firn MUST use Tokio multi-threaded execution with distinct resource classes for I/O, CPU-heavy work, and bounded blocking/FFI pools. Blocking DuckDB and Python work MUST be confined to bounded blocking pools.

Every channel carrying batch data MUST be byte-bounded using batch byte accounting, not merely message-count-bounded. Backpressure MUST propagate from slow destinations toward resources. Resources that cannot pause MUST declare that fact; plans for those resources MUST require spill policy.

firn MUST maintain one memory accounting story by extending DataFusion's `MemoryPool` accounting to package builders, adapter decode buffers, destination staging, and other firn buffers. Budget exhaustion MUST attempt, in order: early segment flush, backpressure, spill, clean failure. Surprise OOM is not an accepted behavior.

Batch size MUST be adaptive between configured floors and ceilings. Live extraction MAY adjust batch sizes based on downstream pressure and spill, but replay MUST use recorded batch boundaries and MUST NOT rederive adaptive decisions.

Every plan node MUST carry boundedness. Bounded plans run to completion. Unbounded plans MUST be illegal unless they declare checkpoint cadence, package rotation, and watermark strategy. MVP supports unbounded plans only in drain mode.

## Acceptance criteria

- Kernel public APIs expose no DataFusion, DuckDB, Python, or network types.
- A dependency graph check can prove lower layers do not import upper layers.
- Runtime tests demonstrate byte-bounded backpressure and clean failure under a small memory budget.
- Boundedness policy rejects an unbounded plan without cadence, rotation, and watermark strategy.

## Explicit exclusions

This spec does not define resource descriptors, package layout, checkpoint schema, CLI UX, or conformance suites except where they enforce layer/runtime constraints.

