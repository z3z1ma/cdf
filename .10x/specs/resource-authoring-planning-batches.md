Status: active
Created: 2026-07-05
Updated: 2026-07-05

# Resources, authoring, planning, and batches

## Purpose and scope

This specification governs resources, sources, authoring tiers, scan planning, pushdown fidelity, and batch identity/provenance. It derives from book Chapters 7, 8, 9, and 20 and decisions D-1, D-2, D-7, D-8, D-17, D-23, D-25, and D-26.

## Resource model

Every resource MUST implement an Arrow-only stream contract with descriptor, partition planning, and open semantics. Pushdown-capable resources MUST additionally expose capabilities and a no-I/O negotiation method that turns a scan request into a scan plan.

A `ResourceDescriptor` MUST include resource id, schema source, primary key, merge key, cursor, write disposition, contract reference, state scope, optional freshness, and trust level.

`CursorSpec` MUST capture cursor field, ordering claim, and lag tolerance. Inexact ordering or nonzero lag MUST use window-close semantics: committed cursor advances to `max(cursor) - lag`, not the naive maximum.

A `Source` groups shared configuration, credentials, discovery, and defaults. Runtime state, planning, contracts, and conformance MUST remain resource-scoped.

## Capabilities and pushdown

Resource capabilities MUST describe projection, filters, limits, ordering, partitioning, incremental shape, replay support, idempotent reads, backpressure, and estimates. Capability claims MUST be conformance-tested and lockfile-snapshotted.

Pushdown fidelity MUST be expressed per predicate as `Exact`, `Inexact`, or `Unsupported`. `Exact` allows the engine to drop its own filter. `Inexact` means the source returns a superset and the engine MUST reapply the predicate. API resources default to `Inexact` unless exact semantics are proven.

`cdf explain` and `cdf plan` MUST show projected fields, pushed predicates with fidelity, unsupported predicates, limits, partitions, estimates, and the derived delivery guarantee.

## Authoring tiers

Tier 0 declarative resources MUST support REST, SQL, and file shapes through schema-validated TOML/YAML compiled into native resources. The declarative REST surface MUST include base URL, auth by secret URI, rate limit policy, path, params, pagination, record selector, keys, cursor, disposition, contract, and partitioning. Declarative resources SHOULD permit narrow escape hatches to Rust or Python functions for isolated transforms.

Tier 1 Rust resources MUST be statically linked. Dynamic Rust plugins are rejected.

Tier 2 Python MUST be authoring and interchange only. `cdf-python` MAY embed Python, but once data crosses via Arrow PyCapsule/C Data Interface or row batching into Rust, downstream execution is Rust. `cdf-sdk` MUST be typed and `py.typed`; examples MUST be pyright-clean. Semantics MUST be identical on GIL and free-threaded builds, with actual parallelism on free-threaded 3.14t+ where available.

Tier 3 WASM Components MUST target WASI 0.3 with a WIT world exporting `describe`, `negotiate`, and async `open(partition) -> stream<u8>` of Arrow IPC bytes. WASM guests MUST use host-mediated HTTP, secrets, and logs. This tier is post-MVP but its seam MUST be preserved.

Tier 4 subprocess adapters MUST support Arrow IPC and NDJSON at MVP, with Singer and Airbyte as fast-follow. Foreign state MUST map into typed positions where possible and into scoped `ForeignState` where opaque.

## HTTP toolkit

`cdf-http` MUST provide paginators, token-bucket rate limiting, server-header respect, jittered backoff with budget accounting, auth refresh through `SecretProvider`, connection reuse, and preformatted redacted tracing. Declarative, Python, Rust, and WASM authoring surfaces MUST reuse it where applicable.

## Batch model

A batch MUST include id, resource id, partition id, observed schema hash, payload reference/value, row count, byte count, optional source position, optional watermarks, stats, and optional CDC operation metadata.

Rows MUST NOT be an engine runtime concept. Row-shaped authoring crosses into batches at the boundary. Quarantine artifacts MAY remain row-grained because errors are naturally row-specific.

## Acceptance criteria

- A Tier A resource can be implemented without DataFusion concepts and still participates in planning, contracts, packages, and checkpoints.
- A Tier B resource can negotiate projection/filter/limit/partitioning without I/O and render its scan plan.
- Capability conformance falsifies incorrect `Exact` filter claims with adversarial null/timezone/collation cases.
- Python examples type-check and yield identical package hashes on GIL and free-threaded execution when deterministic inputs are fixed.

## Explicit exclusions

This spec does not define destination commit protocols, contract policy details, package layout, or checkpoint commit rules.

