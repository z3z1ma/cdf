Status: active
Created: 2026-07-05
Updated: 2026-07-06

# cdf book decision register

## Context

`VISION.md` Chapter 3 contains D-1 through D-28 and states that the Decision Register wins when it disagrees with another chapter. This record preserves the implementation-relevant choices so the repository can eventually remove the book without losing authority.

## Decision

The following decisions govern implementation unless superseded by a later active decision:

| ID | Decision | Revisit trigger |
|---|---|---|
| D-1 | Resources implement an Arrow-only `ResourceStream`; pushdown-capable resources additionally implement `QueryableResource`, wrapped by `cdf-engine` as DataFusion providers. DataFusion is mandatory in the engine and invisible to basic authorship. | Many first-party resources hand-roll pushdown the engine could negotiate. |
| D-2 | Authoring tiers are declarative TOML/YAML, statically linked Rust, embedded Python, WASM Components, and subprocess adapters. | Refined by D-25 and D-26. |
| D-3 | Quarantine is a framework side channel, not a DataFusion multi-output plan. | DataFusion gains first-class multi-output plans. |
| D-4 | Canonical package data is LZ4-framed Arrow IPC; Parquet is archive/interchange/analytics. Manifests and receipts are canonical JSON. | Arrow IPC forward compatibility across arrow-rs majors fails in practice. |
| D-5 | State lives behind `CheckpointStore`; SQLite WAL and in-memory stores ship first; destinations mirror load facts where possible. | Multi-writer local state becomes common before distributed mode. |
| D-6 | Contracts default to `evolve`; `freeze` is explicit; trust presets may make stricter policy easy. | Telemetry shows `evolve` causes silent-drift incidents. |
| D-7 | Pushdown fidelity is per predicate: `Exact`, `Inexact`, or `Unsupported`; API resources default to `Inexact`. | Never; this follows DataFusion's production model. |
| D-8 | Bounded and unbounded resources share `BatchStream`; unbounded plans require checkpoint cadence, rotation, and watermark policy; MVP supports drain mode. | Log-CDC demand outruns the streaming milestone. |
| D-9 | Only in-flight per-batch schema-stable transforms live in cdf; post-load modeling belongs downstream. | Users consistently smuggle post-load SQL into resources. |
| D-10 | Packages are hash-addressed now and signature-ready now; actual signing is post-MVP. | Compliance requires signatures at MVP. |
| D-11 | Distributed execution arrives after local correctness and conformance on three sources and three destinations. | Milestone after conformance. |
| D-12 | Tokio multi-threaded runtime with separated I/O, CPU, and bounded blocking/FFI pools. | Runtime substrate changes upstream. |
| D-13 | One memory ledger extends DataFusion `MemoryPool` to cdf buffers; budget exhaustion goes flush, backpressure, spill, clean failure. | The ordering proves insufficient under conformance or stress. |
| D-14 | Source names are preserved in metadata; `namecase-v1` derives destination identifiers; collisions are plan-time hard errors. | Normalizer version must change. |
| D-15 | Arrow is the closed canonical type system plus metadata annotations for semantics, source-name provenance, and nullability provenance. | Arrow no longer represents a needed source type faithfully. |
| D-16 | `cdc_apply` uses `_cdf_op` plus source positions; MVP handles deletion-aware merge for cursor sources; log CDC is post-MVP. | CDC demand requires earlier log-source support. |
| D-17 | One error taxonomy drives retry at the smallest safe unit under a run-level retry budget; `cdf-http` supplies pagination, limits, backoff, and auth refresh. | Taxonomy fails to classify real failures cleanly. |
| D-18 | Artifacts contain secret references only; runtime resolution uses `SecretProvider` and redaction-aware zeroizing wrappers. | Secret-provider surface proves insufficient. |
| D-19 | `tracing` plus optional OTLP; primary observability is queryable artifacts through `cdf sql`, `doctor`, `inspect`, and `status`. | Operators need a first-party UI; the kernel still must not depend on it. |
| D-20 | Scheduling stays out of kernel; `run --loop` exists only for local development. | Kernel consumers cannot integrate scheduling without unsafe workarounds. |
| D-21 | Testing rests on resource/destination conformance, chaos kills at lifecycle boundaries, and golden-package determinism. | A supported class of defect escapes all three pillars. |
| D-22 | Apache-2.0, one repo, crates on crates.io, serialized artifacts independently versioned with migrations. | Governance changes by explicit decision. |
| D-23 | Python is authoring and interchange only, never the execution substrate; PyO3 is optional and outside the kernel. | Python must host semantics not expressible in Rust. |
| D-24 | Project name and artifact prefixes are `cdf`, `cdf-`, `cdf-sdk`, `cdf:resource`, `.cdf/`, and `_cdf_*`. | Naming conflict or legal blocker. |
| D-25 | Python semantics must be correct on GIL builds and parallel on free-threaded 3.14t+ builds. | Free-threaded Python becomes the ecosystem default. |
| D-26 | WASM Components target WASI 0.3 as baseline; WIT uses native async streams. | WASI 1.0 interface freeze. |
| D-27 | Iceberg and Delta are destinations, not package formats; their transaction metadata appears inside cdf receipts. | A design partner needs Iceberg at MVP. |
| D-28 | Each cdf minor pins one arrow-rs/DataFusion/object_store/duckdb-rs tuple; upgrades are deliberate and golden-package-gated; patch releases do not move load-bearing dependency pins. | Upstream breaks serialized-artifact compatibility. |

## Consequences

Implementation may use examples in the book as normative for semantics and shape, but not as an excuse to overbuild beyond the chosen milestone. Residual open items named by the book remain revisit triggers rather than blockers: D-6 telemetry, whether `ForeignState` needs a dedicated migration tool, and the future default flip for free-threaded Python.

Later active decisions refine this register where explicitly stated. `.10x/decisions/native-arrow-datafusion-parquet-policy.md` refines D-1, D-4, and D-28 for Parquet surfaces: DuckDB-backed Parquet is now a temporary workaround, and native Arrow/DataFusion Parquet is the ratified target even though it requires a narrow temporary `RUSTSEC-2024-0436` exception.
