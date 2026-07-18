Status: active
Created: 2026-07-06
Updated: 2026-07-07

# CDF Product Objective

CDF is not complete when the Chapter 22 MVP cutline passes. The MVP is a delivery milestone and proof path; the project objective is the full production-grade, next-generation, enterprise-scale data integration framework described by `VISION.md`.

The intended system is optimized for AI agents to manage. That means final designs should prefer explicit artifacts, queryable state, durable evidence, deterministic replay, conformance gates, precise receipts, and low-ambiguity operational surfaces over informal logs, hidden runtime state, manual-only recovery, or connector-specific folklore.

When prioritizing work, MVP acceptance demo slices remain valuable only when they harden reusable production mechanisms. Do not accept demo-only shortcuts as final behavior. Post-MVP book surfaces such as WASM distribution, CDC/streaming, distributed execution, lakehouse/warehouse integration, vault-class secrets, package signing, remote state, and native dependency-policy decisions remain part of full-system completion unless an active superseding record explicitly removes them.

Source/onramp mechanisms must be embeddable infrastructure, not CLI-shaped implementations. Candidate enumeration, discovery, schema reconciliation, parser tiers, and evidence artifacts must remain usable in a standalone container, under remote object stores such as Azure, behind interpreted Python or sandboxed WASM parsing, and eventually inside Spark/Flink-style worker execution. Scaling changes executor topology and budgets; it does not create a second semantic path.

## 1.0 Finish Line

CDF 1.0 is done when the full `VISION.md` system is provable from artifacts rather than asserted:

- Any resource, destination, and disposition composes through one orchestrator and one receipt-verified commit gate, with no specialized runtime path required for correctness.
- A platform team can operate CDF for a year without reading source: nouns are inspectable, failures are recoverable by commands, upgrades migrate serialized artifacts with committed fixtures, and an LTS policy is written.
- Snowflake, BigQuery, Databricks, Iceberg, and Delta each pass the sheet and conformance gates with auditor-grade receipt verification.
- CDC and streaming are resident: Postgres logical replication, MySQL binlog sources, `cdc_apply`, and supervisor drain/pause/resume behavior survive chaos tests.
- Distribution is boring: partition leases with fencing over shared state and remote workers pass existing conformance suites unchanged.
- Third-party connectors are safe: WASM execution, signed registry admission, conformance-gated manifests, and sandbox-denial tests are live.
- Security posture is operational: signed releases, SBOM, cargo-vet policy, vault-class secret providers, redaction adversarial tests, and per-source egress policy exist.
- Performance envelopes are published from maintained benchmarks against relevant alternatives, with bias and environment labeled.
- Agents are first-class operators and authors through machine-readable CLI/MCP surfaces and a conformance-gated connector pipeline.

## Decision Queue

The highest-leverage architectural decisions to front-load are:

- Run model: run-id minting, run-ledger ownership, run-to-package/checkpoint mapping, resume/replay/duplicate semantics, and default id policy.
- DataFusion depth: deepen to real `TableProvider` delegation for applicable resources or ratify the current thin boundary.
- Distributed substrate: Ballista adopt/inspire/reject, shared store choice, and lease/fencing model.
- Warehouse order: BigQuery first is the current recommendation, then Snowflake, then Databricks, unless active design-partner evidence supersedes it.
- Catalog strategy: Iceberg REST catalog as the neutral interface, with Glue, Polaris, and Unity as bindings rather than forks.
- Registry trust model: signing, admission, revocation, and conformance requirements.
- Versioning and LTS: artifact-spec versions, migration fixtures, pin-tuple cadence, and support windows.

## Program Map

Run work as concurrent lanes where crate boundaries and decisions allow it:

- P0, the spine: commit sessions, general orchestrator, run ledger, CLI run/resume/replay/backfill/gc/inspect-run/init.
- P1, contracts and governance: row verdicts, quarantine routing, dedup, variants, trust-ring ledger events, `contract freeze/test`, OpenLineage, retention/GC, signing, SBOM, and provenance.
- P2, sources: Postgres SQL first, then MySQL/SQL Server SQL seams, CDC subprograms, Kafka/queue drain, declarative REST, dlt bridge GA, and generated/agent-authored connector specs.
- P3, destinations: BigQuery, Snowflake, Databricks, Iceberg, Delta, and later Redshift/Fabric as demand pulls.
- P4, runtime and performance: benchmark suite first, then Arrow appender loads, streaming package-to-destination commit, local partition parallelism, memory ledger, byte-bounded backpressure, adaptive batching, and published envelopes.
- P5, resident and distributed: streaming supervisor, remote checkpoint-store conformance, leases/fencing, object-storage packages, and remote workers passing existing suites.
- P6, ecosystem and trust: WIT, Wasmtime host, sandbox-denial tests, registry, signed manifests, Rust/Python guest SDKs.
- P7, enterprise operations and AI-native surface: vault providers, OTLP, k8s/Helm/packages, air-gap install, migration guides, and `cdf` MCP tools over JSON command contracts.

Artifact-spec versioning and distributed fencing tests start before the features they protect; both are throughput multipliers once multiple implementation lanes are active.
