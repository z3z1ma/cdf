Status: open
Created: 2026-07-07
Updated: 2026-07-07

# Triage CDF performance investigation backlog

## Scope

Create a durable performance-investigation backlog for CDF based on the current architecture and early code shape: Arrow-native batches, DataFusion-centered planning/query delegation, deterministic packages, receipt-gated destinations, declarative REST execution, Python/WASM/subprocess boundaries, and local-first execution.

This is a parent planning ticket. It is not executable implementation scope and does not ratify any optimization by itself. Each child ticket is an investigation/triage owner that must first validate whether the suspected bottleneck matters, identify the relevant workload, and recommend one of: no action, documentation only, benchmark harness work, specification/decision update, or a separate bounded implementation ticket.

## Child tickets

- `.10x/tickets/2026-07-07-performance-baseline-benchmark-suite.md`
- `.10x/tickets/2026-07-07-local-partition-parallelism-triage.md`
- `.10x/tickets/2026-07-07-package-io-hashing-overhead-triage.md`
- `.10x/tickets/2026-07-07-streaming-package-to-destination-commit-triage.md`
- `.10x/tickets/2026-07-07-duckdb-arrow-bulk-load-triage.md`
- `.10x/tickets/2026-07-07-native-parquet-streaming-write-triage.md`
- `.10x/tickets/done/2026-07-07-datafusion-delegation-pushdown-triage.md`
- `.10x/tickets/2026-07-07-rest-json-to-arrow-performance-triage.md`
- `.10x/tickets/2026-07-07-batch-sizing-segment-coalescing-triage.md`
- `.10x/tickets/2026-07-07-interop-boundary-overhead-triage.md`

## Triage principles

- Do not optimize against intuition alone. Every child ticket must begin by identifying the workload class, current code path, expected cost center, and evidence needed to prove the issue is material.
- Do not degrade CDF's governing contracts for speed without an explicit decision or specification change. Package identity, receipt-gated checkpoint commits, Arrow-only runtime batches, destination receipt verification, conformance, and no-ambient-network boundaries remain active constraints.
- Compare fairly against inspirations. DuckDB, DataFusion, Polars, Spark/Flink, dbt/Dagster, Airbyte/Singer, and custom Arrow Rust code each optimize for different workload envelopes. A benchmark that favors one system's native mode must name that bias.
- Separate measurement from remediation. Triage may produce evidence, research, and recommendations; production code changes require a separate executable ticket with ratified acceptance criteria.
- Preserve local developer ergonomics. Long-running performance suites should be opt-in or target-specific unless the project later ratifies them as release gates.

## Acceptance criteria

- Each child ticket has a clear investigation question, suspected code paths, benchmark or inspection approach, explicit non-goals, and a decision gate before implementation.
- This parent can be closed only after every child is closed, cancelled with rationale, superseded by a narrower owner, or intentionally deferred in an active record.
- Any final performance recommendation mentioned in closure has a durable owner: an evidence record, research record, decision/spec update, new implementation ticket, or no-action rationale.

## Evidence expectations

When this parent is eventually closed, include a rollup record summarizing:

- Which workload envelopes CDF is expected to perform well in.
- Which inspiration comparisons are evidence-backed and which remain hypotheses.
- Which optimizations were rejected or deferred and why.
- Which implementation tickets were opened, if any.
- Which benchmarks are suitable for recurring quality or release gates.

## Explicit exclusions

No source edits, dependency changes, benchmark harness implementation, CI changes, profiling-tool installation, optimization work, destination rewrite, package format change, parallel execution change, or public performance claim is authorized by this parent alone.

## References

- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/specs/destination-receipts-guarantees.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/decisions/native-arrow-datafusion-parquet-policy.md`
- `.10x/knowledge/cdf-product-objective.md`

## Progress and notes

- 2026-07-07: Opened as a side-conversation backlog after a qualitative performance discussion. The backlog intentionally captures questions for later validation rather than authorizing immediate optimization.
- 2026-07-07: DataFusion delegation triage closed in `.10x/tickets/done/2026-07-07-datafusion-delegation-pushdown-triage.md`. It produced active decision `.10x/decisions/datafusion-tier-b-delegation-boundary.md`, blocked tuple owner `.10x/tickets/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md`, blocked adapter owner `.10x/tickets/2026-07-07-datafusion-tableprovider-adapter.md`, and open metadata-honesty owner `.10x/tickets/2026-07-07-datafusion-execution-honesty.md`.

## Blockers

None for opening the investigation backlog. Individual child tickets must validate their own workload relevance before any implementation recommendation.
