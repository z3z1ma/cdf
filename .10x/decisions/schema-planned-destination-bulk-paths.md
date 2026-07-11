Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Schema-planned destination bulk paths

## Context

Destination sessions exist behind a common protocol, but all current implementations materialize segments and/or scalarize rows. Bulk declarations are not a shared scheduling contract, and implementation can contradict the sheet. P3 must make adding a destination automatically participate in bounded streaming, tuning, type fallback, evidence, and conformance.

## Decision

`cdf-runtime` defines neutral bulk-writer vocabulary at segment/batch granularity. Each destination driver exposes one or more `BulkPathDescriptor`s with stable path id/semantic version and:

- accepted Arrow type/mapping conditions and exact/widening/lossy/unsupported preconditions;
- finalized-only versus staged-ingress mode and visibility/rollback/resume guarantees;
- input shape (durable segment reader/accounted batches), ordering requirements, writer concurrency, and useful batch/byte ranges;
- I/O, CPU, or declared blocking/affinity lane needs and native internal parallelism;
- transaction/multipart/staging external-storage behavior and CDF-resident memory working set;
- preflight support, restart/fallback rules, row/count/checksum acknowledgements, and measured evidence version.

The destination driver prepares the eligible path ladder after the shared semantic schema-to-sheet mapping is compiled. Generic orchestration does not interpret destination types or path ids. Unsupported fields name the field, Arrow/destination type, mapping fidelity, and allowed semantic fixes before mutation.

Physical path and tuning choice do not enter package identity when all eligible paths implement the same compiled mapping, order, disposition, and receipt semantics. The selected path id/version/settings, eligibility rationale, fallback/restart, bytes/rows, and timings are recorded in nonidentity run evidence and durable destination receipt details sufficient for verification/audit. Capability sheet/hash remains ordinary plan/lock authority.

Fallback is preflight-first. A driver may choose a compatibility path per schema before mutation. After any payload is accepted, switching path is legal only by aborting/rolling back the entire attempt, proving no target visibility, and redriving under a new attempt; silent mid-load fallback is forbidden. Lossy fallback still requires the existing semantic allowance and cannot be created by a runtime error.

The hot driver boundary consumes one bounded durable segment stream and then accounted batches; no `Vec<CommitSegment>`, package-sized row vector, or generic scalar row representation is permitted in production paths. Vectorized/native paths are default. Scalar fallback must be truthful, schema-scoped, measured, and never selected where an exact bulk path is available.

Destination tuning joins runtime memory/CPU/jobs and source pressure. The driver supplies safe ranges/default evidence; the execution host resolves effective concurrency and batch sizes. A destination can be single-writer while upstream remains parallel. Backpressure and staging limits flow through the common graph.

## Alternatives considered

- Hard-code DuckDB/Postgres/Parquet strategies in runtime: rejected because every new destination would edit orchestration.
- One universal Arrow Flight/ADBC writer: rejected as the only path because native transaction/idempotency/mapping/receipt semantics vary; such protocols can be drivers.
- Catch errors and fall back row-by-row: rejected because partial mutation and lossy behavior become nondeterministic.
- Put physical path choice in package identity: rejected because host/library tuning would churn otherwise identical source evidence and jobs goldens.
- Keep sheet strings informational: rejected because false declarations cannot govern scheduling or conformance.

## Consequences

Destination sheets gain machine-checked bulk descriptors and conformance falsifies them. `CommitSegment` evolves to bounded readers/envelopes under A5. Each first-party destination implements and measures its own strategy without generic branches. Receipt schemas may gain versioned optional physical-load evidence while their package/segment/idempotency verification meaning remains unchanged.
