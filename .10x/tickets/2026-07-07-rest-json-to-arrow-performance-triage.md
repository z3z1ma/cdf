Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage REST JSON-to-Arrow performance

## Scope

Investigate the performance envelope of declarative REST execution from HTTP response bytes through JSON record selection and Arrow `RecordBatch` construction.

This ticket is triage only. It does not authorize changing REST semantics, replacing JSON parsing, adding streaming parsers, changing selectors, altering auth/rate-limit behavior, or modifying the REST runtime.

## Current hypothesis

REST extraction is often network/API-bound, so correctness-first JSON DOM parsing may be perfectly adequate for typical pages. However, deterministic tests and future high-volume APIs may expose overhead from `serde_json::Value`, object cloning, per-field coercion, selector validation, and batch construction. CDF should know whether this is a real bottleneck before optimizing.

## Investigation questions

- What page sizes and schemas should CDF expect for Tier 0 declarative REST resources?
- Is runtime cost dominated by transport latency, JSON parse, selector traversal, object cloning, field coercion, Arrow array construction, auth/retry/pagination overhead, or package writes?
- Does current execution decode whole pages before emitting batches, and is that acceptable for large responses?
- Would a streaming JSON parser materially help, or would it complicate selector behavior and error atomicity too much?
- Should REST page size become a planner/resource estimate that informs package batch sizing?
- Does JSON-to-Arrow conversion preserve fail-closed behavior without emitting partial batches if optimized?

## Candidate validation scenarios

- Small API pages with 10-100 rows, network latency simulated by deterministic transport.
- Medium pages with thousands of rows and primitive schema.
- Wide JSON objects with projection limited to a few fields.
- Selector mismatch and schema coercion failures to ensure optimized paths do not leak partial output.
- Cursor-heavy pages where max cursor calculation may add overhead.

## Acceptance criteria

- Classify REST performance as `network-bound`, `parse-bound`, `coercion-bound`, `package-bound`, or `unknown` for representative deterministic fixtures.
- Identify whether current JSON DOM implementation is acceptable for MVP and what workload would force a streaming parser or typed decoder.
- Identify any cheap safe improvements, such as avoiding object clones, preallocating arrays, or projection-aware decoding.
- If implementation is recommended, open focused tickets with explicit fail-closed and no-ambient-network tests.
- Preserve allowlist-before-transport, auth redaction, pagination correctness, cursor source positions, and all current negative error behavior.

## Evidence expectations

- Source inspection of declarative REST execution and `cdf-http` transport/pagination helpers.
- Deterministic in-memory transport measurements if activated.
- Error-path review for selector mismatch, non-JSON response, missing secret, allowlist denial, and schema coercion failure.

## Explicit exclusions

No live API benchmark, no selector-language expansion, no streaming parser implementation, no auth/rate-limit change, no REST `TableProvider`, no package/checkpoint wiring, and no optimization before triage evidence.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/tickets/done/2026-07-07-declarative-rest-resource-execution.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `crates/cdf-declarative/**`
- `crates/cdf-http/**`

## Progress and notes

- 2026-07-07: Opened from performance discussion. Expected default: REST is usually API-bound, but high-volume deterministic JSON fixtures should prove whether `serde_json::Value` plus cloning is good enough.

## Blockers

None for investigation. Implementation is blocked on measured or clearly demonstrated REST decode overhead.
