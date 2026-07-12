Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-format-json, crates/cdf-source-files/src/runtime.rs, crates/cdf-project/src/schema_discovery.rs, crates/cdf-cli/src/source_registry.rs
Verdict: concerns

# B5 streaming NDJSON driver review

## Findings

No critical or high finding blocks this slice.

The data path is genuinely incremental: input chunks keep source leases, the tape decoder consumes partial records, and batches publish one at a time with retained output leases. The driver crate contains no project, CLI, source, destination, DataFusion, or engine dependency. The old source fallback now rejects NDJSON, so production has one path.

### Significant follow-up — native decoder peak versus output lease

The driver reserves target output bytes before feeding Arrow JSON, but a variable-width row can produce a retained batch larger than the target. `AccountedPhysicalBatch` fails closed after observation, yet Arrow may already have allocated the larger native buffer. B5 must add oversized-row admission/fuzz/RSS evidence and either preflight row bounds, grow through an allocator-aware pool, or spill/fail before unbounded native growth.

### Significant follow-up — row-local quarantine parity

The superseded monolithic NDJSON reader had specialized row-local filtering/residual behavior. The new physical driver leaves schema reconciliation to the shared layer, but malformed/type-drift row localization must be proven through the ordinary pre-contract side channel before B5 closure.

## Verdict

Concerns, accepted for the bounded migration slice. Both findings remain inside open B5 acceptance and are not legacy-path justifications.

## Residual risk

Throughput, malformed-input fail-closed behavior across arbitrary chunking, and constant RSS are not yet evidenced.
