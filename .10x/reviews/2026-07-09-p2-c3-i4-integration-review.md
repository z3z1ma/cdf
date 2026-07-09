Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: C3/I4 working-tree implementation and .10x/tickets/done/2026-07-09-p2-ws-c3-live-destination-normalization-duckdb-postgres.md, .10x/tickets/done/2026-07-09-p2-ws-i4-s5-s7-standalone-conformance.md
Verdict: pass

# P2 C3 and I4 adversarial integration review

## Findings

I4's first review found one minor self-containment gap: S7 proved zero contact but did not independently prove a missing secret was not resolved. The fixture now uses a deliberately absent secret, asserts its absence, and scans the full error envelope. Parallel execution also exposed a single-read HTTP fixture race; bounded header capture now reads through the HTTP terminator. The combined P2 filter passes 8/8.

C3's first review failed closure on one significant policy-authority flaw: a plan could copy the destination `IdentifierPolicy` while retaining column outputs compiled under a different policy. The repair recomputes normalization from the current resource schema and serialized policy, verifies normalizer version and every ordered source/output mapping, preserves the destination-policy equality guard, and fails before writes. Conformance helpers now compile the full program rather than relabel stale evidence. Adversarial spoof/version tests and legacy-serde coverage passed. Final independent re-review passed.

Parent integration initially found four legacy conformance planners still using the default policy; all shared builders were repaired, deterministic golden identities were refreshed from verified repeats, and the final 781-test workspace run passed.

## Assumptions tested

- Pinned resource schema identity is not replaced by destination-normalized output identity.
- Destination policy parameters are executable identity, not descriptive labels.
- Preview, package execution, and destination planning apply the same output names.
- Source-name metadata can restore DuckDB-unbounded identifiers and drive Postgres truncation without losing provenance.
- Collisions, stale versions, stale mappings, and destination-policy mismatches fail before package, state, or destination writes.
- S5/S7 promotion is backed by standalone operator-path conformance rather than registry references.

## Verdict

Pass. All actionable findings were repaired and independently re-reviewed. C3 and I4 may close within their explicit exclusions.

## Residual risk

Direct low-level engine callers are trusted internal plan constructors today, so normalization coherence is enforced at the project live-run boundary. This is an explicit no-action boundary, not an unowned defect: if CDF exposes arbitrary external `EnginePlan` execution, the coherence validator must move into the engine boundary before that surface is supported. Parquet column normalization remains an explicit WS-C semantic checkpoint.
