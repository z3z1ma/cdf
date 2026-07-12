Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Depends-On: .10x/specs/typed-statistics-evidence.md, .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-v2-validation-graph-integration.md

# P3 J0: typed statistics evidence spine

## Scope

Replace the unpopulated lexical `BatchStats` and aggregate-only profile artifact with the CDF-native typed statistic model, vectorized batch computation, deterministic segment/package aggregation, canonical `stats/profile.parquet`, and verified streaming readers required by J1.

## Acceptance criteria

- Supported primitive/decimal/temporal/string/binary columns produce exact typed min/max/null/row evidence; unsupported or ambiguous cases explicitly remain incomplete.
- Computation is vectorized, memory-accounted, segment-streaming, jobs-invariant, and measured against the P3 overhead budget.
- `stats/profile.parquet` is deterministic, manifest-bound, corruption-checked, and readable without payload access.
- Lexical fields and aggregate `profile.json` are deleted with no dual representation or compatibility shim.
- Kernel/package APIs remain DataFusion-free and adding a destination/source/format requires no statistics-specific branch.

## Evidence expectations

Generated Arrow type/density arrays, scalar round-trip/property tests, jobs/golden packages, corruption/schema-generation adversaries, large-package RSS, primitive/wide/nested benchmarks, strict dependency checks, and adversarial review.

## Explicit exclusions

No predicate evaluation, DataFusion type, pruning decision, anomaly policy, or exact unbounded distinct set.

## Blockers

None. A5/V2 provide the fused streaming stage.

## References

- `.10x/specs/typed-statistics-evidence.md`
- `.10x/research/2026-07-12-datafusion-pruning-evidence-readiness-audit.md`
- `.10x/specs/datafusion-currency-bridges.md`
