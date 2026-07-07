Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/decisions/arrow-datafusion-tuple-policy.md, .10x/decisions/datafusion-git-pin-arrow59-tuple.md, .10x/tickets/done/2026-07-07-arrow-datafusion-dependency-tuple-alignment.md

# Evaluate the DuckDB Arrow 58 transitive residual

## Scope

Evaluate and decide what CDF should do about the remaining Arrow `58.3.0` dependency path introduced by `duckdb 1.10504.0` after DataFusion itself has been aligned to Arrow `59.1.0`.

This ticket owns investigation and, if ratified by evidence, a bounded remediation plan. It does not authorize changing DuckDB destination behavior or replacing `duckdb-rs` by itself.

## Acceptance criteria

- Identify whether current or newer `duckdb-rs` releases can avoid the Arrow 58 dependency while preserving the CDF DuckDB destination contract.
- Decide whether D-28's "one tuple" policy applies to private destination-driver transitive dependencies that do not cross the CDF Arrow public API boundary.
- If remediation is available and low-risk, open a bounded implementation ticket with golden/package/destination evidence expectations.
- If remediation is not available or not worth the churn yet, record an explicit no-action or temporary-acceptance rationale with a revisit trigger.
- Keep the DataFusion TableProvider adapter unblocked unless evidence shows the DuckDB residual affects engine execution or lower-layer public APIs.

## Evidence expectations

- `cargo tree --workspace --locked -i arrow-array@58.3.0` evidence for the current residual.
- Registry or source evidence for candidate `duckdb-rs` versions and feature flags.
- Focused destination risk assessment for DuckDB package commit, replay, and receipt verification behavior.
- Supply-chain evidence if dependency versions change.

## Explicit exclusions

No DataFusion tuple changes, no Arrow-major bridge in the engine hot path, no DuckDB destination rewrite, no package format change, and no weakening of `.10x/decisions/datafusion-git-pin-arrow59-tuple.md`.

## References

- `.10x/evidence/2026-07-07-datafusion-git-pin-arrow59-tuple.md`
- `.10x/reviews/2026-07-07-datafusion-git-pin-arrow59-tuple.md`
- `.10x/decisions/arrow-datafusion-tuple-policy.md`
- `.10x/decisions/datafusion-git-pin-arrow59-tuple.md`
- `.10x/tickets/done/2026-07-05-duckdb-destination.md`

## Progress and notes

- 2026-07-07: Opened during closure of the DataFusion git-pin tuple work. The DataFusion engine tuple is aligned to Arrow `59.1.0`, but `cargo tree --workspace --locked -i arrow-array@58.3.0` still finds `duckdb 1.10504.0 -> arrow 58.3.0`.

## Blockers

None for investigation.
