Status: done
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
- 2026-07-07: Investigation completed in `.10x/evidence/2026-07-07-duckdb-arrow58-residual-audit.md`. `cargo search duckdb --limit 10` and `cargo info duckdb` report `duckdb 1.10504.0` as latest/current; registry source shows `duckdb-rs` has an unconditional Arrow `58` dependency with `prettyprint` and `ffi`, so no current feature/version path removes Arrow 58.
- 2026-07-07: Audited CDF DuckDB commit, replay, duplicate receipt, and receipt verification boundaries. CDF package replay enters the destination as Arrow 59 `RecordBatch` values from `cdf-package`, then `crates/cdf-dest-duckdb/src/package.rs` and `rows.rs` lower schemas and arrays to DuckDB SQL type strings and `duckdb::types::Value`. Commit and merge use row appenders/SQL; receipts and mirror verification use JSON and primitive rows. No Arrow 58 structs cross into CDF public Arrow 59 APIs.
- 2026-07-07: Recorded temporary acceptance/no-action decision in `.10x/decisions/duckdb-arrow58-private-driver-residual.md`. No implementation follow-up ticket was opened because current remediation options require upstream change, fork, or driver rewrite rather than a bounded low-risk change.
- 2026-07-07: Supply-chain gates passed without `deny.toml` or `supply-chain/config.toml` changes: `cargo deny check` passed with duplicate warnings and all checks ok; `cargo vet --locked` passed.
- 2026-07-07: Parent orchestrator repaired references after review and moved this ticket to `done/`. The residual is accepted temporarily by `.10x/decisions/duckdb-arrow58-private-driver-residual.md`; no implementation owner remains open until a revisit trigger fires.

## Blockers

None. Revisit triggers live in `.10x/decisions/duckdb-arrow58-private-driver-residual.md`.
