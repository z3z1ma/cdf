Status: done
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/decisions/datafusion-tier-b-delegation-boundary.md

# Make DataFusion execution metadata honest

## Scope

Update current engine explain/operator metadata so it does not imply that CDF executed real DataFusion `TableProvider` or `ExecutionPlan` nodes while the current path remains CDF-native batch execution.

Owns `crates/cdf-engine/**` and any focused tests or snapshots that assert the operator-chain names.

## Acceptance criteria

- Current CDF-native execution plans no longer serialize operator-chain node names that imply real DataFusion physical execution unless the node actually uses DataFusion.
- Pushdown-fidelity mapping to DataFusion vocabulary remains available and tested.
- Existing explain output still reports pushed, inexact, unsupported, partition, estimate, boundedness, and guarantee details.
- Any naming or schema change is intentionally reflected in tests/evidence and does not claim a completed DataFusion `TableProvider` adapter.
- Future real DataFusion adapter work has a clear place to restore real DataFusion provider/scan node names.

## Evidence expectations

- Focused `cdf-engine` tests showing honest operator metadata for Tier A and Tier B current execution.
- `cargo test -p cdf-engine --locked --no-fail-fast`.
- `cargo clippy -p cdf-engine --all-targets --locked -- -D warnings`.
- `cargo fmt --all -- --check`.
- Review confirming the change is metadata honesty only and does not weaken pushdown or residual filtering semantics.

## Explicit exclusions

No generic `TableProvider` adapter, no dependency changes, no predicate-language expansion, no package format change, no CLI UX redesign, and no source/destination behavior changes.

## References

- `.10x/decisions/datafusion-tier-b-delegation-boundary.md`
- `.10x/research/2026-07-07-datafusion-delegation-pushdown-triage.md`
- `.10x/tickets/done/2026-07-05-datafusion-engine-planner.md`
- `.10x/evidence/2026-07-06-datafusion-engine-planner.md`

## Progress and notes

- 2026-07-07: Opened from triage. Current operator metadata includes `DataFusionTableProvider` and `DataFusionScanExec` names even though execution is a CDF-native Arrow loop.
- 2026-07-07: Activated for worker implementation. The worker owns the narrow metadata-honesty code path and any focused fixture/assertion updates needed because `plan/explain.json` is package evidence.
- 2026-07-07: Completed. Engine operator metadata now serializes current CDF-native execution as `cdf_resource_adapter` and `cdf_native_scan`; DataFusion pushdown fidelity mapping remains intact and tested. Live local-file golden evidence was updated for the intentional `plan/explain.json` hash change.

## Evidence

- `.10x/evidence/2026-07-07-datafusion-execution-honesty.md`
- `.10x/reviews/2026-07-07-datafusion-execution-honesty-review.md`

## Blockers

None.
