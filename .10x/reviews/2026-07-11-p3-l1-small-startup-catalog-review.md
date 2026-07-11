Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-l1-small-startup-catalog-followup.md
Verdict: pass

# P3 L1 small/startup follow-up review

## Target

Recipe vocabulary, catalog/report fixtures, timed regions, byte authorities, validation, fixed hashes, and tests.

## Findings

Overloading `synthetic_stream` would have hidden the structured Arrow/file generator and made regeneration ambiguous. The new v2 recipe is the smallest honest vocabulary addition and is joined to the actual fixture catalog and generator version in validation.

The first test edit accidentally placed dataset ids in the legacy benchmark-case list; this was caught immediately and corrected before evidence. Review also found that the old report fixture referenced dataset/workload ids absent from its catalog. Those identities are now aligned, and the report/golden/hash changes are explicit.

Tiny and medium are intentionally separate: tiny includes startup/setup to expose fixed overhead, while medium names prepared-input exclusions for throughput. Neither can be confused with the 100 GiB stress recipe. No large bytes, dependency, runtime behavior, or performance claim was added.

No unresolved finding remains.

## Verdict

Pass.

## Residual risk

The current medium fixture is small enough to emphasize fixed costs; that is intentional. L5 must label it fixture evidence and must not extrapolate its absolute rate to TLC/TPC-H scale.
