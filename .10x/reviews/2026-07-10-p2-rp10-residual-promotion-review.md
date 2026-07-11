Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: RP10 residual capture/promotion implementation and conformance evidence
Verdict: pass

# RP10 adversarial review

## Findings

No critical or high-severity finding remains.

The review found and resolved one critical semantic defect before closure: runtime effective-schema preparation expanded a sampled pin with compatible unseen fields. The format reader therefore treated those fields as projected truth, while the immutable compiled output later omitted them. This could silently drop values. The repaired path reconciles each physical observation against the pinned baseline, disables parse coercion for structural observation, leaves safe extras outside the projection for residual capture, and quarantines incompatible types. The integrated preview/run/promotion scenario proves the values survive and become governed columns.

The review also checked that residual observability did not fork execution semantics. `residual_row_count` is computed from the same post-contract bounded preview batches and is reporting-only. Correction behavior remains behind destination protocol/runtime traits, promotion packages remain replay authority, and GC availability remains separate from retention action.

## Verdict

Pass. Residual capture is a pinned-schema contract event rather than silent schema mutation, the same evidence survives columnar and row-oriented formats, and promotion remains explicit, deterministic, destination-neutral, and crash-safe.

## Residual risk

None within RP10 scope. Remaining remote/cloud and complete S1-S8 matrix cells retain their existing WS-I owner and were not relabeled by this closure.
