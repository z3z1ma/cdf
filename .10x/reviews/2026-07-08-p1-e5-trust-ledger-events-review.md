Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-e5-trust-ring-ledger-events.md
Verdict: pass

# P1 E5 trust-ring ledger events review

## Target

P1 E5 implementation and closure records for trust-ring validation-depth promotion and demotion ledger events.

## Assumptions tested

- Anomaly demotion must not invent a detector or thresholds.
- Ledger events are evidence only and must not alter checkpoint commit authority.
- Existing drift and quarantine demotion behavior must remain intact.
- Inspect/run detail rendering must remain redaction-safe.
- Adding a field to `ValidationProgram` must not break cross-crate manual constructors or old serialized program compatibility.

## Findings

No blocking findings.

Minor residual risk: explicit anomaly facts currently enter through an empty-by-default validation-program field as the MVP explicit-signal seam. This keeps E5 scoped and avoids an invented detector, but a future `ProfileExec` or anomaly-detector producer should move facts into package profile/evidence before product-facing anomaly detection grows.

Minor residual risk: jscpd still reports broad pre-existing duplication in `runtime_tests.rs` and contract test helpers. This slice reduced the new trust-ring request repetition and records `newClones = 0`; broad test-suite dedup is outside E5.

## Verdict

Pass. E5 satisfies the active ticket and `.10x/decisions/contract-anomaly-signal-demotion-policy.md`: anomaly demotion is explicit-signal-only, ledger details include the required fact fields, existing demotion triggers are preserved, checkpoint state still advances only through the receipt gate, and quality gates passed after the cross-crate conformance constructor repair.

## Residual risk

E6 drift-quarantine conformance remains required before Workstream E and P0 can close. Production anomaly detection remains intentionally unimplemented.
