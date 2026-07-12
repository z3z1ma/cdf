Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-contract/src/vector.rs, crates/cdf-contract/src/vector/tests.rs
Verdict: pass

# Adversarial review: V1 vector validation kernel

## Findings

No critical or significant finding remains. The implementation is engine-neutral and adds no dependency or unsafe code. Schema binding fails closed on drift. Masks retain canonical rule order and dispositions; reject dispositions identify the first violating row; quarantine evidence is generated only for selected rows and reuses the scalar redaction authority.

Review caught three semantic risks before closure: domain parsing initially threatened to reject literals the scalar evaluator treats as impossible matches; float equality initially threatened to merge signed zero and mishandle NaN; and timestamp mask construction initially treated domain nulls like freshness nulls. The final implementation preserves canonical string-domain semantics, uses float bits plus explicit NaN authority, and supplies rule-specific null behavior. A boundless float range also correctly admits NaN because the scalar oracle performs no comparison in that case.

The kernel benchmark counts only inspected range/domain/nullability bytes and clears the target. The full-evaluation comparison uses identical all-pass semantics and includes summaries/mask construction, avoiding an evidence-work mismatch. Packed masks are explicitly bounded at `(rules + accepted + quarantine) * ceil(rows/8)` for the test batch.

## Verdict

Pass. V1 acceptance is met. Production replacement and evidence spill remain V2 scope and are not implied by this verdict.

## Residual risk

V3 must broaden performance runs across density, decimal/nested boundaries, and host classes. Those are program envelope obligations, not defects in the V1 type set admitted by the current scalar evaluator.
