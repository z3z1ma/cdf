Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: B7/F2 working-tree implementation and .10x/tickets/done/2026-07-09-p2-ws-b7-rest-observed-reconciliation.md, .10x/tickets/done/2026-07-09-p2-ws-f2-s7-key-disposition-experience.md
Verdict: pass

# P2 B7 and F2 adversarial integration review

## Findings

The first review failed B7 closure on one critical finding: `RestRuntimeDependencies::with_type_policy` could grant parse/lossy authority at runtime without binding the choice into the compiled plan or package identity. That builder and policy field were removed from production dependencies; REST now executes strict reconciliation, and new regressions prove runtime defaults cannot mint policy verdicts.

F2 passed its initial review. Its failures occur before source contact or writes, command/resource wording is preserved across validate/plan/preview/run, append stays keyless and silent, and protective merge behavior remains intact.

The final targeted re-review passed with no findings. Parent-observed integration verification then passed all 773 workspace tests and the applicable formatting, lint, documentation, dependency-policy, advisory, and secret-scan profiles.

## Assumptions tested

- REST response materialization cannot bypass observed-first reconciliation.
- Page-to-page reconciliation-plan changes fail closed.
- Runtime connectors cannot grant semantic coercion authority after compilation.
- Unauthorized parse/lossy cases cannot become package evidence.
- Quarantined non-cursor drift cannot cause already-observed cursor rows to refetch indefinitely.
- Append does not imply a key; merge does.
- Merge-key errors precede source/destination mutation and provide exactly the two ratified remedies.

## Verdict

Pass. The critical policy-authority defect was repaired and regression-tested. B7 and F2 may close within their explicit exclusions.

## Residual risk

A future compiled type-policy surface requires separate ratification and identity coverage; G2 owns that unresolved checkpoint. Exact-row keyless dedup and the standalone S7 conformance scenario remain separately owned and are not closure findings for F2.
