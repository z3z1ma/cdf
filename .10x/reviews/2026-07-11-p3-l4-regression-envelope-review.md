Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-10-p3-ws-l4-ci-envelope-generation.md
Verdict: pass

# P3 L4 baseline-reset and envelope review

## Target

Comparison semantics, baseline persistence, envelope generator/golden, documentation claims, workflow routing, tests, and evidence.

## Findings

The first policy draft contained a critical logical error: exact full comparability-key equality included CDF revision, which would make cross-revision regression testing impossible. It was immediately superseded. V2 records revision as the intentional delta while holding all environmental/data/reference authority exact.

Review then found three significant evidence risks and resolved them:

- a matching key could still compare different row/byte work; report validation now requires stable work per sample and comparator/envelope refuse cross-report work mismatch;
- a content-addressed report alone did not protect a tampered baseline index/history; every install now revalidates every indexed report digest, safe path, evidence reference, and current pointer before atomic replacement;
- generic debug rendering made the generated envelope toolchain-format-dependent; host CPU/memory/storage render through explicit stable formatters.

CI remains deliberately lean: no performance job was added to pull-request fast checks, benchmark work moved out of the already broad slow workflow, and the dedicated performance workflow is scheduled/manual. Gitleaks uses the same source-only boundary as CI. The README links only the generated envelope and states its pre-baseline no-claim status.

No critical, significant, or minor unresolved finding remains within L4.

## Verdict

Pass.

## Residual risk

GitHub-hosted runner hardware may drift, but host-class mismatch makes comparisons inconclusive. The full-year/constant-memory targets remain unavailable until L5 and later runtime work produce honest cells; the generator displays those absences rather than implying green. Baseline durability uses file and directory sync on the supported macOS/Linux lab platforms and has not been power-loss tested.
