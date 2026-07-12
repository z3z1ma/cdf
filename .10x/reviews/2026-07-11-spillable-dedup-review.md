Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/decisions/spillable-package-order-dedup.md, .10x/specs/spillable-package-dedup.md, .10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md
Verdict: pass

# Spillable dedup shaping review

## Findings

No critical or significant shaping issue remains. The design preserves exact semantics under spill, resolves the exact-row placement drift, refuses hash collisions as authority, bounds provenance, and leaves algorithm selection to evidence without leaving behavioral ambiguity.

## Verdict

Pass after L5/A2/A3 dependencies.

## Residual risk

Complete Arrow equality is subtle, especially NaN, map ordering, dictionary arrays, and extension metadata. A6 must first freeze a reference semantic matrix and fail planning for unsupported encodings; it cannot silently equate canonicalized JSON or display strings.
