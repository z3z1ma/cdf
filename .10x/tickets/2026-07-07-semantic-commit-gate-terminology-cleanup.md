Status: blocked
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-07-mechanical-cdf-identity-rename.md

# Semantic commit-gate terminology cleanup

## Scope

Review mechanically transformed "CDF line" terminology against `VISION.md` and update source, `.10x/` records, CLI output, tests, and specifications to the ratified semantic vocabulary.

Candidate vocabulary from `VISION.md`:

- "commit gate" is the central state-advancement invariant.
- "guarantee line" appears in the MVP demo around `cdf plan` output.

## Acceptance Criteria

- Exact mapping for the former line metaphor is user-ratified or record-backed.
- Source, tests, CLI strings, specs, tickets, evidence, reviews, and knowledge records use the ratified terms consistently.
- Any renamed spec/ticket paths keep references coherent.
- Residual scan for the mechanically transformed line phrase returns no unintended matches.
- Relevant Rust tests and `QUALITY.md` fast gates pass.

## Evidence Expectations

Record the semantic mapping, path/reference rewrite, residual terminology scans, focused tests for changed CLI/source behavior, and closure review.

## Explicit Exclusions

No behavior changes beyond terminology unless a governing spec is superseded first.

## References

- `VISION.md` Preface, D-24, Chapter 13, and MVP demo section.
- `.10x/tickets/done/2026-07-07-mechanical-cdf-identity-rename.md`
- `.10x/evidence/2026-07-07-mechanical-cdf-identity-rename.md`

## Progress and Notes

- 2026-07-07: Opened during mechanical CDF identity rename closure because the mechanical pass intentionally did not choose the semantic replacement for the former line metaphor.

## Blockers

The exact mapping is unclear: implementation would invent whether each occurrence should become "commit gate", "guarantee line", or another CDF term.
