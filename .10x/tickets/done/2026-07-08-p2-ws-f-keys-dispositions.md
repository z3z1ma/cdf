Status: done
Created: 2026-07-08
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/data-onramp-source-experience-cli.md

# P2 WS-F keys and dispositions

## Scope

Make key/disposition behavior match reality: append is default and keyless, merge requires an explicit merge identity, key suggestions come only from evidence, and exact-row dedup remains available as a separate keyless option.

Split executable child tickets before code for compiler defaults, plan errors, scaffold changes, docs/error-message sweep, and dedup option handling.

## Acceptance criteria

- Append resources validate, plan, and run without primary or merge keys and without warnings nudging fake keys.
- Merge without a merge key fails once at plan time with exact remediation.
- `cdf add` suggests keys only when discovery evidence supports a clearly labeled suggestion.
- Existing docs, examples, scaffolds, and errors stop implying append needs keys.
- Keyless exact-row dedup is available only through an explicit ratified option.

## Evidence expectations

Plan/validate/run tests for append and merge, CLI snapshots, docs/example scans, source-message regression tests, and S7 conformance evidence.

## Explicit exclusions

This ticket does not implement SCD2 or new destination disposition families.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-08: Split first executable child `.10x/tickets/done/2026-07-08-p2-ws-f1-append-default-merge-key-error.md` for append default and merge-key validation in declarative compilation.
- 2026-07-09: F1 closed in `.10x/tickets/done/2026-07-08-p2-ws-f1-append-default-merge-key-error.md`; declarative compilation now defaults append keylessly, merge requires explicit `merge_key`, and the local append scaffold omits key fields.
- 2026-07-09: Split executable child, now terminal at `.10x/tickets/done/2026-07-09-p2-ws-f2-s7-key-disposition-experience.md`, for exact validate/plan/preview/run S7 behavior, command-correct merge remediation, and current scaffold/message/example audit. Exact-row dedup remains outside this child pending ratification.
- 2026-07-09: F2 closed with `.10x/evidence/2026-07-09-p2-b7-f2-integration.md` and `.10x/reviews/2026-07-09-p2-b7-f2-integration-review.md`. Append is silent and keyless across validate/plan/preview/run, and merge-without-key fails once before source or project mutation with both fixes. WS-F remains open for explicitly named exact-row dedup semantics and WS-I's standalone S7 golden-path scenario.
- 2026-07-10: F3 closed as `.10x/tickets/done/2026-07-10-p2-ws-f3-keyless-exact-row-dedup.md`. Explicit append-only exact-row dedup now compiles into the generic validation program, uses typed Arrow row identity across the full schema, retains first package order, and records deterministic package evidence without inventing keys or destination behavior.
- 2026-07-10: Workstream closed with keyless append, exact merge remediation, evidence-only suggestions, explicit typed exact-row append dedup, and standalone S7 conformance.

## Blockers

None.
