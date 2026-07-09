Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
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
- 2026-07-08: Split first executable child `.10x/tickets/2026-07-08-p2-ws-f1-append-default-merge-key-error.md` for append default and merge-key validation in declarative compilation.

## Blockers

None.
