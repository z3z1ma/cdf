Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 B9: read-only spreadsheet codecs

## Scope

Add XLSX/XLS/XLSB/ODS workbook drivers with sheet units, pinned ranges/header/formula/merged-cell policies, workbook date systems, and strict no-execution security.

## Acceptance criteria

- Macros/formulas/links/connections never execute or contact external systems.
- Cached formula absence, date epochs, merged/hidden cells, types, empty termination, and selected sheet order match catalog semantics.
- Large sheets decode with bounded memory or explicit seek/spool capability; preview/run match.
- Native reference correctness/performance and hostile workbook tests are recorded.

## Evidence expectations

Dependency/security review, representative office corpora, formula/date/merge goldens, zip-bomb/malformed tests, memory, and profiles.

## Explicit exclusions

No workbook writing, recalculation, macros, charts, or styling extraction.

## Blockers

Depends on FX1 and L5.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
