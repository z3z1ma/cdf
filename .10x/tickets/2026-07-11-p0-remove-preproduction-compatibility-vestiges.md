Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: None
Depends-On: None

# P0: Remove pre-production compatibility vestiges

## Scope

Apply `.10x/decisions/pre-production-current-format-only.md` repository-wide. Enumerate and delete old artifact readers/migrations, deprecated CLI aliases and parser prepasses, compatibility-only API defaults/helpers, dormant destination/source fallbacks, and tests/fixtures whose only purpose is preserving a superseded CDF snapshot. Preserve current external protocol interoperability and benchmark-only before/after controls.

## Acceptance criteria

- Current package, schema snapshot, discovery manifest, dedup, event, sheet, and checkpoint writers/readers expose one canonical version each; older CDF artifact versions fail as unsupported without migration readers.
- CLI grammar and Rust APIs contain no deprecated aliases or compatibility shims.
- Destination/source capability sheets advertise only live current implementations.
- `rg`-based inventory classifies every compatibility/legacy occurrence as removed, current external-protocol interoperability, benchmark control, or non-compatibility English usage.
- Workspace current-format tests, strict lint, artifact goldens, and gitleaks pass with a leaner build graph.

## Evidence expectations

Before/after inventory, deleted-line/build-target counts, current-format golden tests, focused protocol tests, workspace check/lint, and adversarial review for accidentally removed current external interoperability.

## Explicit exclusions

No compatibility promise for the first production release is introduced here. No external protocol dialect is removed merely because that protocol names it legacy.

## Blockers

None.

## References

- `.10x/decisions/pre-production-current-format-only.md`
