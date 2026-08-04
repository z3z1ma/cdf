Status: open
Created: 2026-08-03
Updated: 2026-08-03
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

# D0 remove Postgres-owned merge-dedup policy

## Scope

Remove the active code/config/CLI/replay special case that lets PostgreSQL own or accept
`merge_dedup`. Package construction remains winner authority; destinations retain unconditional
duplicate-finalized-key rejection as corruption/safety behavior.

## Non-goals

- package-native delete effects;
- changing the ratified package winner rules;
- redesigning unrelated adapter policy values;
- adding a compatibility alias or ignored CLI/config field.

## Acceptance criteria

1. `PostgresDestinationPolicy`, `PostgresMergeDedupPolicy`, and the first-class Postgres field in
   project `DestinationPolicy` are removed; generic adapter policy remains only for real adapter
   execution choices.
2. Run, replay, and state-recovery CLI surfaces accept no `--merge-dedup`, and JSON/human reports do
   not expose it as caller authority.
3. Postgres planning requires no policy value to plan append/replace/merge.
4. A finalized merge input with duplicate keys fails as corrupted/invalid input before target
   mutation; Postgres never selects first/last.
5. Package construction and replay retain their recorded winner authority unchanged.
6. Examples, generated CLI artifacts, docs, fixtures, and focused tests contain no current
   `merge_dedup` contract. Historical/superseded 10x evidence remains historical.
7. Formatting, `git diff --check`, focused project/CLI/runtime/Postgres tests, and targeted strict
   Clippy pass without a whole-workspace suite.

## References

- `.10x/decisions/destination-introspection-package-and-current-replay-policy.md`
- `.10x/decisions/state-current-schema-and-effect-package-recovery.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/research/2026-08-03-project-compiler-authority-inventory.md`

## Assumptions

- Current-format-only removal and package-owned winner selection are active user-ratified
  authority.

## Journal

- 2026-08-03: Opened from confirmed D0 source/record drift. No product code changed in this shaping
  turn.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending focused review with the D foundation boundary.

## Retrospective

Pending execution.
