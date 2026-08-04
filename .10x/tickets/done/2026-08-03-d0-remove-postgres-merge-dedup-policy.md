Status: done
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
- 2026-08-03: Execution started after C1 closed. Inventory found the policy in the project model,
  Postgres runtime/load plan, replay and recovery CLI grammar, destination runtime capabilities,
  generated CLI artifacts, docs, fixtures, and focused tests. Success reports do not serialize the
  value, so their typed JSON/human authority needs no replacement field.
- 2026-08-03: Removed the typed Postgres project policy, destination/replay planning policy,
  caller-selected replay capability, CLI flags, and all generated/reference surfaces. Current
  project parsing explicitly rejects `destination_policy.postgres`; it does not accept an ignored
  legacy value. Postgres merge now unconditionally guards finalized input for duplicate keys and
  uses the stage table directly.
- 2026-08-03: Added a live transaction proof: a duplicate-key finalized package returns `Data` and
  rolls back target DDL. Regenerated CLI help, completions, man pages, command docs, and error docs.
  `graphify update .` could not run because `graphify` is not installed on this host; no graph
  output is claimed as current.

## Blockers

None.

## Evidence

- AC1/AC3/AC4: affected all-target `cargo check` passed; focused Postgres planning and live rollback
  tests passed. The public plan/input no longer contains a destination dedup policy.
- AC2/AC6: the clap authority rejects both removed flags as unknown; generated CLI artifacts and
  reference docs reproduce exactly from current code; source/docs inventory contains the spelling
  only in negative-removal tests and package-owned dedup evidence.
- AC5: package/engine winner selection was not modified; existing package-level dedup tests and
  artifacts remain the authority consumed by destinations.
- AC7: `cargo nextest` selected 7 tests across Postgres/project/CLI and all passed, with 656 skipped;
  affected all-target strict Clippy passed warning-free; formatting and `git diff --check` passed.
  The first test attempt reached the known local `libduckdb` link boundary; rerun with the exact
  repository DuckDB 1.5.4 library environment passed.

## Review

Focused diff review passed. It verified package-owned winner logic was untouched, Postgres retains
an unconditional pre-mutation duplicate guard, no caller/report/config authority survived, current
removed syntax fails closed, and unrelated ClickHouse adapter execution policy remains intact.
Residual risk is limited to external integrations compiling directly against the removed net-new
Rust API, which is intentionally unsupported under the current-only policy.

## Retrospective

The replay-policy capability was not a reusable abstraction: its only concrete consumer was the
Postgres override that contradicted package identity. Removing that field and the destination-local
first/last CTE made both the runtime contract and hot merge SQL smaller. Negative parser/config
tests are essential in a no-compatibility codebase because deleting a typed field is insufficient
when a flattened generic map could otherwise accept it silently.
