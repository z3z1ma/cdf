Status: done
Created: 2026-08-05
Updated: 2026-08-05
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/done/2026-08-05-u3-selected-preparation-multi-resource-plan-run.md`

# U4 portable plan export and consumption

## Scope

Add `cdf plan SELECTOR... --out <path>` as an additive, canonical portable-plan publication effect
and `cdf run --plan <path>` as a mutually exclusive execution authority. Reuse the exact selected
compiled resource, source, schema, destination, checkpoint, and native execution identities already
owned by project/runtime types. Preflight every resource without repair or mutation before any
resource executes, then preserve U3 independent terminal outcomes and existing terminal plan/run
documents.

## Non-goals

- embedding payload data, credentials, host paths, sessions, or trait objects;
- archive/bundle or remote artifact distribution;
- automatic replan, schema evolution, drift acceptance, or plan repair;
- source discovery/generation/add redesign (U5);
- package/recovery run modes and scoped doctor (U6).

## Acceptance Criteria

1. `cdf plan SELECTOR... --out <path>` keeps the existing human plan documents and adds a typed
   artifact effect with redacted path, semantic hash, bytes, resource count, and
   `created|unchanged` status.
2. Portable artifact bytes are versioned, canonical, bounded, deny unknown fields, content-hashed,
   secret-redacted, deterministically ordered, and published only after every selected resource is
   ready; a nonidentical existing file is not overwritten.
3. The artifact binds authored/resolved selection, current project/environment/compiler/resource
   artifact/lock/schema/source/native plan/destination/checkpoint identities, failure policy,
   required host capabilities, and `locked|proposed_first_use` schema authority without serializing
   a report, DataFusion plan, credential, or runtime handle.
4. `cdf run --plan <path>` conflicts with selectors and semantic plan-shaping flags while allowing
   presentation and runtime-only ceiling tightening.
5. Run validates the complete artifact/project/registry/source generation/destination/checkpoint/
   host authority set before package, destination, ledger, receipt, checkpoint, lock, or compiled
   artifact mutation. Any difference fails all selected resources with exact re-plan guidance.
6. Successful preflight executes the exact canonical resource set/native prepared authorities,
   commits an exact proposed-first-use baseline before effects, and binds the portable plan hash in
   typed aggregate execution evidence.
7. Terminal-only plan behavior remains unchanged and local-only/unverifiable portable bindings fail
   only artifact export with an owned diagnostic.
8. Focused round-trip/tamper/version/bounds/redaction/no-overwrite/preflight/no-mutation/CLI report
   tests, affected-package check, strict Clippy, formatter, generated artifacts, and diff check pass.

## References

- `.10x/specs/portable-plan-artifact.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/knowledge/cli-report-authority.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`
- `.agents/skills/audit-error-ownership/SKILL.md`
- `.agents/skills/audit-project-file-publication/SKILL.md`

## Assumptions

- User-ratified: `--out`, `run --plan`, additive terminal rendering, strict cross-machine
  portability, whole-plan no-repair preflight, and `--plan` rather than encoding-coupled naming.
- Record-backed: U2 immutable compiled resource artifacts and U3 retained prepared objects are the
  only compilation/execution authorities; portable plans bind and validate them rather than
  introducing another executable IR.
- Record-backed: source adapters already own portable physical-plan/generation validation and
  destination capability sheets already have canonical typed identity.

## Journal

- 2026-08-05: Activated after U3 completed and pushed on main. The implementation boundary is one
  canonical envelope over existing native authorities plus a fail-closed hydrator/preflight, not a
  serialization of CLI plan reports.
- 2026-08-05: Added canonical `plan --out` publication and mutually exclusive `run --plan`
  consumption. The artifact embeds native compiled/engine authority plus content-addressed external
  source task metadata, never payload bytes or runtime handles. Whole-plan preflight validates
  project/environment/compiler/lock/source generation/destination/checkpoint/host authority before
  atomically publishing any proposed first-use schema/compiled authority and before execution.
- 2026-08-05: Applied CLI-report authority by keeping terminal plan rendering unchanged when
  `--out` is absent and deriving the additive human/JSON artifact facts from one typed report.
  Applied error ownership by preserving project/contract/data/environment kinds through portable
  parsing, source attestation, destination resolution, and guarded publication. Applied project-file
  publication authority by committing schema sidecars, compiled artifacts, index, and `cdf.lock`
  through one guarded transaction with `cdf.lock` last.

## Blockers

None.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-cli --all-targets --locked` passed after the final
  implementation.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli portable_plan --locked` passed 3 focused
  export/consume/tamper/stale-generation tests; `cargo test -p cdf-cli plan_out --locked` passed 2
  canonical/no-overwrite/additive-terminal tests; the exclusive `run --plan` parser test passed.
- The first-use round trip proves plan writes no project authority, run preflights then publishes
  `cdf.lock`/schema/compiled index and executes DuckDB, and aggregate JSON binds the plan hash.
  Tampered bytes and changed file generation both fail before lock/index/package/state/destination
  writes.
- Strict affected-package Clippy passed with `-D warnings`; `cargo fmt --all` and `git diff --check`
  passed. `cargo machete --with-metadata` completed and reported only pre-existing `cdf-cli` direct
  dependency `flate2`; the new direct `base64` dependency is used. `graphify update .` could not run
  because `graphify` is not installed in this environment.
- A broader `tests::planning` run passed 19/23 at observation time. The stale explain helper was
  repaired and its focused test then passed. Three PostgreSQL executable-backfill tests remain on
  the pre-existing schema-coercion failure and are assigned to U6's explicit backfill audit; they
  do not exercise U4 portable-plan paths.

## Review

Deferred by user instruction to the final combined U-tranche barrier; no U4 red-team review runs.

## Retrospective

The important portability boundary was not the JSON envelope; file planning already externalizes
high-cardinality canonical task authority into a temporary content store. Rejecting that authority
would have made the ordinary file-source journey non-portable. Embedding its bounded,
content-addressed task metadata and reinstalling it into a runner-owned temporary store preserves
the source adapter's native reader/attestation path without embedding payload data or inventing a
second plan IR. The reusable lesson is to audit transient authorities owned beneath a supposedly
portable typed plan before declaring the outer artifact portable.
