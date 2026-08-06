Status: active
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

## Blockers

None.

## Evidence

Pending execution.

## Review

Deferred by user instruction to the final combined U-tranche barrier; no U4 red-team review runs.

## Retrospective

Pending execution.
