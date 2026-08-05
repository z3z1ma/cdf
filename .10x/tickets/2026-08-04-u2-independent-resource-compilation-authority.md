Status: open
Created: 2026-08-04
Updated: 2026-08-04
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/2026-08-04-u1-resource-selectors-static-validate.md`

# U2 independent resource compilation authority

## Scope

Replace the current exact-project compilation snapshot with one current-only resource authority:

- make each `cdf.lock` resource entry independently sufficient to fence that resource's governed
  output schema and bind its compiler/dependency, normalizer, selected source configuration,
  reachable semantics, contract, destination capability/mapping, and immutable compiled-artifact
  identity;
- publish one bounded, canonical, content-addressed compiled resource artifact beneath
  `.cdf/compiled/` containing the existing native source/relational/contract/semantic/destination
  plans and exact selected-input bindings required for offline verification;
- replace the monolithic `.cdf/manifest.json` project graph with a bounded compilation index whose
  per-resource entries are `current`, `stale`, `failed`, or `absent`, contain only a verified
  current artifact reference plus safe status facts/diagnostic, and never carry stale plan bytes;
- make artifact/index/lock parsing and stable loading validate one resource without requiring an
  exact authored/locked/project resource set or invalidating unrelated current entries;
- implement `cdf compile [RESOURCE_SELECTOR...] [--exclude <RESOURCE_GLOB>] [--locked]` so selected
  work uses the U1 path-first selector, unscoped work explicitly attempts every resource, and every
  independently successful resource is durably published even when another resource fails;
- delete `compile --refresh`, offline/refresh mode distinctions, monolithic manifest readers and
  fixtures, whole-project exact-set validators, global semantic/destination authorities that
  duplicate per-resource bindings, and all compatibility representations;
- preserve system/package/checkpoint SQL availability while exposing compilation-index status and
  only verified current compiled facts from immutable artifacts.

## Non-goals

- routing plan/run through the selected preparation seam or changing their execution behavior (U3);
- portable plan export/consumption (U4);
- schema diff/promotion redesign beyond adapting them to the one new lock/artifact authority;
- source discovery or thin resource generation (U5);
- destination, package, receipt, checkpoint, run-ledger, or state mutation;
- garbage collection of superseded immutable compiled artifacts;
- compatibility with the old lock version, monolithic manifest schema, refresh flag, or exact-set
  validation behavior.

## Acceptance criteria

1. A lock entry and immutable artifact for resource A verify from only the selected project/env,
   A's SQL/source/semantic/destination inputs, dependency tuple, and exact lock entry. Missing,
   stale, failed, corrupt, or newly authored B cannot invalidate or block A.
2. Editing A marks only A stale; editing an unselected source or resource does not inventory,
   parse, resolve, contact, or stale A. A genuinely shared bound input marks exactly the entries
   that recorded that binding stale.
3. Immutable artifacts use canonical closed serialization, bounded reads/counts/strings, exact
   content hashes, create-or-verify publication, multiline authored-SQL whitespace admission, and
   existing secret/path/control-character fences. Execution never serves a stale/failed artifact.
4. The index is canonical, bounded, deterministic by resource id, and records safe status facts.
   Selected updates preserve previously known unrelated entries without inspecting their authored
   files; unscoped compile refreshes the complete path-derived status surface it is authorized to
   inventory.
5. Selected compile supports exact/glob positives and repeated exclusions. It attempts each selected
   resource independently in canonical order, publishes every success, records every safe failure,
   renders one aggregate typed JSON/human report, and exits nonzero iff any selected resource
   failed. Unscoped compile has the same partial-success law over explicit all.
6. `--locked` permits rebuilding missing/stale derived artifacts only from sufficient unchanged
   lock authority, forbids first-use or changed schema/lock commitment, and fails that resource
   before publication requiring a lock change. Ordinary compile may establish missing first-use
   authority but never promotes an existing governed output schema.
7. Each resource publication runs under the existing mutation guard with exact guards and durable
   order: create-or-verify immutable artifact/sidecars, install the updated index, then install
   `cdf.lock` last iff its bytes change. Pending publication is forward-recovered only by mutating
   retry; read-only loads fail closed without mutation; concurrent unrelated edits are preserved.
8. Process-exit/race tests cover every new artifact/index/lock boundary, missing/corrupt private
   state, idempotent retry, post-install concurrent changes, and error ownership (`Internal` private
   invariant, `Environment` host failure, `Contract` unrelated public authority).
9. `cdf sql` mounts system/package/checkpoint history regardless of compilation health, exposes all
   index statuses, and mounts compiled resource/field/semantic/lineage facts only from independently
   verified `current` artifacts.
10. CLI help/completions/man/reference docs contain selector/locked compile and no refresh grammar.
    Focused authority/compile/SQL tests, strict affected Clippy, explicit cognitive-complexity
    diagnostic, formatter, generated-artifact checks, publication audit, and `git diff --check`
    pass; broader pre-existing failures are isolated rather than laundered.

## References

- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/research/2026-08-04-resource-preparation-ergonomics-inventory.md`
- `.10x/research/2026-08-04-selector-plan-discovery-authority-inventory.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.10x/knowledge/cli-report-authority.md`
- `.agents/skills/audit-project-file-publication/SKILL.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`
- `.agents/skills/audit-error-ownership/SKILL.md`

## Assumptions

- User-ratified: lock entries are per-resource output-schema fences; ordinary compile may establish
  first-use authority but never silently accepts later governed output drift.
- Record-backed: selected exact work cannot inventory unselected authored paths. Therefore the
  compilation index is a durable index of known attempts/current authority, while only an unscoped
  command may claim a complete current path inventory; selected updates preserve unknown/unselected
  entries rather than pretending to rediscover them.
- Record-backed: the current `ManifestResource` already contains most immutable compiled payload
  facts, and the existing project transaction plus content-addressed sidecar patterns own durable
  publication. U2 replaces their project-wide envelope; it does not add a second publisher.
- Record-backed: query-first foundation D3 is closed at `503b58ae`. The usage branch is based on
  that final authority. Later committed `main` work and the other executor's uncommitted shared
  checkout must be reconciled deliberately in this worktree without switching or touching the
  shared checkout.

## Journal

- 2026-08-04: Opened after U1 implementation was committed and pushed. Inspection found the exact
  coupling U2 must remove: `compile_project_manifest` requires equality between all compiled and
  locked resource ids; the manifest header binds whole lock/config/environment bytes; source,
  semantics, destination, lineage, and native plans live in one 64 MiB project artifact; and
  compile is split between fail-fast locked-offline and refresh modes over a whole `ProjectContext`.
- 2026-08-04: Publication audit confirms the existing one-way transaction is the required fence:
  synced temporaries and pending marker, forward-only recovery, exact prior/new expectations,
  stable read-only generation sampling, and lock-last commit. U2 will publish independently per
  resource through that mechanism and will not add rollback or load-time recovery.
- 2026-08-04: The shared checkout is intentionally untouched and currently contains another
  executor's uncommitted connector/fixture work, including context/manifest/input fixes. Execution
  must consume only committed upstream state and preserve those changes.

## Blockers

None. The first execution step is a deliberate committed-upstream reconciliation in this worktree.

## Evidence

Pending execution.

## Review

Pending the combined U1+U2 authority-foundation review barrier.

## Retrospective

Pending execution.
