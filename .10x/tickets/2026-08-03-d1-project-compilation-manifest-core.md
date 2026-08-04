Status: open
Created: 2026-08-03
Updated: 2026-08-03
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/2026-08-03-c1-semantic-registry-core-consumer-migration.md`, `.10x/tickets/2026-08-03-d0-remove-postgres-merge-dedup-policy.md`

# D1 project compilation manifest core

## Scope

Implement the versioned canonical project-manifest models, deterministic layered hashes,
validation/read/write bounds, typed compilation assembly from existing project/lock/resource plan
authority, reachable semantic snapshot binding, and crash-safe `.cdf/manifest.json` publication.

## Non-goals

- CLI command grammar and SQLite table mounting, owned by the dependent CLI child;
- SQL resource parsing/native IR;
- content-addressed child artifacts or manifest-history database;
- external service publication;
- implicit source refresh.

## Acceptance criteria

1. One closed versioned manifest model covers header, authored inputs, resources, fields, reachable
   semantics, lineage, and diagnostics with canonical ordering and explicit bounds.
2. Layered typed hashes preserve existing source/schema/contract/destination meanings and one
   top-level semantic hash excludes its own field and nonsemantic time.
3. Manifest validation fails closed on unknown version, wrong hash, duplicate ids/paths/field
   ordinals, dangling references, inconsistent child hashes, secrets, absolute host leakage, stale
   environment, or lock mismatch.
4. Compilation assembles from existing typed plans/artifacts without DataFusion debug strings,
   driver contact, runtime replanning, or generic JSON reinterpretation.
5. `CdfLock.semantics` pins reachable canonical reference → definition hash while the manifest
   records complete definitions, normalized parameters, and per-field usage.
6. `.cdf/manifest.json` publication uses the existing project mutation guard, synced pending
   marker, prior/new hash checks, forward recovery, stable generation, and owner/path fences.
   Manifest-only publication uses manifest last; manifest+lock uses `cdf.lock` last.
7. Repeated compilation of identical inputs is byte/hash stable; changing one input/semantic/plan
   changes only its affected layered hashes and the top-level hash.
8. Crash/race/failure injection proves no mixed public generation, no unrelated overwrite,
   read-only fail-closed behavior, and correct Contract/Internal/Environment ownership.
9. Formatting, `git diff --check`, focused `cdf-project` tests/checks, and targeted strict Clippy
   pass without a whole-workspace suite.

## References

- `.10x/specs/project-compilation-manifest.md`
- `.10x/decisions/project-manifest-path-compile-and-query-policy.md`
- `.10x/research/2026-08-03-project-compiler-authority-inventory.md`
- `.10x/skills/audit-project-file-publication/SKILL.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.10x/knowledge/content-addressed-sidecar-publication.md`
- `.10x/specs/semantic-type-registry.md`

## Assumptions

- Artifact path, commit policy, compile modes, table set, semantic lock/manifest split, and no-
  history first slice are user-ratified.
- Existing typed plan hashes retain their meanings and are not interchangeable.

## Journal

- 2026-08-03: Opened after D0 inventory and user ratification. The required publication skill was
  read during shaping. No product code changed in this turn.

## Blockers

None after dependencies close.

## Evidence

Pending execution.

## Review

Pending one independent D1 review after the CLI child integrates.

## Retrospective

Pending execution.
