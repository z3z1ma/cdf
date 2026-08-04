Status: done
Created: 2026-08-03
Updated: 2026-08-03
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-c1-semantic-registry-core-consumer-migration.md`, `.10x/tickets/done/2026-08-03-d0-remove-postgres-merge-dedup-policy.md`

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
- 2026-08-03: Began execution after both dependencies closed. Re-read the publication recovery
  authority and confirmed `graphify` is unavailable in this environment, so source inspection is
  the fallback permitted by the project instructions.
- 2026-08-03: Implemented one current-only lockfile v2 with mandatory reachable semantic pins;
  version 1 now fails closed and no reader, default, migration, or compatibility shim remains.
- 2026-08-03: Added the bounded closed manifest v1 model and typed layered identities for inputs,
  environment/dependency/lock binding, each compiled resource, semantic profiles/snapshot,
  lineage, and the complete semantic artifact. The full typed `CompiledSourcePlan`, compiler
  binding, canonical Arrow schema/fields, lock contract snapshot, and destination sheet retain
  their existing meanings instead of being reconstructed from display text.
- 2026-08-03: Added typed project/adapter/built-in semantic provenance, complete reachable
  definitions, normalized parameter values, nested-field usage, and full definition/profile
  hashes. The manifest compiler requires explicit provenance for every non-built-in definition.
  A project-defined `finance.currency@1(code="USD")` is compiled directly onto a Decimal field,
  pinned in `cdf.lock`, and snapshotted as project authority.
- 2026-08-03: Cut the semantic-catalog injection seam through declarative field compilation,
  contract compilation/redaction/destination validation, project lock generation/freezing/schema
  pinning, and CLI project context. Existing built-in compilation remains the ordinary catalog,
  while the project compiler can supply one composed catalog without another type lattice.
- 2026-08-03: Implemented canonical parsing with exact byte round-trip, count/size/order bounds,
  typed cross-reference and child-hash recomputation, secret/host-path stop-lines, selected
  environment and exact lock validation, and a stable read that samples both transaction generation
  and all three public files before returning.
- 2026-08-03: Reused the existing multi-file transaction protocol. Manifest-only publication
  reasserts the bound `cdf.lock` under the same guard and commits the manifest last; combined
  publication installs the manifest before `cdf.lock`. The existing pending marker, synced private
  temporary, forward recovery, and third-value preservation mechanisms remain the sole authority.
- 2026-08-03: The first focused test attempt reached link only and failed because local DuckDB was
  not on the linker path. Re-running the same selection with the repository's existing
  `target/b3-perf/deps` library path passed; no product change was made for that host setup.
- 2026-08-03: Integrated the dependent CLI red-team repairs into core authority. Stable manifest
  loading now verifies live project-relative authored bytes, selected destinations bind through an
  explicitly supplied canonical built-in id, and manifest-only offline publication rejects rather
  than recovers a pending project transaction. The active specification and publication knowledge
  were corrected to make those boundaries durable.

## Blockers

None after dependencies close.

## Evidence

- AC 1–5 and 7: `cargo check -p cdf-project --all-targets` passed. The four focused manifest tests
  passed under nextest: custom currency pin/snapshot, byte/hash determinism and timestamp exclusion,
  closed parse/hash tamper rejection, and manifest/lock publication plus stable read. The custom
  test proves the full definition, `code = USD` normalized parameter, field path, provenance, and
  lock pin rather than only presence of a semantic string.
- AC 2–5: the focused existing lock/contract tests passed (3/3): lock semantic round-trip/diff,
  contract freeze authority preservation, and contract drift reporting. The lock test also rejects
  version 1 explicitly.
- AC 6 and 8: the new manifest publication test proves manifest-before-lock ordering, exact stable
  load, expectation drift rejection, and preservation of an unrelated public value. Existing
  `cdf-project` process-loss, pending read-only failure, forward recovery, race, marker-corruption,
  missing-managed-temporary, and Environment/Contract/Internal ownership tests remain the reused
  publication protocol evidence. Their focused nextest rerun passed 7/7 with 269 tests skipped.
- AC 9: `cargo check -p cdf-cli --all-targets`, `cargo fmt --all`, `git diff --check`, and strict
  warning-free Clippy for `cdf-contract`, `cdf-declarative`, `cdf-project`, and `cdf-cli` all passed.
  No whole-workspace test suite ran.
- AC 3, 6, and 8: the final three-test red-team closure selection passed with 576 unrelated tests
  skipped, and strict Clippy passed for `cdf-builtin-drivers`, `cdf-project`, and `cdf-cli`. The
  focused cases prove current authored-input binding, canonical destination alias binding, and
  no-recovery offline publication.

## Review

The integrated independent review initially failed the lane on three significant authority gaps,
all recorded in the dependent CLI ticket. Each now has a direct repair and focused regression
proof. Reconciled verdict: `pass`; no critical finding or accepted unresolved concern remains.

## Retrospective

Layered artifact hashes, stable publication, and read-only query exposure form one authority
boundary: the loader must re-bind live authored inputs, canonical adapter identity must come from
the composition root, and an offline publisher must be unable to trigger recovery. Treating any of
those as a local CLI detail would have left the manifest core semantically incomplete.
