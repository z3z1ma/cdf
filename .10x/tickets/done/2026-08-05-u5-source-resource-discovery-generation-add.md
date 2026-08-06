Status: done
Created: 2026-08-05
Updated: 2026-08-05
Parent: `.10x/tickets/done/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/done/2026-08-05-u4-portable-plan-export-consumption.md`

# U5 source/resource discovery, generation, and add

## Scope

Implement the two ratified discovery identity spaces: adapter-owned catalog discovery for one
configured source and bounded schema discovery for selected authored resources. Add canonical
optional discovery artifacts, explicit create-or-verify thin-resource generation with namespace
override and partial conflict reporting, and reconcile `cdf add` to the same thin current-only
authoring model and `plan` next action.

## Non-goals

- schema pinning, compilation, lock/index publication, destination contact, or execution;
- generic inference of database/collection/object identity outside source adapters;
- overwrite/force behavior or compatibility aliases;
- doctor/package/resume/recovery work owned by U6.

## Acceptance Criteria

1. CLI grammar/help exposes `discover source` and `discover resource`, selectors bind before I/O,
   `--namespace` requires `--generate`, and removed schema-discover grammar remains absent.
2. Source adapters expose bounded deterministic catalog candidates with canonical relation id,
   safe label, resource-token proposal, complete upstream options, schema summary when available,
   and generation/truncation evidence; unsupported drivers fail precisely.
3. Resource discovery invokes only selected source drivers, observes schema into temporary
   authority, reports coverage/schema/generation, and writes no project/runtime/destination state.
4. `--out` adds one canonical bounded content-hashed artifact without replacing terminal output or
   overwriting nonidentical bytes.
5. `--generate` publishes nonconflicting thin resources with an explicit safely quoted top-level
   projection when schema is available and `SELECT *` only as a reported fallback, through guarded
   create-or-exact transactions; it preserves configured-source identity under namespace override,
   retains partial successes, and reports created/unchanged/conflicted outcomes.
6. `cdf add` writes the same thin query shape, supports explicit configured-source identity, loads
   only the selected proposal boundary, and points to `cdf plan <resource>` without lock/compile.
7. Focused CLI/report/redaction/negative-write/generation-conflict tests plus affected check,
   formatter, strict Clippy, and diff check pass. Broader suite/review remains U7 by user direction.

## References

- `.10x/specs/source-discovery-resource-generation.md`
- `.10x/specs/cli-command-intent-and-effects.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/knowledge/cli-report-authority.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `.10x/knowledge/project-file-publication-recovery.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`
- `.agents/skills/audit-error-ownership/SKILL.md`
- `.agents/skills/audit-project-file-publication/SKILL.md`

## Assumptions

- User-ratified: source/resource discovery scopes, read-only default, explicit generation,
  namespace override, partial generation success, thin generated files, and ergonomic add.
- Record-backed: configured source, upstream relation, and path-derived resource identities remain
  distinct; source adapters own catalog identity/options while generic code owns selection,
  bounds, publication, redaction, and reports.

## Journal

- 2026-08-05: Activated after U4 completed and pushed. Implementation will extend the existing
  source registry rather than add generic source-kind switches, and will reuse guarded project
  publication plus existing in-memory schema discovery.
- 2026-08-05: User refined generation to enumerate discovered top-level fields for immediate
  editing, with `SELECT *` retained only as an honest schema-unavailable fallback. Governing spec
  and acceptance criterion updated before implementation.
- 2026-08-05: Added adapter-owned bounded catalog discovery through the source registry, with local
  Files path discovery and per-selected-file schema inference plus SQLite table enumeration and
  catalog schema. Added source/resource CLI scopes, exact/glob selection, canonical artifact
  publication, aggregate resource failures, and guarded create-or-verify generation.
- 2026-08-05: Reworked `cdf add` to read only cdf.toml and its selected environment/source proposal,
  accept an explicit configured source independent from path namespace, emit the same thin query
  shape, and direct the user to plan. Unrelated invalid resource SQL no longer blocks add.
- 2026-08-05: Applied CLI report authority through one typed source/resource report per human and
  JSON path; applied error ownership without flattening adapter/file/catalog failures; applied
  project publication authority with exact cdf.toml and catalog/schema drift guards.

## Blockers

None.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-cli` passed after the final implementation.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli-core --lib` passed 51 parser/render/core tests.
  `cargo test -p cdf-cli --lib tests::discover` passed 6 focused discovery tests and
  `cargo test -p cdf-cli --lib tests::add` passed 7 focused add tests.
- Focused behavior proves read-only source discovery, Files field inference, SQLite table/schema
  enumeration, explicit safely quoted projections, reported star fallback, namespace override,
  partial conflict success, aggregate resource failures, temporary resource authority, canonical
  artifact create/unchanged/no-overwrite behavior, explicit add source identity, and add isolation
  from unrelated invalid resource SQL.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-runtime -p cdf-project -p cdf-source-files
  -p cdf-source-sqlite -p cdf-cli-core -p cdf-cli --all-targets -- -D warnings` passed. The explicit
  cognitive-complexity diagnostic reported only pre-existing functions outside changed code.
  `cargo fmt --all` and `git diff --check` passed.
- `cargo machete --with-metadata` completed and reported only the pre-existing `cdf-cli` dev
  dependency `flate2`; the new direct `glob` dependency is used. `graphify update .` could not run
  because `graphify` is not installed in this environment.
- Manual sandbox exercise discovered `data/events.ndjson`, inferred `id` and `updated_at`, generated
  an explicit projection in a temporary namespace, then discovered that authored resource with two
  fields. The exact temporary file and directory were removed after the check.

## Review

Deferred by explicit user direction to the single final U7 tranche review.

## Retrospective

The useful seam was separating cheap catalog enumeration from per-selected schema observation.
Putting schema probes inside enumeration would have made a broad catalog perform payload I/O for
every candidate before selectors could narrow work. A second adapter-owned schema method lets the
generic layer select first while preserving driver authority. The same isolation principle exposed
and removed `cdf add`'s accidental whole-project compilation dependency.
