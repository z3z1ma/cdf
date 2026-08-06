Status: done
Created: 2026-08-04
Updated: 2026-08-05
Parent: `.10x/tickets/2026-08-04-resource-first-cli-experience-program.md`
Depends-On: `.10x/tickets/done/2026-08-04-u0-manifest-text-diagnostic-ownership.md`

# U1 resource selectors and static validate

## Scope

Implement the first reusable resource-first boundary and cut validate down to its ratified intent:

- exact/glob positive selectors, repeated exclusions, canonical union/sort/dedup, per-positive
  match errors, and explicit all behavior;
- path-shape-only glob expansion and exact direct resolution before parsing or driver work;
- `cdf validate [selector...]` using pure project/config/path/SQL/reference/driver-schema checks and
  local generated-authority integrity/status only;
- deterministic aggregate typed report and existing renderer vocabulary for counts, ordered
  per-resource diagnostics, checked/skipped effect facts, and JSON/human parity;
- removal of `validate --deep` and every validate-time secret/environment/source/destination/state
  access path, help entry, generated artifact, test, and fixture.

## Non-goals

- applying selectors to plan/run/compile/discover/preview/backfill in this child;
- resource-sharded lock or manifest publication;
- doctor implementation or moving deep observations beyond ensuring validate no longer owns them;
- source discovery, schema commitment/promotion, destination compatibility, or any write;
- compatibility grammar for `validate --deep`.

## Acceptance criteria

1. Exact, overlapping glob, repeated exclusion, duplicate, typo/miss, empty-final-set, quoted-help,
   and canonical-order behaviors match the active selector spec.
2. Exact resolution does not inventory unselected resource files; glob expansion reads only path
   shape/token identity before selection. Unselected malformed SQL/config/credentials never block
   selected validation.
3. Validate performs zero secret-provider calls, environment-variable/file-secret reads, source
   data enumeration/stat, driver discovery/health, destination/state opens, network calls, or
   writes, proven with counters/fault sentinels rather than claims.
4. Static checks cover root/config grammar, resource placement/tokens/UTF-8/bounds, SQL
   syntax/envelope, configured-source and semantic references, pure closed driver option schemas,
   secret-reference syntax, and integrity/status of locally present generated authority.
5. Missing lock/compiled authority is reported as missing status but does not fail an otherwise
   valid first-use project. Static invalidity exits nonzero after attempting all selected resources.
6. Human and JSON output come from one typed aggregate report and agree on selector resolution,
   environment/source/resource/valid/warning/error/current/stale/missing counts, ordered
   diagnostics, and skipped operational checks.
7. CLI help/completions/man/reference artifacts contain selectors and no `validate --deep`; no
   alias, fallback parser, retired deep fixture, or rejection-only compatibility test remains.
8. Focused behavioral tests, affected-package checks, strict affected Clippy, explicit cognitive-
   complexity diagnostic, formatting, generated-artifact checks, and `git diff --check` pass.

## References

- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/resource-selector-batch-commands.md`
- `.10x/decisions/static-validation-operational-readiness-boundary.md`
- `.10x/specs/project-cli-observability-security.md` (superseded only where the decision states)
- `.10x/specs/data-onramp-source-experience-cli.md` (superseded only where the decision states)
- `.10x/knowledge/cli-report-authority.md`
- `.agents/skills/audit-cli-report-authority/SKILL.md`

## Assumptions

- User-ratified: selector grammar/batch law and strictly static/offline validate behavior.
- Record-backed: root TOML syntax and path/config identity must be known before resource selection;
  pure driver JSON-schema admission is not external observation.
- Record-backed: missing generated authority is a normal first-use state because plan/run own
  preparation in the active model.

## Journal

- 2026-08-04: Opened after user confirmation. Current source inspection proves validate loads a
  whole `ProjectContext`, resolves secrets, and uses `--deep` to reach source and destination
  authorities. The child intentionally establishes one shared selector seam but wires it only to
  validate so later commands reuse evidence-backed behavior rather than a speculative framework.
- 2026-08-04: Execution began after U0 closed with independent review pass. Re-read every governing
  record and `QUALITY.md`. The current validate path is not repairable by trimming one call:
  `ProjectContext` inventories/compiles every resource, inspects the destination, hydrates locks,
  and constructs a secret provider before `validate_project` resolves every discovered reference.
  U1 therefore needs a separate static project-load boundary plus a reusable path-first selector;
  it will reuse pure source option schemas and SQL parsers but never construct runtime plans.
- 2026-08-04: Added a shared path-first selector authority in `cdf-project`. Exact selectors resolve
  one canonical `cdf/<namespace>/<resource>.cdf.sql` path directly; glob selectors inventory path
  identity only. Positive match requirements, repeated exclusions, canonical union/sort/dedup,
  empty-final rejection, exact suggestions, and explicit unscoped-all behavior are covered without
  parsing unselected SQL or configuring a driver.
- 2026-08-04: Replaced validate's `ProjectContext` path with a static boundary whose dependency
  surface contains project bytes, pure source schemas, SQL/semantic parsers, and locally present
  lock/manifest bytes only. The typed report owns ordered resource/global diagnostics, aggregate
  counts, local authority status, and checked/skipped effect facts for both JSON and human output.
  Missing generated authority is normal status; corrupt local authority is an aggregate error and
  is never repaired.
- 2026-08-04: Deleted `validate --deep`, its 970-line executor/renderer, and deep-only source schema,
  destination, and runtime-baseline helpers. Updated Clap, generated help/completions/man pages,
  reference docs, conformance examples, diagnostics, and tests with no legacy parser or shim.
- 2026-08-04: Broad `cdf-cli --lib` and `cdf-project --lib` runs remain red in runtime/replay/schema
  fixtures outside U1 (168/274 and 271/305 passed respectively). A clean detached worktree at the
  pre-U1 commit `f7347083` reproduced the representative
  `authored_envelope_does_not_pollute_equivalent_execution_identity` failure unchanged, proving the
  red family predates this slice. Focused changed-surface certificates are green and the baseline
  worktree was removed after isolation.
- 2026-08-05: Integration against current main preserved its destination-registry and Mongo source
  changes. Independent review found that static validation rejected inline credentials but did not
  reject malformed state/destination URI syntax. Added pure syntax admission for `sqlite://path`,
  destination `<scheme>://location`, and whole-value secret references without resolving or opening
  any service.

## Blockers

None. U0 closed with independent review pass.

## Evidence

- AC1-2: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project --lib
  resource_selector::tests::` passed 5/5. It covers exact resolution with a malformed unselected
  sibling, overlapping/duplicate globs, `?` and bracket classes, repeated zero-match exclusions,
  per-positive misses, canonical ordering, empty-final rejection, and explicit unscoped all.
  `project_inputs` focused tests passed 10/10 after consolidating path enumeration behind one
  authority.
- AC3-5: `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-cli --lib tests::init_validate::` passed 11/11.
  Fault-sentinel coverage removes source data, supplies an unresolved environment secret, configures
  a statically invalid unselected source, and leaves destination/state paths absent; selected
  validation succeeds, emits neither the secret key nor a value, and creates no lock, schema,
  package, state, or destination artifact. Aggregate invalid SQL, corrupt unchanged lock bytes, and
  current-to-stale local authority are also proven.
- AC6: JSON and human assertions consume one `ProjectStaticValidationReport` and agree on selected
  facts, authority counts, effects, and secret absence. Static source gates find no renderer
  primitives or anonymous JSON construction in `project_command.rs`/`commands.rs`.
- AC7: `cargo test -p cdf-cli-core --all-features --locked` passed 56/56 plus doc/bin targets.
  Generated CLI artifact and docs `--check` modes both passed; the only remaining `--deep` match in
  `crates`/`docs` is the protective help assertion that it is absent.
- AC8: affected all-target `cargo check` and strict `clippy -D warnings` passed for `cdf-project`,
  `cdf-cli`, `cdf-cli-core`, `cdf-declarative`, and `cdf-conformance`. The explicit cognitive-
  complexity diagnostic reported no changed production function. `cargo fmt --all -- --check`,
  `git diff --check`, and `cargo machete --with-metadata` passed.
- Maintainability: first-party `jscpd` completed at 2.44% duplicated lines under the 10% ceiling and
  found no clone involving `static_validation.rs`, `resource_selector.rs`, or the changed project
  renderer. CLI-core normal/all-feature graph counts were 161/166 lines; the only first-party
  dependency beneath CLI core is the expected `cdf-kernel` edge.
- Report authority: benchmark check passed. `cli_renderer` measured 2.13-2.14 Gelem/s for the
  million-event iteration baseline, 21.52-21.66 Melem/s for million buffered events, 19.68-19.89
  Melem/s for high-partition buffering, 2.97-3.03 Melem/s for the prebuilt 10k-row report, and
  1.93-2.00 Melem/s for 10k-row build-and-render.
- Product smoke: the built CLI ran both JSON and human
  `cdf validate fineweb.documents` against `/Users/alexanderbut/code_projects/cdf_sandbox` and
  exited 0. It selected one of seven configured sources/resources, reported current local authority,
  zero errors/warnings, `writes = none`, and explicit skipped secret/source/network/destination/state
  checks without contacting the unavailable source path.
- Integration repair: `tests::init_validate::validate_aggregates_malformed_environment_uris_without_writes`
  passed and proves malformed state and destination values aggregate as
  `CDF-VALIDATE-ENVIRONMENT` while resource checks continue and no lock/schema/package/state/
  destination artifacts are created. The existing inline-credential project test also passed.

## Review

Independent red-team review found no semantic merge loss across the destination registry, Mongo
source registration, compile dispatch/remediation, project loading, manifest string security,
project-input inventory, or deep-validate deletion. It raised one significant static URI-syntax
gap; the integration repair and focused regression above close that finding. Full workspace and
live Mongo suites were not repeated because the merge does not alter their implementations; the
affected compile, strict Clippy, selector, validate, manifest, SQL/compile, generated-artifact, and
dependency checks passed.

## Retrospective

The crucial simplification was recognizing that static validation cannot safely be a mode on the
runtime project loader: the loader's construction graph already commits to secrets, destinations,
compiled authority, and whole-project inventory. A separate narrow input boundary both enforces the
offline promise structurally and deleted substantially more code than it added. All-feature CLI-core
testing caught that hand-updated reference docs were insufficient because committed shell/man/help
artifacts share the Clap authority; regenerating all surfaces restored the fence. Broad-suite noise
was isolated once at the pre-U1 commit instead of expanding this ticket into unrelated runtime
fixture repair. The reusable lesson is to prove effect absence with an incapable dependency surface
plus hostile filesystem/config sentinels, not with a boolean called `offline`.
