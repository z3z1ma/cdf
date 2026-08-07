Status: done
Created: 2026-08-06
Updated: 2026-08-06
Parent: `.10x/tickets/done/2026-08-06-state-backed-schema-authority-program.md`
Depends-On: `.10x/tickets/done/2026-08-06-s1-state-schema-authority-foundation.md`, `.10x/tickets/done/2026-08-06-s2-state-backed-preparation-portable-plan.md`, `.10x/tickets/done/2026-08-06-s3-schema-drift-dispositions.md`, `.10x/tickets/done/2026-08-06-s4-state-backed-promotion-settlement.md`, `.10x/tickets/done/2026-08-06-s5-delete-lockfile-product-surface.md`

# S6 state-schema integration certificate

## Scope

Freeze and certify the complete state-backed cutover after S1–S5:

- reconcile child statuses, active specs/decisions, generated artifacts, docs, examples, VISION,
  system SQL, and current command grammar;
- run one broad workspace behavior/quality barrier proportional to the final change range;
- build the bundled release binary without developer-only DuckDB linkage;
- exercise disposable copies of the supplied sandbox through first use, portable planning,
  active-schema drift, variant, quarantine, fail, promotion, exact-package/recovery, state history,
  and scoped doctor;
- perform one fresh final review under the then-current coordination policy, focusing on authority,
  fencing, negative effects, redaction/report parity, and current-only deletion;
- close every child and the parent only when evidence maps every criterion.

## Non-goals

- new functionality beyond a concrete integration defect;
- Postgres/distributed testing, schema export/import, nested/future-only promotion;
- repeated broad suites during repair; use targeted checks after the one broad barrier exposes a
  concrete owner;
- preserving a lockfile to ease sandbox migration.

## Acceptance criteria

1. Workspace behavior suite and doc tests pass, with declared skips/limits recorded honestly.
2. Formatting, all-target/all-feature check, strict Clippy, generated-reference checks, scoped
   duplication, dependency/module cleanup, product smoke matrix, and diff checks pass.
3. Release first-use journey proves validate offline, plan no-write plus `--out`, run-from-plan exact
   batch establishment, schema show from state, and no lockfile.
4. Variant journey proves new safe fields create accepted residuals, no migration, unchanged head,
   and exact output/counts.
5. Quarantine/fail journeys prove durable quarantine settlement versus pre-mutation failure and
   checkpoint semantics.
6. Promotion journey proves dry plan, complete top-level historical correction, settlement fence,
   exact one-generation advancement, and subsequent use of the new version.
7. Portable/preparation adversarial cases prove relevant-state invalidation, unrelated-state
   tolerance, selector isolation, and all-selected preflight.
8. Package/recovery/doctor journeys remain source/compiler-isolated where their named authority is
   sufficient.
9. Final current-only sweep finds no shipped lockfile/evolve/freeze/global-quarantine/removed-command
   surface or compatibility machinery.
10. Final review has no unresolved critical/significant authority, correctness, security, or data
    loss finding; residual risks are explicit.

## References

- `.10x/tickets/done/2026-08-06-state-backed-schema-authority-program.md`
- `.10x/decisions/state-backed-schema-authority.md`
- `.10x/specs/resource-preparation-command-experience.md`
- `.10x/specs/portable-plan-artifact.md`
- `.10x/specs/schema-drift-dispositions.md`
- `.10x/specs/schema-promotion-corrections.md`
- `.10x/knowledge/cli-report-authority.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `QUALITY.md`

## Assumptions

- User-ratified: release binary and supplied sandbox are meaningful final integration authority.
- Record-backed: broader verification runs once at this final integration boundary; repairs use
  focused checks and do not spiral into repeated reassurance suites.
- Record-backed: current-only closure deletes rather than tests compatibility.

## Journal

- 2026-08-06: Opened dependency-gated behind S1–S5; no certificate work starts during S0.
- 2026-08-06: S5 closed and pushed at `86e958f1`; began the single consolidated integration
  barrier. Per explicit user direction, broad verification runs once, concrete failures receive
  focused repair/checks, and exactly one independent adversarial review occurs after the release
  sandbox evidence is assembled.
- 2026-08-06: The one broad no-fail-fast workspace behavior run executed 2,217 tests: 2,184 passed,
  33 failed, and 54 were skipped. The failures clustered in stale lock-era fixtures, first-use
  state-parent creation, replay/recovery authority binding, ad-hoc authority establishment,
  discovery identity, and four deterministic engine/package expectations. The standalone-host
  cancellation failure passed in isolation and was classified as a broad-run timing flake rather
  than changed product behavior.
- 2026-08-06: Repaired the concrete integration defects without another workspace rerun. A focused
  176-test pass across the affected CLI, project replay, conformance replay, engine, and package
  modules reached 174 passing with two test-held SQLite handles remaining; both handles were then
  released before nested CLI retry and both focused failures passed. First-use local/HTTP Parquet,
  ordinary ad-hoc retry, promotion recovery details, state recovery, replay fencing, discovery
  identity, schema quarantine, projection closure, and package determinism now pass targeted
  behavioral checks.
- 2026-08-06: Closure quality checks pass: workspace all-target/all-feature check, strict
  all-target/all-feature Clippy, formatting, diff whitespace, generated CLI command/error
  references, generated memory-owner matrix, doctests (one compile-fail doctest passed), workspace
  rustdoc, the 11-cell product smoke matrix, `cargo machete`, and scoped `jscpd` at 2.51% duplicated
  lines/2.67% duplicated tokens under the 10% threshold. The explicit cognitive-complexity
  diagnostic found no warning in changed `cdf-cli` or `cdf-project` production code; its warnings
  were in unchanged dependency crates.
- 2026-08-06: A plain release build correctly failed to link because it requested dynamic
  `-lduckdb`; the repository release workflow establishes that published builds require
  `--features bundled-duckdb`. Updated the source-build operator command to match that authority and
  started the correct bundled release build without the developer-only DuckDB download override.
- 2026-08-06: Release integration found that state authority was hashing only the admitted source
  fields while packages, receipts, and replay use the complete logical output, including
  `_cdf_variant`. Unified compile, state, checkpoint, receipt, and replay on the destination-neutral
  logical output schema while retaining the source-schema hash as admission authority. Focused
  engine/project/replay checks pass.
- 2026-08-06: The promotion journey exposed two historical-correction defects. Proposed snapshots
  did not bind complete target/package receipt evidence when a package had no residual rows, and
  DuckDB compact provenance lookup discarded the logical namespace. Promotion plans now bind every
  selected target package and receipt, exact qualified target identity reaches destination
  corrections, and replace readback excludes only fully superseded packages for destinations that
  support residual readback. A three-package regression proves one clean package, one superseded
  residual package, and one current residual package produce a valid deterministic plan and one
  generation advance.
- 2026-08-06: The final fail-policy journey found project SQL accepted only `EXPERIMENTAL` and
  `GOVERNED` even though the typed compiler and active specification define four presets. Removed
  the parser split by accepting and compiling `FINANCIAL` and `SERVING`; a focused grammar test
  covers all four current presets. A final affected-package check and strict Clippy pass.
- 2026-08-06: Built the final publishable binary with `cargo build -p cdf-cli --bin cdf --release
  --locked --features bundled-duckdb` in 11m06s, without the developer-only DuckDB linkage
  override. Disposable release-binary sandboxes then proved governed promotion and quarantine plus
  financial pre-mutation failure. The supplied sandbox itself and the installed user binary were
  not modified.
- 2026-08-06: Final current-only search found no product-path `cdf.lock`, `compile --refresh`,
  `schema pin`, contract-freeze, lock inspection, evolution/freeze mode, or global quarantine
  configuration surface. `graphify` is unavailable in this checkout environment, so no graph
  refresh was possible; this limits only the optional derived navigation artifact, not product
  verification.
- 2026-08-06: The sole independent red-team review found two significant integration defects. An
  active logical output schema could incorrectly replace the physical source-admission schema,
  breaking projected/aliased resources, and fully superseded replace packages could disappear
  from promotion correction authority. No other critical or significant finding was reported.
- 2026-08-06: Repaired source/output authority separation by preserving the compiled source plan
  when binding active logical state, retaining explicit SQL aliases, and admitting only the exact
  compiler-added self-provenance difference at the manifest boundary. Five source-planning tests,
  including active projected compile/plan/run, and state-backed REST/Postgres compilation pass.
- 2026-08-06: Added typed superseded-package correction evidence and field authority. Replace
  promotion now proves a zero-operation historical settlement, performs the required schema
  migration through the real destination protocol, publishes a verified receipt/checkpoint, and
  never fabricates a row or receipt. Fourteen schema-promotion tests, including live Postgres and
  the fully superseded replace regression, pass.
- 2026-08-06: Final post-review validation passed for formatting, diff whitespace, an
  all-target/all-feature check and strict Clippy across all eleven changed packages, focused
  correction contracts/DuckDB behavior, the 14-test promotion module, and the five-test source
  planning module. The broad workspace suite was deliberately not repeated.

## Blockers

None. S1–S5 are closed with state as sole schema/promotion authority and no live lock surface.

## Evidence

1. The single broad `cargo nextest` barrier executed 2,217 tests: 2,184 passed, 33 exposed the
   integration clusters journaled above, and 54 were declared skips. Every deterministic failure
   received a focused regression and passed after repair; the standalone-host cancellation case
   passed alone and remains classified as a timing flake. Per the user-approved one-suite policy,
   the identical broad command was not rerun, so this criterion is supported by the broad discovery
   run plus focused closure rather than a second all-green workspace count.
2. Workspace all-target/all-feature check and strict Clippy, formatting, diff whitespace,
   generated command/error/memory-owner references, doctest, rustdoc, the 11-cell product smoke
   matrix, `cargo machete --with-metadata`, and first-party `jscpd` all passed. Duplication measured
   2.51% of lines and 2.67% of tokens against the 10% threshold. Explicit cognitive-complexity
   diagnostics found no warning in changed production code.
3. The release-binary first-use and portable-plan scenarios completed during the S2 certificate;
   S6's final release journeys reconfirmed no lockfile and state-backed schema establishment/show.
   Static validation and no-write plan behavior are additionally covered by the passed product
   smoke matrix and focused CLI preparation tests.
4. In `/tmp/cdf-s6-certificate.CUI2P2`, generation 1 had `id`, `updated_at`, and `_cdf_variant`.
   Two subsequent governed runs accepted four total rows, captured one then two residuals for
   `new_safe_field`, performed no migration, and left generation 1 unchanged.
5. In `/tmp/cdf-s6-quarantine.nEPuXO`, changing `VendorID` from Int32 to Utf8 produced zero accepted
   rows, one terminal quarantined partition, a durable quarantine record, a zero-row receipt, and a
   committed checkpoint while generation 1 stayed active. In
   `/tmp/cdf-s6-release-fail.yZO7Uc`, a `FINANCIAL` resource with an unknown field failed before
   destination mutation: checkpoint count remained zero, generation 1 remained active, no
   destination database existed, and only bounded failed-attempt trace metadata remained.
6. The promotion sandbox planned generation 1 to 2 from complete evidence for three packages
   (current residual: two rows; prior residual: one row; clean package: zero rows). Execution
   published exactly generation 2 with three cutoff checkpoints and one committed target. DuckDB
   retained rows 1/2 with null promoted values and rows 3/4 with `kept`/`still-kept`; the correction
   artifact addressed only the live replace package and its two exact row ordinals.
7. Focused portable-plan/preparation tests pass for exact relevant-state invalidation, unrelated
   state tolerance, selector isolation, and all-selected establishment before effects.
8. Focused package replay, recovery, state recovery, discovery identity, and source-planning tests
   pass. The product smoke matrix confirms static doctor/validate boundaries remain independently
   callable without restoring compiler or lock authority.
9. The final `rg` current-only sweep over product code, tests, docs, and tooling returned no removed
   lock/evolve/freeze/global-quarantine command or model surface. The four unrelated untracked user
   artifacts remained untouched and excluded from review/staging.
10. The sole independent review initially reported two significant findings and no other critical
    or significant finding. Both were resolved with focused behavioral regressions: active
    projected resources preserve source-admission authority, and fully superseded replace packages
    settle through typed zero-operation correction evidence. No significant finding remains open.

## Review

Verdict: pass after resolution of the sole review's findings.

- Significant: active state binding could subtract `_cdf_variant` from the logical output and use
  that result as source input, conflating physical source admission with relational output. Fixed
  by preserving the exact compiled source plan and binding state only to the logical descriptor;
  projected/aliased compile, plan, and run now pass end to end.
- Significant: replace promotion discarded packages whose addressed rows were fully superseded,
  leaving no durable proof that their historical corrections were intentionally absent. Fixed by
  binding typed package hash/count/digest supersession evidence into the correction artifact,
  destination request, receipt, and checkpoint transition; a zero-operation regression passes.
- Reconciliation: both findings were repaired by the primary executor and falsified with focused
  behavioral tests. Per the user's explicit one-review limit, no second adversarial review was
  commissioned.
- Residual risk: no distributed shared-state/cross-host execution or Postgres zero-operation
  correction journey was run. Postgres ordinary promotion and catalog/state integration pass live
  tests; a Postgres state backend and cross-host certificate are explicit non-goals.

## Retrospective

- The release sandbox was materially more valuable than another immediate workspace rerun: it
  falsified cross-crate authority assumptions that isolated tests had not represented, especially
  the difference between admitted source schema and complete logical output schema.
- Promotion correctness needs complete target/package evidence, including zero-effect members;
  otherwise a deterministic hash can still authorize an incomplete historical correction.
- Destination correction capabilities must own their history semantics. Replace-history filtering
  is valid for in-place residual readback but would corrupt package-addressed Parquet correction if
  applied globally.
- Ratified vocabularies should have one typed grammar boundary. Duplicating trust-preset subsets in
  project SQL allowed a current model to exist below a user-inaccessible parser.
- The one broad barrier plus focused repairs kept feedback proportional. The honest residual limit
  is that there is no post-repair all-workspace green count; targeted regressions, strict quality
  gates, release builds, and behavioral sandboxes provide the closure evidence instead.
- Logical output authority and physical source-admission authority must remain separate even when
  their schemas happen to match. Explicit SQL aliases are the smallest regression that exposes an
  accidental collapse of those domains.
- Replace semantics can make a historical correction set empty without making its evidence
  irrelevant. Zero-effect transitions require typed, content-bound settlement rather than omission
  or a fabricated data segment.
