Status: done
Created: 2026-07-26
Updated: 2026-07-27
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-d1c-product-error-audit.md`

# Complete typed CLI report authority

## Scope

Make one typed serializable report the sole success authority for every command and move all
human layout construction out of command execution modules into report renderers. Remove
superseded command-local layout helpers and plain compatibility surfaces.

## Non-goals

- No command grammar, execution behavior, JSON field removal, or dynamic renderer plugin.
- No trait-per-command service architecture.
- No visual redesign beyond consistency required to complete the authority migration.

## Acceptance criteria

- Every command constructs one typed report consumed by JSON and human rendering.
- Command execution modules contain no `RenderDocument`/primitive layout assembly.
- Report renderers own TTY/headless documents and have paired snapshots plus JSON parity tests.
- Redaction occurs before both render paths.
- Static migration gates reject new command-local layout and legacy plain-string output.
- CLI-core build-graph and renderer throughput floors remain green.

## References

- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/decisions/cli-design-language-and-renderer.md`
- `.10x/specs/product-build-graph-boundaries.md`

## Assumptions

- Source-backed: `CommandOutput` already accepts only `RenderDocument`; this ticket completes the
  partially migrated report-to-document ownership rather than replacing output transport.

## Journal

- 2026-07-26: Inventory found many typed reports and a closed plain-output gate, but layout
  methods/functions remain distributed through command modules.
- 2026-07-27: Activated after D1c closure. Governing records preserve command behavior, JSON
  fields, redaction, and the existing `CommandOutput::rendered` transport boundary; this ticket
  moves only human layout construction and adds the migration fence.
- 2026-07-27: Moved command-family layout into report-owned renderer modules. Replaced the
  remaining anonymous/mismatched success values with typed reports while preserving JSON shapes
  through transparent/flattened serialization, and made report-only presentation inputs
  (`--explain-memory`, plan command/URI, state scope context) explicit non-serialized fields.
- 2026-07-27: Extended the static migration gate across every command execution module. A full
  `cdf-cli` library run passed 297/297 after the authority migration; the focused inspect parity
  test also proves URI userinfo is absent from both JSON and headless human rendering.
- 2026-07-27: Bounded delegated review returned one PASS and one CONCERNS verdict with three
  medium findings. Closure repaired the two code findings by redacting typed report projections
  before storage and applying the layout fence to every non-renderer source module. The evidence
  finding is resolved below from the already-run product/core/graph/throughput gates; no second
  exploratory review round was opened.

## Blockers

None.

## Evidence

- **One report authority / stable JSON:** dispatch help/version, inspect project/resources/
  destinations/package, state show/history/rewind, project validation, package list, plan, and run
  now pass the same named typed report to JSON and human rendering. Transparent and flattened
  wrappers preserve legacy top-level array/object shapes. Focused
  `tests::inspect_package_typed_report_preserves_manifest_json_shape` and
  `tests::inspect_project_redacts_the_same_typed_report_for_json_and_human_output` passed 2/2.
- **Renderer ownership / fence:** `rg -n 'RenderDocument|primitives::\\{'` over `commands.rs`,
  `*_command.rs`, and nested executors returned no matches. The static
  `tests::renderer_migration_gate_rejects_raw_human_output_bypasses` passed and now protects every
  `cdf-cli/src` module except explicit renderer authorities (`*/render.rs`, `reports.rs`, and
  `resume_command/report.rs`).
- **Human/JSON parity:** `cargo test -p cdf-cli --lib -- --nocapture` passed 297/297 after the
  full migration. Existing paired rich/headless product assertions cover plan, run, backfill,
  resume, replay, inspect-run, and state report families; JSON assertions cover every command
  family changed here. D3 retains the explicitly broader 40/80/160 and all-family visual matrix.
- **Redaction:** inspect project/package reports now contain redacted typed projections rather
  than raw URI-userinfo values, and plan stores a redacted explicit destination in its skipped
  presentation field. The two typed-report parity tests plus the plan next-command regression
  passed after the review repair.
- **CLI-core graph / throughput:** `cargo test -p cdf-cli-core --all-features --locked` passed
  53/53, including rich/headless snapshots, 40/80/160 terminal semantics, bounded progress state,
  and the nonblocking 10,000-event throughput floor. Locked Cargo trees measured 80 unique normal
  packages and 84 all-feature normal packages, both below the 113 ceiling; the named forbidden
  product/engine/driver/database/codec scan returned no matches.
- **Compile/lint:** `cargo check -p cdf-cli --all-targets` passed. Strict
  `cargo clippy -p cdf-cli --all-targets --all-features -- -D warnings`, formatting, and
  `git diff --check` passed after the final construction-context repair.
- **Durable learning:** `.10x/knowledge/cli-report-authority.md` and canonical/mirrored
  `audit-cli-report-authority` skills were added; both skill copies are byte-identical and pass
  `quick_validate.py`.
- **Limit:** `graphify update .` could not run because `graphify` is not installed in this
  environment. Source, static gate, tests, Cargo trees, and strict lint supplied closure evidence.

## Review

- OCR deterministic preview/rules froze `9f7ac8e3..d7fe0b2a` as 38 D2 files under the product CLI
  boundary rule.
- Reviewer Noether found no critical/high/medium defect and returned PASS.
- Reviewer Aristotle found three medium concerns: raw secret-bearing report projections, a
  filename-based nested-module gap in the static fence, and pending closure evidence. The first
  two were repaired and focused tests/strict Clippy passed; this ticket's final Evidence section
  resolves the third with reproducible commands and explicit D3 limits.
- Verdict: pass after bounded repair. No critical/high finding remained, and no additional review
  cycle was warranted.

## Retrospective

- **What broke/surprised:** a typed transport alone did not guarantee one report authority:
  anonymous JSON values, extra renderer arguments, and human-only redaction still allowed JSON and
  human output to diverge.
- **What worked:** contiguous renderer-module extraction with a compile after each batch kept the
  large mechanical move reviewable. `#[serde(transparent)]`, `#[serde(flatten)]`, and
  `#[serde(skip)]` preserved machine shapes while making presentation context explicit.
- **Dead ends:** the first schema extraction attempted to write its child before creating the
  directory, and one parity assertion used a streaming header type rather than the serializable
  manifest identity. Both failed immediately and produced no semantic rework.
- **Five whys:** layout kept returning to execution modules because the original gate protected
  legacy shims but not layout types; it used filename heuristics because renderer ownership was
  implicit; implicit ownership existed because the partial migration had no named report audit
  procedure. The all-non-renderer fence, knowledge record, and mirrored skill now make that
  boundary executable.
- No follow-up debt was discovered. The visual/information-architecture expansion remains D3's
  pre-existing owner rather than being pulled into this authority ticket.
