Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-07-p0-structural-debt-program.md

# P1 product experience, instrumentation, and enterprise surface program

## Scope

Implement the P1 product-experience program: live runtime events, coherent CLI grammar, a uniform human-rendering system, structured remedial errors, live progress, in-repository docs/onboarding, Python resources through the product front door, and release engineering/distribution.

This parent is a plan and orchestration record. Workstream tickets own the major lanes below; implementation inside a broad workstream MUST be split into bounded executable child tickets before code changes when the workstream contains multiple independent outcomes.

## Governing records

- `VISION.md`, especially Chapters 18, 20, 21, 22, and 23 and decisions D-17, D-19, D-22, D-23, and D-25.
- `.10x/specs/project-cli-observability-security.md`.
- `.10x/specs/run-orchestration-ledger.md`.
- `.10x/specs/resource-authoring-planning-batches.md`.
- `.10x/specs/conformance-governance-roadmap.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.
- P1 directive attachment `/Users/alexanderbut/.codex/attachments/d7a887db-710d-44db-a630-90428560d519/pasted-text.txt`, mirrored by the active Codex goal objective file read on 2026-07-08.

## Hard guardrails

- The `--json` contract is sacred: machine-mode envelopes, exit-code taxonomy, and existing field names remain stable. JSON changes are additive only.
- Redaction binds every renderer, progress line, panel, error, and verbose trace.
- Headless mode degrades cleanly: no TTY means no ANSI or spinners, `NO_COLOR` and `--no-color` are honored, and piped stdout remains readable.
- Rendering and wall-clock display MUST NOT affect deterministic package artifacts, hashes, receipts, checkpoints, or goldens.
- Once the rendering system merges, no new command may land on the old raw human-output path.

## Workstreams

- `.10x/tickets/2026-07-08-p1-product-ws1-runtime-event-spine.md`
- `.10x/tickets/2026-07-08-p1-product-ws2-command-grammar-redesign.md`
- `.10x/tickets/2026-07-08-p1-product-ws3-rendering-system-design-language.md`
- `.10x/tickets/2026-07-08-p1-product-ws4-error-experience-catalog.md`
- `.10x/tickets/2026-07-08-p1-product-ws5-live-progress.md`
- `.10x/tickets/2026-07-08-p1-product-ws6-docs-onboarding.md`
- `.10x/tickets/2026-07-08-p1-product-ws7-python-front-door.md`
- `.10x/tickets/2026-07-08-p1-product-ws8-release-engineering.md`

## Sequencing

WS1 runs first because live progress and the observability bridge depend on the run event spine.

WS2 and WS3 may proceed in parallel after this program is opened. WS4 lands with WS3's rollout. WS5 depends on WS1 and WS3. WS6, WS7, and WS8 may start immediately, with their generated-reference and CI-artifact closure criteria depending on the grammar, error catalog, and release pipeline slices they consume.

Existing non-CLI program lanes may continue, but any CLI-adjacent ticket opened after WS2/WS3 merge MUST adopt the new grammar and rendering path.

## Acceptance criteria

- Every command renders through the design system in both TTY and headless modes with snapshots.
- The grammar table's shortest forms all parse and resolve.
- No command requires a user-minted identifier when the system can mint or derive it.
- Live progress ships for run, replay, resume, and backfill.
- The error catalog is generated into docs and every `CliError` construction site has a stable code.
- `python://` resources run end to end through `cdf run`, plan, and preview surfaces.
- The docs quickstart is executable by a stranger.
- The release pipeline has cut a checksummed pre-release with completions and man pages.
- The Chapter 23 demonstration has a recorded terminal session through the new experience.

## Evidence expectations

Each workstream records focused evidence and adversarial review. Parent closure requires aggregate evidence mapping every acceptance criterion, coverage-matrix updates, redaction adversarial output, TTY and headless snapshots, generated-reference freshness checks, CI/release evidence, and a final review that verifies the old raw rendering path is no longer used for command output.

## Explicit exclusions

No dashboard or GUI. No breaking changes to JSON output. No scheduler semantics. No OTLP exporter implementation unless a later child ticket ratifies and scopes it; WS1 only creates the tracing bridge and a feature-flag follow-up for OTLP export.

## Progress and notes

- 2026-07-08: Opened from the P1 directive after the P0 structural-debt program was closed. The existing CLI surface is considered functionally broad but product-experience-incomplete; this program owns the experience, instrumentation, documentation, Python front door, and release surface gap.
- 2026-07-08: Activation evidence recorded in `.10x/evidence/2026-07-08-p1-product-experience-program-activation.md`; activation review recorded in `.10x/reviews/2026-07-08-p1-product-experience-program-activation-review.md`.
- 2026-07-08: WS7 Python front-door shaping added `.10x/specs/python-front-door-product-surface.md` and child tickets WS7A-WS7D, with evidence/review under `.10x/evidence/2026-07-08-p1-product-ws7-python-front-door-shaping.md` and `.10x/reviews/2026-07-08-p1-product-ws7-python-front-door-shaping-review.md`.
- 2026-07-08: WS4 error-catalog shaping added `.10x/specs/cli-error-experience-catalog.md` and child tickets WS4A-WS4D, with evidence/review under `.10x/evidence/2026-07-08-p1-product-ws4-error-catalog-shaping.md` and `.10x/reviews/2026-07-08-p1-product-ws4-error-catalog-shaping-review.md`.
- 2026-07-08: WS5 live-progress shaping added `.10x/specs/cli-live-progress.md` and child tickets WS5A-WS5D, with evidence/review under `.10x/evidence/2026-07-08-p1-product-ws5-live-progress-shaping.md` and `.10x/reviews/2026-07-08-p1-product-ws5-live-progress-shaping-review.md`.
- 2026-07-08: WS1 remaining event-spine shaping added `.10x/specs/runtime-event-spine.md` and child tickets WS1B-WS1F, with evidence/review under `.10x/evidence/2026-07-08-p1-product-ws1-remaining-event-spine-shaping.md` and `.10x/reviews/2026-07-08-p1-product-ws1-remaining-event-spine-shaping-review.md`.
- 2026-07-08: User reiterated that the current human CLI output is the weakest product surface and that hand-authored docs should not continue to outrun the renderer/live-progress/error experience. After the already-scoped WS6D init README scaffold, prioritize WS3 renderer foundation and WS5 prerequisites over additional prose-only docs work; generated docs remain tied to WS2/WS4 freshness.
- 2026-07-08: WS1B and WS1C closed, giving the runtime event spine durable fanout plus lifecycle/payload breadth. WS2B and WS2C closed, giving the clap parser foundation and product grammar semantics. WS3B and WS3C closed, giving the renderer foundation and the first high-value plan/run/replay rendering migration. WS3D and WS1E are now active worker lanes.

## Blockers

None for parent activation. Workstream implementation tickets may carry technical dependencies.
