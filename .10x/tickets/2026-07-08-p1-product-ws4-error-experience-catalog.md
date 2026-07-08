Status: active
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-experience-program.md
Depends-On: .10x/specs/cli-error-experience-catalog.md, .10x/tickets/2026-07-08-p1-product-ws3-rendering-system-design-language.md

# P1 product WS4: Error experience and catalog

## Scope

Make CLI errors structured, stable, remedial, redacted, and documented without changing the existing exit-code taxonomy.

Implementation may split into catalog spec, `CliError` migration, suggestion engine, renderer integration, and generated docs child tickets.

## Child tickets

- `.10x/tickets/done/2026-07-08-p1-product-ws4a-error-envelope-foundation.md`
- `.10x/tickets/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`
- `.10x/tickets/2026-07-08-p1-product-ws4c-error-suggestions.md`
- `.10x/tickets/2026-07-08-p1-product-ws4d-error-rendering-docs.md`

## Required outcomes

- Define a stable error-code catalog layered onto the existing error kind and exit-code taxonomy.
- Human errors show what failed, why, redaction-safe offending values or locations, and what to do next.
- Unknown commands, resources, and targets use edit-distance suggestions over project inventory where available.
- `--json` errors gain additive `code` and `remediation` fields.
- Not-yet-supported paths keep exit 78 and name the owning ticket or layer.
- The catalog generates a reference page consumed by WS6 docs.

## Acceptance criteria

- Every `CliError` construction site carries a stable code.
- Snapshot tests cover each error kind and representative remediation line.
- Suggestion tests cover unknown command/resource/target cases.
- Redaction adversarial tests prove secrets do not appear in errors or remediation output.

## Evidence expectations

Record catalog spec or decision, generated docs proof, per-kind snapshots, construction-site coverage tests, suggestion tests, and redaction evidence.

## Explicit exclusions

No exit-code changes. No breaking JSON field removals or renames. No broad CLI grammar migration outside WS2.

## Progress and notes

- 2026-07-08: Opened from P1 product directive. This workstream lands with WS3's renderer rollout.
- 2026-07-08: Shaped `.10x/specs/cli-error-experience-catalog.md` and split execution into WS4A-WS4D child tickets. Shaping evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws4-error-catalog-shaping.md`; shaping review recorded in `.10x/reviews/2026-07-08-p1-product-ws4-error-catalog-shaping-review.md`.
- 2026-07-08: WS4A envelope foundation completed; evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws4a-error-envelope-foundation.md` and review recorded in `.10x/reviews/2026-07-08-p1-product-ws4a-error-envelope-foundation-review.md`.

## Blockers

Final human presentation and generated docs depend on WS3 renderer and WS6 generated-reference lanes. WS4A may proceed independently.
