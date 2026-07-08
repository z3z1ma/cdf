Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws6-docs-onboarding.md
Depends-On: .10x/specs/docs-onboarding-surface.md, .10x/tickets/done/2026-07-08-p1-product-ws6a-docs-topology-quickstart.md

# P1 product WS6D: Init README scaffold

## Scope

Update `cdf init` so new projects include a README pointing users into the in-repository quickstart and supported first commands.

## Acceptance criteria

- `cdf init` creates a README unless an existing README would be overwritten without `--force`.
- The README points to `docs/quickstart.md` and names only supported commands.
- The scaffold contains no secrets, generated runtime state, absolute local paths, or machine-specific assumptions.
- Existing init JSON and exit-code behavior remains compatible.
- Init tests cover fresh, existing-no-force, and force replacement behavior for README handling.

## Evidence expectations

Focused init tests, JSON compatibility proof, and scaffold content review.

## Explicit exclusions

No docs content implementation beyond README text. No command grammar changes.

## Blockers

None. The WS6A quickstart dependency is complete at
`.10x/tickets/done/2026-07-08-p1-product-ws6a-docs-topology-quickstart.md`.

## Progress and notes

- 2026-07-08: Implemented README scaffolding in `cdf-project`, preserving the
  existing `cdf-cli` output boundary. `README.md` is now created for fresh init,
  blocks unforced overwrite, and is replaced under `--force`.
- 2026-07-08: Updated focused init tests for fresh, existing-no-force, and force
  replacement README behavior. Tests also prove README content points to
  `docs/quickstart.md`, names supported commands, and avoids secrets/local root
  interpolation.
- 2026-07-08: Updated only the narrow quickstart note/output that became false
  once init creates `README.md`.
- 2026-07-08: Evidence recorded in
  `.10x/evidence/2026-07-08-p1-product-ws6d-init-readme-scaffold.md`.
  Closure review passed in
  `.10x/reviews/2026-07-08-p1-product-ws6d-init-readme-scaffold-review.md`.
- 2026-07-08: Retrospective: no new reusable project knowledge, operational
  skill, or follow-up ticket was needed. The only notable friction was broad
  `jscpd` noise from pre-existing duplicate blocks in the large CLI test module;
  the scaffold/docs-focused duplication scan passed and the broad findings are
  outside this ticket's edited range.
