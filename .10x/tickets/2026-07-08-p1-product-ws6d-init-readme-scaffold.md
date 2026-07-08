Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws6-docs-onboarding.md
Depends-On: .10x/specs/docs-onboarding-surface.md, .10x/tickets/2026-07-08-p1-product-ws6a-docs-topology-quickstart.md

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

Depends on the quickstart path from WS6A so the scaffold does not point to a missing document.
