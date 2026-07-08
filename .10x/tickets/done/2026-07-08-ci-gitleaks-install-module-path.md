Status: done
Created: 2026-07-08
Updated: 2026-07-08

# CI Gitleaks install module path

## Scope

Fix GitHub Actions quality workflows that install `gitleaks` with the wrong Go module path for the pinned `v8.18.4` release.

## Acceptance criteria

- Fast and slow quality workflows install the pinned Gitleaks version from the module path declared by that release.
- The fix does not change the Gitleaks version or quality-gate semantics.
- Local verification proves the corrected `go install` command resolves.

## Explicit exclusions

No Gitleaks version upgrade. No quality-gate behavior changes. No release workflow changes.

## Progress and notes

- 2026-07-08: CI failure showed `go install github.com/gitleaks/gitleaks/v8@v8.18.4` conflicts with the module path declared by that release, `github.com/zricethezav/gitleaks/v8`.
- 2026-07-08: Updated fast and slow quality workflows to install `github.com/zricethezav/gitleaks/v8@v8.18.4`, preserving the pinned version.
- 2026-07-08: Verified the corrected install locally, parsed the edited workflow YAML, ran a scoped Gitleaks scan over the changed files, and recorded evidence in `.10x/evidence/2026-07-08-ci-gitleaks-install-module-path.md`.

## Blockers

None.

## Evidence

- `.10x/evidence/2026-07-08-ci-gitleaks-install-module-path.md`

## Review

- `.10x/reviews/2026-07-08-ci-gitleaks-install-module-path-review.md`
