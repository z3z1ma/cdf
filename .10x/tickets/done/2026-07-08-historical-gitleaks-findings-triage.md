Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: None

# Triage historical Gitleaks findings

## Scope

Triage the two full-history `gitleaks detect` findings that recur in repository-level scans:

- `generic-api-key` in historical path `src/cdf/core/project.py`, commit `7fd1eddf7e6ab65afb4b9c63556d49e95ff5d50e`.
- `generic-api-key` in historical path `src/cdf/core/feature_flag/harness.py`, commit `3e93ea80bfad6cd7a905b4a15ea9c3f6adb01dd8`.

Own the repository-level decision on whether these are false positives, require credential rotation, require history rewrite, or should be documented as accepted historical noise for current source-focused closure gates.

## Acceptance criteria

- Confirm whether either historical value was a real credential, token, or private key.
- If any value was real and could still be live, record the rotation or revocation evidence without storing the secret.
- Decide and record whether repository history should be rewritten, preserved with an allowlist, or handled through a documented full-history scanner exception.
- Update the quality-gate knowledge or scanner config so future tickets do not repeatedly rediscover the same two findings without context.
- Keep current-tree and staged-diff secret scans as hard gates for implementation slices.

## Evidence expectations

Record the exact `gitleaks` report metadata, current-tree absence checks, any rotation/revocation confirmation, and the final scanner/config/knowledge change.

## Explicit exclusions

Do not rewrite git history, rotate credentials, or add scanner allowlist entries without explicit user approval for that action.

## Blockers

None for source-level implementation closure. History rewrite or credential rotation may require user approval after triage.

## Progress and notes

- 2026-07-08: Opened from the CLI plan/explain closure quality pass. Full-history `gitleaks detect` reported the two findings above, while focused source-only scans over the touched CLI/project/destination paths passed and the historical paths are absent from the current tree.
- 2026-07-08: Triage classified both findings as false positives on Harness SDK-key schema/config field declarations, not committed credential values or private-key material. Current tracked-source Gitleaks scan passed with zero findings. Recorded evidence `.10x/evidence/2026-07-08-historical-gitleaks-findings-triage.md`, review `.10x/reviews/2026-07-08-historical-gitleaks-findings-triage-review.md`, and knowledge `.10x/knowledge/historical-gitleaks-findings.md`; preserved history and did not add a broad scanner allowlist.
