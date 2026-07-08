Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Historical Gitleaks Findings

Full-history Gitleaks scans currently report two known `generic-api-key` findings from removed Python-era files:

- `7fd1eddf7e6ab65afb4b9c63556d49e95ff5d50e:src/cdf/core/project.py:generic-api-key:158`
- `3e93ea80bfad6cd7a905b4a15ea9c3f6adb01dd8:src/cdf/core/feature_flag/harness.py:generic-api-key:47`

Triage evidence in `.10x/evidence/2026-07-08-historical-gitleaks-findings-triage.md` classifies both as false positives on Harness SDK-key schema/config field declarations, not committed credential values or private-key material.

The historical files are absent from the current tree. Current-tree, tracked-source, and staged-diff secret scans remain hard gates. Any new Gitleaks finding outside the exact fingerprints above is not covered by this record and must be triaged as a potential secret until proven otherwise.

Do not rewrite repository history or rotate credentials for these two findings based on current evidence. Do not add a broad scanner allowlist. If full-history Gitleaks becomes a required passing release gate, use a narrow baseline or exact-fingerprint exception tied to this knowledge record and keep source/staged scans mandatory.
