Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/specs/versioning-lts-release-policy.md, .10x/tickets/done/2026-07-08-p1-product-ws8a-ci-quality-workflows.md, .10x/tickets/done/2026-07-08-p1-product-ws8b-release-artifact-workflow.md, .10x/tickets/done/2026-07-08-p1-product-ws8c-changelog-installer-channel.md
Verdict: pass

# P1 WS8 release shaping review

## Target

The WS8 release-engineering shaping slice: versioning/LTS spec plus CI, release-artifact, and installer/changelog child tickets.

## Findings

- pass: The spec preserves the existing publication block for crates.io while DataFusion remains pinned to git.
- pass: The spec keeps artifact-version compatibility stronger than pre-1.0 Rust API compatibility, matching the conformance-governance spec.
- pass: The CI child explicitly carries the reusable CodeQL database requirement and avoids committing generated scanner outputs.
- pass: The release workflow child separates binary pre-release artifacts from crate publication and gates completions/man pages on WS2D.
- pass: The installer child requires checksum verification before mutation and avoids privileged installation assumptions.
- minor: Actual release workflow target feasibility is not proven yet. WS8B correctly requires target gaps to be recorded with evidence rather than silently skipped.

## Verdict

Pass. The shaping is sufficiently bounded for child execution and does not implement release behavior in the same slice as the new governing spec.

## Residual risk

The CI/release workflow details may expose platform-specific constraints once implementation starts. Those constraints belong in WS8A/WS8B evidence and should not weaken the active versioning/LTS policy without a superseding record.
