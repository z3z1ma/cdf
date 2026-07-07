Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md
Verdict: pass

# Native Arrow/DataFusion Parquet policy review

## Target

Review of the ratified policy transition from the DuckDB-backed Parquet workaround to native Arrow/DataFusion Parquet with a narrow `RUSTSEC-2024-0436` exception.

## Assumptions tested

- The user ratification is explicit enough to supersede the prior no-advisory-ignore posture for this single advisory path.
- The exception is scoped to `paste 1.0.15` through arrow-rs/DataFusion Parquet, not to all unmaintained advisories.
- The implementation is split into executable tickets rather than bundled into a broad policy/code rewrite.
- Existing DuckDB-backed Parquet code remains in place until replacement tickets produce evidence.

## Findings

No unresolved findings.

The decision preserves the important safety rail: advisory scanners remain mandatory, and evidence must prove only the ratified `paste` advisory is accepted. The split tickets avoid changing `deny.toml`, dependencies, readers, writers, and archive behavior in one unreviewable step.

## Verdict

Pass. The shaping ticket can close as a ratified policy decision with bounded implementation owners.

## Residual risk

The exception will create intentional scanner noise until tool policy is updated to ignore only this advisory. That is now handled by `.10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md`.
