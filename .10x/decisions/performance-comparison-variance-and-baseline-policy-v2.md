Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Performance comparison variance and baseline policy v2

Supersedes `.10x/decisions/superseded/performance-comparison-variance-and-baseline-policy.md`.

## Context

The P3 spec requires median-of-N comparison, failure above 10% regression, high-variance inconclusiveness, and evidence-backed baseline replacement. The initial decision incorrectly required exact full comparability-key equality, including CDF revision, which would prohibit the intended comparison across code changes.

## Decision

- Two observations are comparable only when report schema version, dataset, workload, timed-region version, dependency tuple, host class, OS/toolchain, I/O mode, and reference identity are exactly equal and both cells are observed with summaries derived from retained samples. CDF revision is the one intentional key delta and both revisions remain recorded.
- An observation is high variance when median absolute deviation is strictly greater than 10% of median wall time. If either side is high variance, the comparison is inconclusive. Exactly 10% remains comparable.
- A current wall-time median strictly greater than 110% of baseline fails as a regression. Exactly 110% passes the threshold boundary. Improvements do not erase variance or comparability failures.
- Baseline reports are stored by canonical SHA-256. Replacement requires an existing `.10x/evidence/*.md` reference, appends a new index entry, and never overwrites or deletes a prior report.
- Missing, failed, timed-out, unavailable, or inconclusive cells stay visible and cannot produce a pass verdict.

## Alternatives considered

- Compare only identical CDF revisions: rejected because it cannot detect regressions caused by a change.
- Ignore dependency/toolchain deltas: rejected because movement could come from the environment rather than CDF.
- Standard deviation/CV: rejected because macro samples are small and outlier-sensitive; MAD matches the report schema and is robust.
- A looser 20% variance cutoff: rejected because it would let unstable CI measurements masquerade as evidence.
- Treat exactly 10% regression as failure: rejected because the program contract says greater than 10%.
- Mutable `baseline.json`: rejected because it allows silent resets and destroys prior distributions.

## Consequences

Comparisons measure a CDF revision delta while holding every other authority fixed. Noisy hosts create explicit inconclusive results rather than flaky claims. Baseline history is auditable and storage grows only by small report artifacts. Changing the threshold requires a superseding decision and old/new distribution evidence.
