Status: superseded
Created: 2026-07-11
Updated: 2026-07-11

# Performance comparison variance and baseline policy

Superseded by `.10x/decisions/performance-comparison-variance-and-baseline-policy-v2.md` because exact full-key equality incorrectly prohibited comparison across CDF revisions.

## Context

The P3 spec requires median-of-N comparison, failure above 10% regression, high-variance inconclusiveness, and evidence-backed baseline replacement, but does not quantify high variance or the exact threshold boundary. CI and envelope generation need one deterministic policy.

## Decision

- Two observations are comparable only when report schema version, complete comparability key, and reference identity are exactly equal and both cells are observed with summaries derived from retained samples.
- An observation is high variance when median absolute deviation is strictly greater than 10% of median wall time. If either side is high variance, the comparison is inconclusive. Exactly 10% remains comparable.
- A current wall-time median strictly greater than 110% of baseline fails as a regression. Exactly 110% passes the threshold boundary. Improvements do not erase variance or comparability failures.
- Baseline reports are stored by canonical SHA-256. Replacement requires a `.10x/evidence/*.md` reference, appends a new index entry, and never overwrites or deletes a prior report.
- Missing, failed, timed-out, unavailable, or inconclusive cells stay visible and cannot produce a pass verdict.

## Alternatives considered

- Standard deviation/CV: rejected because macro samples are small and outlier-sensitive; MAD matches the report schema and is robust.
- A looser 20% variance cutoff: rejected because it would let unstable CI measurements masquerade as evidence.
- Treat exactly 10% regression as failure: rejected because the program contract says greater than 10%.
- Mutable `baseline.json`: rejected because it allows silent resets and destroys prior distributions.

## Consequences

Noisy hosts create explicit inconclusive results rather than flaky pass/fail claims. Baseline history is auditable and storage grows by small report artifacts. Changing the threshold requires a superseding decision and old/new distribution evidence.
