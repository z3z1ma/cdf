Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-11-p3-z1-envelope-evidence-reconciliation.md

# P3 Z2: scale demonstrations and adversarial performance review

## Scope

Record the before/after full-year TLC HTTPS-glob-to-DuckDB demonstration, execute/attach the 1 TB synthetic glob-to-Parquet profile under default budget, run an adversarial workload suite explicitly designed to embarrass the envelope, and review architecture/performance/correctness jointly.

## Acceptance criteria

- TLC demo includes exact commands/config, source identities, host/network/cache labels, raw profile, package/receipt/checkpoint proof, baseline comparison, and replay.
- 1 TB demo includes generated recipe, RSS/cgroup/ledger/spill curve, throughput/core/device saturation, verified destination/package/receipt/checkpoint, and cleanup.
- Adversarial suite includes tiny-file/cardinality, wide/nested, high compression, malformed/quarantine-heavy, all-unique dedup, skew, slow destination, remote latency, foreign boundary, and mixed-schema cases.
- No critical/high finding remains unresolved; lesser residual risks have durable owners or explicit measured no-action rationale.
- Demo assets are reproducible pointers/artifacts, not prose-only claims or committed giant datasets.

## Evidence expectations

Recorded sessions, raw reports/profiles/traces, checksums, generated recipes, package/receipt/state verification, adversarial review record, resolved-finding evidence, and release-note-ready comparison.

## Explicit exclusions

No distributed/exabyte throughput claim, edited timing, warm/cold ambiguity, or manual success assertion without artifacts.

## Blockers

Blocked on Z1 green envelope/reconciliation.

## References

- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/performance-lab-and-envelope.md`
