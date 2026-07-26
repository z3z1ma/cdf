Status: done
Created: 2026-07-11
Updated: 2026-07-25
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-11-p3-z1-envelope-evidence-reconciliation.md

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

None. Z1 is terminal with an honest generated envelope; non-green cells remain explicit.

## References

- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/performance-lab-and-envelope.md`

## Journal

- 2026-07-25: Reconciled the final host-labelled TLC and 1.0086 TiB observations instead of
  provisioning another benchmark host. The TLC demo preserves exact source/configuration,
  commands, phase profile, raw/default/local comparisons, package replay, and the explicitly
  accepted composite-target residual. The TiB demo preserves its generator, enforced memory
  topology, exact package/receipt/checkpoint, discovery cardinality, cleanup, and the hard-link
  limit that prevents a false unique-byte device-saturation claim.
- 2026-07-25: Performed the adversarial review across all ten requested categories and traced the
  generic source-task, bounded runtime, package, destination-capability, receipt, and checkpoint
  topology. No critical/high issue remains in the implemented P3 scope. Four non-green envelope
  results remain visible with accepted or explicit no-action rationale.

## Evidence

- `.10x/evidence/2026-07-25-p3-z2-scale-demo-adversarial-review.md` maps every acceptance category
  to reproducible machine evidence or a precisely bounded residual.
- TLC terminal run:
  `.10x/evidence/.storage/2026-07-19-p3-g4-hf-complete-final-clean.json`.
- One-TiB terminal summary:
  `.10x/evidence/.storage/2026-07-25-p3-f4-ec2-1t-summary.json`.
- Honest envelope authority: `docs/performance-envelope.md`.

## Review

Verdict: pass with recorded residuals.

Fresh-hat review tried to falsify the scale claims by separating logical from physical bytes,
overlapping phase duration from wall time, product success from retained identifier text, and
component overhead from aggregate overhead. It found no unsupported critical/high claim after
those limits were made explicit. It confirmed that the source/destination-specific mechanisms
remain behind codec, task, and ingress capability boundaries.

Residual risk: the remote public mirror is one provider/host observation; the one-TiB fixture is
not a unique-byte storage stress; the final remote report did not retain ephemeral lifecycle ids;
and the aggregate overhead cell remains not demonstrated. Those limitations are stated in both
the Z2 evidence and final envelope.

## Retrospective

The useful closure move was not another expensive run. It was joining already-authoritative raw
observations without erasing their bias labels. Scale evidence is easy to overclaim: overlapping
phase times, logical hard-link bytes, and successful receipt-gated execution each prove something
different. Future closeouts should require one compact machine summary per scale law and preserve
negative cells as first-class outputs.
