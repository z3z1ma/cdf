Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Contract anomaly signal demotion policy

## Context

`VISION.md` Chapter 11 says trust-ring validation depth demotes on drift, anomaly spike, or quarantine event. `.10x/specs/types-contracts-normalization.md` and `.10x/decisions/contract-live-verdict-execution-semantics.md` preserve that requirement, but they do not define a heuristic anomaly detector, thresholds, or ownership for deriving an anomaly spike from nearby counters.

At decision time, P1 E5 was partially implemented and blocked on this boundary. The runtime already recorded drift and quarantine demotions from explicit package/runtime facts. It also had a `ValidationTransitionTrigger::AnomalySpike` vocabulary value, but no runtime fact proved an anomaly spike.

## Decision

Trust-ring anomaly demotion consumes explicit anomaly evidence only. E5 MUST NOT infer anomaly spikes from row counts, quarantine counts, destination failures, elapsed time, or arbitrary package-profile heuristics.

An anomaly spike is a fact emitted by `ProfileExec`, a contract/profile evaluator, or a future anomaly detector into the live run evidence stream. The minimal event payload is:

- `metric`: stable metric identifier;
- `observed`: observed value as a redacted string or number;
- `threshold`: threshold as a redacted string or number;
- `window`: stable comparison window label or identifier.

When a compiled `PromotionPolicy` has `demote_on_anomaly = true` and at least one explicit anomaly fact is present for the current package, the runtime records a validation-depth demotion event with trigger `anomaly_spike`. The run-ledger event carries the usual resource, package, run, checkpoint, schema-hash, and depth fields, plus the anomaly fact fields. These details are evidence only and MUST NOT advance checkpoint state except through the existing receipt-gated checkpoint store.

If no explicit anomaly fact is present, E5 records no anomaly demotion. That absence is not a clean-run override; drift and quarantine demotion still apply independently.

## Alternatives considered

Infer anomaly spikes from quarantine count.

- Rejected because quarantine is already its own demotion trigger. Conflating them would double-count one mechanism and hide which condition actually fired.

Infer anomaly spikes from row-count or null-count deltas.

- Rejected because no active record defines a baseline, threshold, metric owner, or comparison window. Encoding one in E5 would invent product semantics and make passing tests ratify a guess.

Treat destination commit failures as anomaly spikes.

- Rejected because destination failures belong to the run/destination failure taxonomy. Trust depth is about validation policy for data admitted to the package.

Defer all anomaly vocabulary.

- Rejected because the book, spec, and compiled policy already expose `demote_on_anomaly`. The correct MVP seam is an explicit signal input and ledger representation, not removal.

## Consequences

P1 E5 can close by wiring explicit anomaly facts into trust-ring transition evaluation and tests without building a detector.

Future profiling/anomaly work can emit the same fact shape without changing run-ledger transition semantics.

The first E5 implementation may use a focused test-only or package-evidence injection seam to prove the trigger. Production anomaly detection remains a separate ticket because thresholds and baselines are not ratified here.
