Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-contract-depth-program.md
Depends-On: .10x/tickets/done/2026-07-08-p1-e4-variant-capture-evolution-event.md

# P1 E5: Trust-ring promotion and demotion ledger events

## Scope

Record validation-depth promotion and demotion as run-ledger events driven by compiled promotion policy and live verdict outcomes.

Owns:

- `crates/cdf-contract/src/program.rs` transition event use and summaries;
- `crates/cdf-project/src/runtime/ledger.rs` event kinds/details;
- `crates/cdf-project/src/runtime/orchestration.rs` or adjacent runtime modules for event emission;
- inspect/run evidence surfaces where required by existing run-ledger contracts;
- focused tests for promotion/demotion triggers.

## Governing records

- `VISION.md` Chapter 11.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/package-lifecycle-determinism.md`.
- `.10x/specs/destination-receipts-guarantees.md`.
- `.10x/specs/run-orchestration-ledger.md`.
- `.10x/decisions/run-ledger-commit-session-spine.md`.
- `.10x/decisions/contract-live-verdict-execution-semantics.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Acceptance criteria

- New resources run discovery/full validation according to compiled trust policy.
- Clean stable runs may promote only after the configured consecutive count and stable schema hash.
- Drift, anomaly, or quarantine events demote when the compiled promotion policy says so.
- Every promotion/demotion is recorded as a redacted run-ledger event with resource id, from/to depth, trigger, schema hash where known, and package/run pointers where available.
- Ledger events are evidence only; checkpoint state advancement still occurs solely through the receipt-gated checkpoint store.

## Evidence expectations

Record ledger event tests, inspect-run redaction checks if output changes, package/run evidence mapping, jscpd and `rust-code-analysis-cli` metrics, direct unsafe scan, and adversarial review.

## Explicit exclusions

No UI. No sampled-fast-path performance optimization unless needed to represent the event model. No new run-ledger backend.

## Progress and Notes

- 2026-07-08: Activated after E4 closure. Parent orchestrator delegated the core project/state ledger implementation to a focused worker and is retaining integration, quality evidence, review, and closure ownership.
- 2026-07-08: Partial implementation landed for `validation_depth_transition_recorded` run-ledger events, SQLite run-ledger schema v2 migration, first-contact `new_resource`, clean-stable promotion, drift demotion, quarantine demotion, run/inspect event-detail visibility, and focused tests. Partial evidence recorded at `.10x/evidence/2026-07-08-p1-e5-trust-ledger-partial.md`.
- 2026-07-08: Unblocked anomaly-spike semantics with `.10x/decisions/contract-anomaly-signal-demotion-policy.md`: anomaly demotion consumes explicit anomaly facts only and MUST NOT infer anomalies from row counts, quarantine counts, destination failures, elapsed time, or arbitrary package-profile heuristics.
- 2026-07-08: Implemented explicit anomaly facts on the validation program as the current explicit-signal seam, with `metric`, `observed`, `threshold`, and `window` fields. Runtime trust transition evaluation records `anomaly_spike` demotion only when `demote_on_anomaly = true`, the prior committed head has earned sampled-fast-path status, and an explicit anomaly fact is present. No detector, threshold derivation, row-count heuristic, quarantine-count heuristic, or destination-failure heuristic was added.
- 2026-07-08: Added focused live-run tests proving explicit anomaly demotion and no anomaly demotion without explicit facts, preserved drift/quarantine behavior, repaired the conformance property-fuzz manual `ValidationProgram` constructor, and reduced repeated trust-ring request construction with a local test helper.
- 2026-07-08: Closure evidence recorded in `.10x/evidence/2026-07-08-p1-e5-trust-ledger-events.md`; adversarial review recorded in `.10x/reviews/2026-07-08-p1-e5-trust-ledger-events-review.md`.

## Blockers

None.
