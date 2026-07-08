Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-contract-depth-program.md
Depends-On: .10x/tickets/2026-07-08-p1-e2-quarantine-routing-redaction.md

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

## Blockers

None once E2 is closed.
