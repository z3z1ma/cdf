Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-contract-depth-program.md
Depends-On: .10x/tickets/done/2026-07-08-p1-e3-merge-dedup-live-path.md, .10x/tickets/2026-07-08-p1-e4-variant-capture-evolution-event.md, .10x/tickets/2026-07-08-p1-e5-trust-ring-ledger-events.md

# P1 E6: Drift-quarantine conformance scenario

## Scope

Add the end-to-end drift-quarantine conformance scenario required by P0 Workstream E.

Owns:

- `crates/cdf-conformance/**` scenario fixtures and assertions;
- focused fixture/project files needed for the scenario;
- golden or evidence records only where deterministic outputs are intentionally snapshotted.

## Governing records

- `VISION.md` Chapter 11.
- `VISION.md` Chapter 20.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/package-lifecycle-determinism.md`.
- `.10x/specs/destination-receipts-guarantees.md`.
- `.10x/specs/run-orchestration-ledger.md`.
- `.10x/specs/conformance-governance-roadmap.md`.
- `.10x/decisions/contract-live-verdict-execution-semantics.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Acceptance criteria

- The scenario freezes a resource contract, drifts a fixture type, and proves the violating rows route to package quarantine.
- Accepted rows continue to package, destination commit, receipt verification, and checkpoint gating.
- Package evidence includes validation program, verdict/quarantine summaries, quarantine artifacts, dedup evidence where applicable, and trust-ring events where triggered.
- Destination quarantine mirror behavior is asserted where the sheet supports it and explicitly excluded where unsupported.
- The scenario is wired into the conformance cadence, and any new runtime path it depends on is covered per `.10x/knowledge/runtime-conformance-throughput-rule.md`.
- Workstream E aggregate evidence and adversarial review can close after this scenario and prior child evidence pass.

## Evidence expectations

Record conformance scenario output, package artifact inspection, destination receipt verification, checkpoint evidence, duplicate/replay behavior where affected, jscpd and `rust-code-analysis-cli` metrics, security scans for redaction, and adversarial review.

## Explicit exclusions

No public demo script unless split from the broader MVP killer-demo owner. No new source archetype or destination.

## Blockers

None once E3, E4, and E5 are closed.
