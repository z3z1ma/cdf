Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-contract-depth-program.md
Depends-On: .10x/tickets/done/2026-07-08-p1-e1-row-level-verdicts-live-chain.md, .10x/tickets/done/2026-07-08-p1-e2-quarantine-routing-redaction.md

# P1 E3: Deterministic pre-merge dedup in the live path

## Scope

Apply contract `Dedup { keys, keep }` rules to accepted rows before merge destination mutation.

Owns:

- `crates/cdf-contract/**` dedup rule evaluation and summaries;
- `crates/cdf-engine/src/execution.rs` accepted-row ordering and package evidence;
- `crates/cdf-project/src/runtime/destinations/**` only where merge planning must consume dedup evidence;
- conformance tests for append/replace non-effect and merge determinism.

## Governing records

- `VISION.md` Chapter 11.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/package-lifecycle-determinism.md`.
- `.10x/specs/destination-receipts-guarantees.md`.
- `.10x/specs/run-orchestration-ledger.md`.
- `.10x/decisions/destination-introspection-package-and-cli-policy.md`.
- `.10x/decisions/contract-live-verdict-execution-semantics.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Acceptance criteria

- Dedup runs after row verdict filtering and before destination merge commit.
- `keep = first` and `keep = last` use decision-ratified package order; `keep = fail` aborts before destination mutation.
- Dedup summaries are written as package evidence and visible to replay/inspect paths without re-running extraction.
- Existing destination-level duplicate protections remain as safety rails and do not invent semantics absent from the compiled program.
- Replay of a package uses recorded deduped segments and does not re-evaluate dedup against a changed destination state.

## Evidence expectations

Record focused unit tests, live run tests for merge redelivery determinism, replay identity checks, jscpd and `rust-code-analysis-cli` metrics, direct unsafe scan, and relevant conformance output.

## Explicit exclusions

No `cdc_apply` semantics. No destination-specific inference of merge keys or dedup policy. No performance optimization beyond preserving vectorized/batch behavior.

## Blockers

None; E1 is closed at `.10x/tickets/done/2026-07-08-p1-e1-row-level-verdicts-live-chain.md` and E2 is closed at `.10x/tickets/done/2026-07-08-p1-e2-quarantine-routing-redaction.md`.

## Progress and notes

- 2026-07-08: Activated after E2 closure in `6552e6a7`; assigned as the next ordered P1 contract-depth child slice.
- 2026-07-08: Closed with deterministic package-order dedup in `cdf-contract`, merge-only live application in `cdf-engine`, identity-participating `stats/dedup-summary.json` package evidence, live DuckDB merge replay/redrive coverage in `cdf-project`, and a compatibility default for legacy serialized `EnginePlan` JSON missing `write_disposition`. Evidence: `.10x/evidence/2026-07-08-p1-e3-merge-dedup-live-path.md`. Review: `.10x/reviews/2026-07-08-p1-e3-merge-dedup-live-path-review.md`.
