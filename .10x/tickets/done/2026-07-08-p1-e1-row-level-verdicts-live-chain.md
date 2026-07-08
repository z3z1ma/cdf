Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/done/2026-07-08-p1-contract-depth-program.md

# P1 E1: Row-level verdicts in the live operator chain

## Scope

Implement the compiled row-verdict evaluator and wire it into the live engine path before normalization.

Owns:

- `crates/cdf-contract/src/program.rs`;
- `crates/cdf-contract/src/compiler.rs`;
- new focused `cdf-contract` modules if needed for row rule evaluation;
- `crates/cdf-engine/src/execution.rs`;
- focused contract/engine/conformance tests for row-verdict behavior.

## Governing records

- `VISION.md` Chapter 11.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/package-lifecycle-determinism.md`.
- `.10x/specs/destination-receipts-guarantees.md`.
- `.10x/specs/run-orchestration-ledger.md`.
- `.10x/decisions/contract-live-verdict-execution-semantics.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Acceptance criteria

- `ValidationProgram` serializes executable row rule programs for nullability, domain/enum, range, regex, freshness, and dedup metadata without requiring the original `ContractPolicy` at execution time.
- `cdf-contract` exposes a pure Arrow batch evaluator returning accepted row selection, quarantine candidates, and verdict summary from `ValidationProgram` plus `ContractEvaluationContext`.
- `cdf-engine` calls the evaluator from `ContractExec` in the live package path. Accepted rows continue to `NormalizeExec`; reject-batch and reject-run abort before package finalization and destination mutation.
- Freshness uses the decision-ratified package execution `observed_at_ms` context and fails closed for missing context or incompatible timestamp columns.
- The existing schema/column coverage checks remain or are replaced by stronger checks; missing program coverage still fails closed.
- The package still serializes the validation program at `plan/validation-program.json`.
- A focused throughput benchmark or benchmarkable test path records type/null/domain rule throughput on 100k-row batches, with the result labeled environment-local and non-public.

## Evidence expectations

Record targeted tests, compile/clippy gates for `cdf-contract` and `cdf-engine`, focused conformance checks, throughput benchmark command/output, jscpd and `rust-code-analysis-cli` metrics for touched source, direct unsafe scan, and security/supply-chain gates appropriate to dependency changes. If no dependencies change, record that no lockfile/supply-chain mutation occurred.

## Explicit exclusions

No quarantine artifact writing beyond returning quarantine candidates. No destination quarantine mirrors. No variant promotion events. No trust ledger events. No public performance claim.

## Dependencies

None.

## Progress And Notes

- 2026-07-08: Implemented serialized row rule programs for generated non-null observed fields plus policy nullability, domain, range, regex, freshness, and dedup metadata. Explicit policy rules fail closed on missing columns; generated projection-sensitive nullability rules skip when their source column is absent from the batch.
- 2026-07-08: Added a pure Arrow batch evaluator in `cdf-contract` that validates batch column coverage/types, returns accepted-row selection, quarantine candidates, and verdict summaries, and fails closed for missing freshness context, malformed regex/range literals, unsupported row-rule column types, and incompatible timestamp columns.
- 2026-07-08: Wired `ContractExec` in the live engine path before normalization. Accepted rows are filtered into `NormalizeExec`; reject-batch and reject-run dispositions return contract errors before package finalization or destination mutation. The package still writes `plan/validation-program.json`; freshness packages also write `plan/contract-evaluation-context.json` with package-level `observed_at_ms`.
- 2026-07-08: Added focused contract, engine, and conformance tests, including a local/non-public 100k-row type/null/domain benchmarkable path. Evidence is recorded in `.10x/evidence/2026-07-08-p1-e1-row-level-verdicts-live-chain.md`.
- 2026-07-08: Parent review found and repaired an all-quarantined-batch edge where the live path could write a zero-row package segment. `execute_to_package_inner` now skips post-contract empty batches, and the freshness engine test asserts no data segments are emitted when all rows are rejected.
- 2026-07-08: Parent review found no blocking issues and recorded the adversarial pass in `.10x/reviews/2026-07-08-p1-e1-row-level-verdicts-live-chain-review.md`. Live quarantine artifact writing remains excluded here and owned by `.10x/tickets/done/2026-07-08-p1-e2-quarantine-routing-redaction.md`; live merge/dedup enforcement is closed at `.10x/tickets/done/2026-07-08-p1-e3-merge-dedup-live-path.md`.

## Blockers

None.
