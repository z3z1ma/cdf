Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Contract live verdict execution semantics

## Context

P0 Workstream E requires live row-level contract behavior from `VISION.md` Chapter 11 and `.10x/specs/types-contracts-normalization.md`: row verdicts, quarantine routing, dedup, variant capture, and trust-ring events. The current code has `ValidationProgram` vocabulary and schema/column coverage checks, but `cdf-engine::execution::apply_contract_exec` does not evaluate row rules or split quarantine rows.

The implementation needs one stable API shape before workers edit `cdf-contract`, `cdf-engine`, package writing, and conformance in parallel.

## Decision

`cdf-contract` owns a pure Arrow batch verdict evaluator. The evaluator takes a serialized `ValidationProgram`, a `ContractEvaluationContext`, a `RecordBatch`, and the batch source position, and returns accepted rows, quarantined rows, and a verdict summary. `cdf-engine` calls this evaluator from the live operator chain before normalization; package and destination layers consume the evaluator output rather than reimplementing contract semantics.

`ValidationProgram` MUST serialize row rule programs explicitly. Live execution MUST NOT depend on the original `ContractPolicy` being available after compilation.

The first live evaluator MUST cover:

- nullability;
- domain/enum;
- range;
- regex;
- freshness;
- dedup rules as a pre-merge package-order operation.

Freshness compares the row timestamp value to `ContractEvaluationContext::observed_at_ms`, captured once per package execution and recorded in package evidence. Missing evaluation time, missing columns, unsupported column types, malformed rule literals, or incompatible timestamp values fail closed with a contract error rather than silently accepting or quarantining by guess.

Row verdicts use the compiled program's total lattice. A row with no rule violations is accepted. A violation follows the compiled row disposition: accept, quarantine, reject batch, or reject run. Reject-batch and reject-run dispositions abort before package finalization and before destination mutation.

Quarantine is a framework side channel, not a DataFusion multi-output plan. Quarantine records MUST include, at minimum, source row ordinal, rule id, error code, source position, and redacted observed value. Values with semantic tags beginning `pii:` MUST use the compiled redaction decision, with SHA-256 as the default hash action. Quarantine artifacts are package identity evidence under `quarantine/`.

Dedup runs after row verdict filtering and before merge destination planning/commit. Package order is the order of accepted rows after extraction, residual filtering, limit/projection, and contract acceptance. `keep = first` keeps the first row in package order, `keep = last` keeps the last row in package order, and `keep = fail` aborts before destination mutation. Dedup decisions MUST be recorded in package evidence.

Variant capture stores unknown or violating nested substructure in `_cdf_variant` with semantic tag `json`. Promoting a variant to typed columns is a contract-evolution event, not an implicit normalization side effect.

Trust promotion and demotion are run-ledger events. Drift, anomaly, or quarantine events demote to full validation when the compiled promotion policy says so. Promotion requires the configured consecutive clean-run count against a stable schema hash.

## Alternatives considered

Keep row checks in `cdf-engine` only.

- Rejected because the validation program would not be the single source of truth promised by Chapter 11.

Use DataFusion multi-output plans for quarantine.

- Rejected by D-3 and Chapter 11. The accepted stream may use DataFusion execution, but quarantine remains framework-owned.

Treat freshness as wall-clock-only and omit it from package evidence.

- Rejected because verdicts must be explainable and replayable from package/run evidence. The captured evaluation time is the minimum stable context.

Perform dedup inside each destination.

- Rejected because Workstream E requires pre-merge determinism under redelivery. Destinations may enforce their own safety checks, but package-order dedup semantics belong before mutation.

## Consequences

Workstream E child tickets can implement in crate-local slices without inventing semantics.

The evaluator API may be synchronous for MVP. Future DataFusion physical-plan integration may wrap the same evaluator, but must not change verdict semantics.

Golden packages may change only when the new package evidence is intentionally added and recorded.

Throughput benchmarks must measure the evaluator path directly so performance work optimizes the real contract boundary.
