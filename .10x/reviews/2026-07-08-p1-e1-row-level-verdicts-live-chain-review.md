Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-e1-row-level-verdicts-live-chain.md
Verdict: pass

# P1 E1 row-level verdicts review

## Target

Review of the E1 implementation and evidence:

- `crates/cdf-contract/Cargo.toml`
- `Cargo.lock`
- `crates/cdf-contract/src/program.rs`
- `crates/cdf-contract/src/compiler.rs`
- `crates/cdf-contract/src/evaluator.rs`
- `crates/cdf-contract/src/lib.rs`
- `crates/cdf-contract/src/tests.rs`
- `crates/cdf-engine/src/execution.rs`
- `crates/cdf-engine/src/tests.rs`
- `crates/cdf-conformance/src/property_fuzz/contract.rs`
- `.10x/evidence/2026-07-08-p1-e1-row-level-verdicts-live-chain.md`

## Findings

No blocking findings.

Minor finding, resolved during parent review: a batch whose rows were all rejected by the contract evaluator could continue as a zero-row `RecordBatch` and produce a zero-row package data segment. `execute_to_package_inner` now skips post-contract empty batches before normalization and package writing, and `freshness_contract_writes_observed_at_context_when_rule_requires_it` asserts that all-rejected input leaves `output.segments` empty.

The evaluator is intentionally conservative: unsupported rule/column combinations return contract errors rather than partial acceptance, and existing missing-program coverage checks were strengthened to include batch type compatibility. This matches the decision-ratified fail-closed execution semantics.

The largest duplication reported by `jscpd` is in tests and typed Arrow branch handling. Keeping it explicit is acceptable for E1 because it avoids introducing a generic scalar abstraction before E2/E3 prove additional reuse pressure.

## Assumptions tested

- Scope boundary: no package quarantine writing, destination quarantine mirrors, variant promotion events, trust ledger events, CLI surfaces, or unrelated destination/runtime code were added.
- Program sufficiency: runtime evaluation reads only `ValidationProgram`, `ContractEvaluationContext`, and the Arrow batch; it does not require the original `ContractPolicy`.
- Freshness: packages use one observed-at context for execution, fail closed when context is absent, and fail closed for incompatible timestamp columns.
- Destination safety: reject-batch and reject-run errors occur in `ContractExec`, before package finalization and before any destination mutation.
- Determinism: `plan/validation-program.json` is still written; `plan/contract-evaluation-context.json` is written only for freshness programs where the active decision requires runtime context.
- Dependency hygiene: new direct dependencies are already locked elsewhere in the workspace, and cargo-vet, cargo-audit, cargo-deny, OSV, machete, Gitleaks, and Semgrep results do not show a new blocking supply-chain or source issue.

## Residual risk

The 100k-row throughput path is local and non-public. It establishes a repeatable benchmarkable check, not a stable or public throughput claim.

Dedup live enforcement and quarantine artifact routing are deliberately not implemented by E1. They remain tracked by the existing E3 and E2 child tickets named in the evidence limits.

## Verdict

Pass. E1 satisfies its acceptance criteria with focused implementation, tests, evidence, supply-chain/source checks, and adversarial review sufficient to close the child ticket.
