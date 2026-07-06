Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-contract-compiler-normalization.md, .10x/specs/types-contracts-normalization.md

# Contract compiler and normalization evidence

## What was observed

`crates/firn-contract` now contains serializable contract policy models, trust preset expansion, observed-schema and validation-program models, row verdict totality checks, decimal/timestamp fidelity guards, `namecase-v1` identifier normalization, nested/variant policy decisions, transform descriptions, promotion/demotion event models, destination type-mapping fidelity checks, and PII redaction decisions for package/quarantine consumers.

No `crates/firn-kernel` additions were required. No DataFusion execution plan work was added.

The workspace already had dirty `crates/firn-http/**` and `crates/firn-package/**` manifests/source from other work. Running Cargo after the contract dependency changes refreshed `Cargo.lock` for the current shared workspace, including those pre-existing dirty manifests, so `--locked` checks can run against the worktree state.

## Procedure

Read before editing:

- `.10x/tickets/done/2026-07-05-contract-compiler-normalization.md`
- `.10x/specs/types-contracts-normalization.md`
- `.10x/specs/architecture-layering-runtime.md`
- `.10x/knowledge/firn-glossary.md`
- `.10x/knowledge/quality-gate-execution.md`
- `firn-the-book-of-the-system.md` Chapter 6 and Chapter 10 excerpts
- `.10x/decisions/firn-book-decision-register.md`
- `.10x/tickets/2026-07-05-implement-firn-system.md`
- `.10x/tickets/done/2026-07-05-kernel-core-types.md`

Commands run:

- `cargo test -p firn-contract --no-fail-fast` passed: 10 unit tests passed, 0 failed; doctests 0 passed, 0 failed.
- `cargo fmt -p firn-contract` completed successfully.
- `cargo test -p firn-contract --locked --no-fail-fast` passed: 10 unit tests passed, 0 failed; doctests 0 passed, 0 failed.
- `cargo clippy -p firn-contract --all-targets --locked -- -D warnings` passed.
- `git diff --check` passed.
- `cargo fmt -p firn-contract -- --check` passed.
- `cargo fmt --all -- --check` failed on out-of-scope dirty formatting in `crates/firn-http/src/lib.rs` and `crates/firn-package/src/lib.rs`. Those files are outside this child ticket's write scope and were not reformatted by this worker.

## What this supports or challenges

This supports the ticket acceptance criteria:

- The validation program serializes through `serde_json` and `assert_verdict_lattice_total` covers every `RuleOutcome`.
- Decimal source claims compiling as float and naive/zoned timestamp timezone loss both fail at compile time.
- `namecase-v1` normalizes through Unicode NFC, preserves `firn:source_name`, truncates with an 8-hex suffix, and hard-errors post-normalization collisions.
- `TrustLevel::{Experimental, Governed, Financial, Serving}` expand into the policy shapes required by `.10x/specs/types-contracts-normalization.md`.
- PII semantic tags produce public `RedactionDecision` values for quarantine/package code.

## Limits

This evidence does not prove DataFusion `ExecutionPlan` execution, package artifact writing, actual observed-value hashing, or downstream quarantine materialization. Those are explicitly outside this child ticket or owned by later engine/package work.

Initial worker all-workspace formatting was blocked by concurrent dirty package and HTTP files. Parent closure reran all-workspace formatting after integration and recorded the pass in `.10x/evidence/2026-07-06-package-contract-http-quality-gates.md`.
