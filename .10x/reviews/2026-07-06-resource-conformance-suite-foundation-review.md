Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md
Verdict: pass

# Resource conformance suite foundation review

## Target

Closure review for `.10x/tickets/done/2026-07-06-resource-conformance-suite-foundation.md`, including the new `firn_conformance::resource` harness, declarative consumer tests, scoped dependency updates, mutation hardening, and `QUALITY.md` evidence.

## Assumptions tested

- The ticket owns only a planning-level resource conformance foundation and must not implement source reads, resource runtimes, chaos, golden packages, or parent-level completeness/replay behavior.
- The harness may rely only on public `ResourceStream` and `QueryableResource` contracts, not private declarative-resource internals.
- Declarative REST, SQL, and file examples should consume the reusable harness without replacing format-specific compiler assertions.
- The crate root should remain thin per `.10x/knowledge/rust-crate-organization.md`.
- Quality evidence must include the mandatory `QUALITY.md` checks, a reusable CodeQL database path, and bounded mutation testing over the harness plus a downstream consumer.

## Findings

No blocking findings.

The implementation stays within the child scope. It adds a focused `resource` module in `firn-conformance`, keeps `lib.rs` thin, and integrates declarative consumer tests without adding production resource runtime behavior or calls to `CompiledResource::open`.

The harness checks all acceptance-scope public contracts: descriptor/schema field coherence, schema-source evidence, partition identity and scope honesty, checkpoint scope shape, capability preconditions, mismatched-resource rejection, request identity preservation, and pushed/unsupported predicate classification with exact/inexact fidelity.

Negative self-tests and mutation testing materially reduce false-positive risk. Missed mutants during the first bounded mutation runs were converted into additional tests before the final mutation pass.

`Cargo.lock` changes are consistent with the new scoped dev-dependency edges: `firn-conformance` needs `arrow-schema` for harness self-tests, and `firn-declarative` needs `firn-conformance` as a dev-dependency for consumer tests.

The simple predicate-operator matching strategy is a residual limitation, not a closure blocker. The current public predicate model is string-based; introducing parser semantics would exceed the ticket and could invent behavior not ratified by the resource specs.

## Verdict

Pass. The child ticket's acceptance criteria are supported by `.10x/evidence/2026-07-06-resource-conformance-suite-foundation.md`. Remaining source execution, completeness, replay, chaos, golden-package, and MVP demo behavior correctly remains with the parent ticket.

## Residual risk

The new harness proves planning honesty only. Future resource runtime work still needs separate tickets and evidence for data completeness, boundedness, replay suffix behavior, and chaos lifecycle guarantees.
