Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-05-declarative-resources.md, .10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md

# Implement resource conformance suite foundation

## Scope

Implement the first reusable resource conformance harness in `firn-conformance` over the current public `ResourceStream` and `QueryableResource` contracts. The foundation must verify planning-level resource honesty that is expressible without executing source reads, and consume the harness from the already implemented declarative REST, SQL, and file resource compiler.

Owns `crates/firn-conformance/**` and the smallest necessary `crates/firn-declarative/**` test integration. Keep `crates/firn-conformance/src/lib.rs` thin by adding focused resource modules rather than expanding the crate root.

## Acceptance criteria

- `firn-conformance` exposes a reusable resource conformance harness that accepts a `ResourceStream` or `QueryableResource` candidate and representative `ScanRequest` cases without depending on private resource internals.
- The suite asserts descriptor/schema coherence for declared primary keys, merge keys, cursor fields, and schema-source evidence where those fields are present in the public descriptor and Arrow schema.
- The suite asserts partition plans are non-empty, use unique partition ids, carry non-empty scopes compatible with declared partitioning support, and echo enough scope information for checkpointing.
- The suite asserts `QueryableResource::negotiate` rejects mismatched resource ids, preserves request identity, returns partition plans for the requested resource, and classifies pushed versus unsupported predicates consistently with declared filter capabilities.
- The suite asserts `Exact`, `Inexact`, and `Unsupported` pushdown claims are reflected in the negotiated plan in a way the engine can later use to decide residual filtering.
- The suite asserts replay and incremental capability claims have the minimum descriptor/state preconditions currently expressible in public types; for example, cursor incremental claims require a cursor, file incremental claims require file-like partition scope support, and replay-from-position claims require a position-bearing state shape.
- The suite includes negative self-tests with deliberately faulty resources so mutation testing can prove the harness catches false descriptor/schema claims, duplicate partition ids, unsupported-scope claims, mismatched negotiated requests, and dishonest pushdown classification.
- Declarative REST, SQL, and file `CompiledResource` examples consume the reusable planning-level harness while retaining existing declarative compiler tests for format-specific semantics.

## Evidence expectations

Record focused `cargo test -p firn-conformance --locked --no-fail-fast`, `cargo test -p firn-declarative --locked --no-fail-fast`, `cargo clippy -p firn-conformance --all-targets --locked -- -D warnings`, `cargo clippy -p firn-declarative --all-targets --locked -- -D warnings`, `cargo fmt --all -- --check`, and the required `QUALITY.md` closure checks.

Because this is a reusable conformance harness, mutation testing must include the harness and at least one downstream consumer, for example a bounded `cargo mutants` run over `crates/firn-conformance/src/resource` with `firn-declarative` included in the test oracle where feasible. If mutation tooling exposes build-time limits, record the exact limit and harden with negative self-tests before closure.

## Explicit exclusions

No source data execution, no calls to unsupported `CompiledResource::open`, no data partition-union completeness proof, no position replay suffix checks, no chaos killpoints, no golden-package fixtures, no MVP killer-demo harness, no new production resource runtime behavior, no CLI changes, and no changes to destination or checkpoint semantics.

Full resource data completeness, replay, and boundedness honesty remain parent scope until an openable resource runtime or an explicit resource-level boundedness signal exists.

## References

- `firn-the-book-of-the-system.md` Chapter 7, Chapter 8, Chapter 9, Chapter 19, and Chapter 22.
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-05-declarative-resources.md`
- `.10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md`

## Progress and notes

- 2026-07-06: Split from the conformance parent after read-only parent/subagent inspection found `firn-conformance` currently exports checkpoint-store and destination suites only. The current public resource traits support a planning-level foundation now; full resource data completeness, position replay, chaos, golden packages, and the MVP demo remain separate later work.

## Blockers

None for the planning-level resource-conformance foundation.
