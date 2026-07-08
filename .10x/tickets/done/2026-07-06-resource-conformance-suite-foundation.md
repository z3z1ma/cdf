Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-conformance-chaos-golden.md
Depends-On: .10x/tickets/done/2026-07-05-declarative-resources.md, .10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md

# Implement resource conformance suite foundation

## Scope

Implement the first reusable resource conformance harness in `cdf-conformance` over the current public `ResourceStream` and `QueryableResource` contracts. The foundation must verify planning-level resource honesty that is expressible without executing source reads, and consume the harness from the already implemented declarative REST, SQL, and file resource compiler.

Owns `crates/cdf-conformance/**` and the smallest necessary `crates/cdf-declarative/**` test integration. Keep `crates/cdf-conformance/src/lib.rs` thin by adding focused resource modules rather than expanding the crate root.

## Acceptance criteria

- `cdf-conformance` exposes a reusable resource conformance harness that accepts a `ResourceStream` or `QueryableResource` candidate and representative `ScanRequest` cases without depending on private resource internals.
- The suite asserts descriptor/schema coherence for declared primary keys, merge keys, cursor fields, and schema-source evidence where those fields are present in the public descriptor and Arrow schema.
- The suite asserts partition plans are non-empty, use unique partition ids, carry non-empty scopes compatible with declared partitioning support, and echo enough scope information for checkpointing.
- The suite asserts `QueryableResource::negotiate` rejects mismatched resource ids, preserves request identity, returns partition plans for the requested resource, and classifies pushed versus unsupported predicates consistently with declared filter capabilities.
- The suite asserts `Exact`, `Inexact`, and `Unsupported` pushdown claims are reflected in the negotiated plan in a way the engine can later use to decide residual filtering.
- The suite asserts replay and incremental capability claims have the minimum descriptor/state preconditions currently expressible in public types; for example, cursor incremental claims require a cursor, file incremental claims require file-like partition scope support, and replay-from-position claims require a position-bearing state shape.
- The suite includes negative self-tests with deliberately faulty resources so mutation testing can prove the harness catches false descriptor/schema claims, duplicate partition ids, unsupported-scope claims, mismatched negotiated requests, and dishonest pushdown classification.
- Declarative REST, SQL, and file `CompiledResource` examples consume the reusable planning-level harness while retaining existing declarative compiler tests for format-specific semantics.

## Evidence expectations

Record focused `cargo test -p cdf-conformance --locked --no-fail-fast`, `cargo test -p cdf-declarative --locked --no-fail-fast`, `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`, `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`, `cargo fmt --all -- --check`, and the required `QUALITY.md` closure checks.

Because this is a reusable conformance harness, mutation testing must include the harness and at least one downstream consumer, for example a bounded `cargo mutants` run over `crates/cdf-conformance/src/resource` with `cdf-declarative` included in the test oracle where feasible. If mutation tooling exposes build-time limits, record the exact limit and harden with negative self-tests before closure.

## Explicit exclusions

No source data execution, no calls to unsupported `CompiledResource::open`, no data partition-union completeness proof, no position replay suffix checks, no chaos killpoints, no golden-package fixtures, no MVP acceptance demo harness, no new production resource runtime behavior, no CLI changes, and no changes to destination or checkpoint semantics.

Full resource data completeness, replay, and boundedness honesty remain parent scope until an openable resource runtime or an explicit resource-level boundedness signal exists.

## References

- `VISION.md` Chapter 7, Chapter 8, Chapter 9, Chapter 19, and Chapter 22.
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/conformance-governance-roadmap.md`
- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/quality-gate-execution.md`
- `.10x/tickets/2026-07-05-conformance-chaos-golden.md`
- `.10x/tickets/done/2026-07-05-declarative-resources.md`
- `.10x/tickets/done/2026-07-06-destination-conformance-suite-foundation.md`

## Evidence and review

- `.10x/evidence/2026-07-06-resource-conformance-suite-foundation.md`
- `.10x/reviews/2026-07-06-resource-conformance-suite-foundation-review.md`

## Progress and notes

- 2026-07-06: Split from the conformance parent after read-only parent/subagent inspection found `cdf-conformance` currently exports checkpoint-store and destination suites only. The current public resource traits support a planning-level foundation now; full resource data completeness, position replay, chaos, golden packages, and the MVP demo remain separate later work.
- 2026-07-06: Parent marked active and assigned to a worker. Worker owns the scoped resource conformance harness and declarative consumer tests; parent owns final review, quality evidence, closure records, and commit.
- 2026-07-06: Worker implemented `cdf_conformance::resource` with planning-only `ResourceStream`/`QueryableResource` checks, negative faulty-resource self-tests, and declarative REST/SQL/file consumer tests. Real workspace `cargo fmt --all -- --check` passes, but `cargo test -p cdf-conformance --locked --no-fail-fast` and `cargo test -p cdf-declarative --locked --no-fail-fast` fail before compilation because the new scoped dev-dependency edges require a `Cargo.lock` update outside the worker write boundary. Worker left `Cargo.lock` untouched and verified the same test/clippy commands in `/tmp/cdf-resource-conformance-verify` after an isolated lock update: `cargo test -p cdf-conformance --locked --no-fail-fast`, `cargo test -p cdf-declarative --locked --no-fail-fast`, `cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings`, and `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings` all passed.
- 2026-07-06: Parent accepted the scoped `Cargo.lock` update required by the new dev-dependency edges and independently reran focused conformance/declarative test, clippy, nextest, fmt, and diff checks successfully.
- 2026-07-06: Parent hardened the worker implementation after bounded mutation runs exposed missed harness self-test cases, then reran `cargo mutants` over `crates/cdf-conformance/src/resource/mod.rs` with `cdf-conformance` and `cdf-declarative` as the test oracle. Final result: 27 mutants tested, 22 caught, 5 unviable.
- 2026-07-06: Parent ran the mandatory `QUALITY.md` closure checks, including workspace check/test/clippy/doc/feature-matrix checks, nextest, coverage, semver, audit/deny/vet/OSV, semgrep, gitleaks, machete, udeps, rust-code-analysis, jscpd, direct unsafe inventory, CodeQL with reusable database path `target/quality/codeql-db-rust`, Miri, cargo-careful, and geiger. Results are recorded in `.10x/evidence/2026-07-06-resource-conformance-suite-foundation.md`.
- 2026-07-06: Closure review passed with no blocking findings. Remaining source execution, completeness, replay suffix, chaos, golden-package, MVP demo, and live Postgres behavior remains parent scope.

## Blockers

None for the planning-level resource-conformance foundation.
