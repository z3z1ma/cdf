Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-g-source-diagnostics-deep-validate.md
Depends-On: .10x/specs/data-onramp-source-experience-cli.md, .10x/specs/cli-error-experience-catalog.md

# P2 WS-G1 source diagnostics and deep validate foundation

## Scope

Land the first P2 source-diagnostics slice: command-correct wording, source-specific "resource not compiled" detail, and a `cdf validate --deep` CLI doorway that runs current compiler-front-end checks without extraction or writes.

## Acceptance criteria

- `cdf validate --deep` parses and renders distinctly from ordinary `cdf validate`.
- Deep validate resolves declarative project resources, file globs/list counts, schema discovery where supported without pin writes, schema reconciliation, identifier normalization, and destination-sheet compatibility for current implemented archetypes.
- Resource-not-compiled errors list compiled resource ids, source files where known, mapping pattern match status, and likely causes.
- `plan` errors no longer reuse `run` wording except when recommending a next command.
- Source-experience errors carry a P1 error code and concrete remediation.

## Evidence expectations

CLI parser/help tests, deep-validate project fixture tests, error snapshot tests, command-name regression tests, redaction checks, and normal quality gates.

## Explicit exclusions

This ticket does not implement the full final deep-validate matrix for cloud transports, compression, Python, WASM, or future Avro-like formats.

## Progress and notes

- 2026-07-09: Opened as the first executable WS-G child. Enough WS-A/B/C/D primitives now exist to make a useful deep validate doorway possible.
- 2026-07-09: Worker G1 implemented the CLI-side foundation: `cdf validate --deep` parser doorway and distinct report path; per-resource deep validation over current compiler-front-end checks; enriched resource-not-compiled errors with compiled ids, source files, mapping status, likely causes, suggestions, and `CDF-RESOURCE-NOT-COMPILED`; command-correct scan wording for destination-planning errors; and focused CLI tests for help, no-write local Parquet discovery deep validation, resource-not-compiled detail, and plan wording.
- 2026-07-09: The earlier concurrent `cdf-declarative` compile blocker was resolved by the E2/D lane before closure verification. Final verification covers `cargo test -p cdf-cli validate_deep --locked`, `cargo test -p cdf-cli resource_not_compiled --locked`, full `cargo test -p cdf-cli --locked`, full workspace tests, nextest, and coverage. Closure evidence: `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md`. Review: `.10x/reviews/2026-07-09-p2-e2-g1-b4-batch-review.md`.

## Blockers

None.
