Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-g-source-diagnostics-deep-validate.md
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

## Blockers

None for the foundation slice.
