Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/specs/data-onramp-source-experience-cli.md, .10x/specs/cli-error-experience-catalog.md

# P2 WS-G source diagnostics and deep validation

## Scope

Extend P1 errors and validation for source-onramp failures: command-correct wording, compiled-resource diagnostics, type mismatch remediation, and `cdf validate --deep` as the compiler-front-end check before extraction.

Split executable child tickets before code for error catalog additions, command-name sweeps, resource-not-compiled diagnostics, type mismatch messaging, and deep validate.

## Acceptance criteria

- "Resource not compiled" errors list compiled resources, source files, and likely cause taxonomy.
- `cdf plan` errors do not say `cdf run` unless they are naming a next command.
- Type mismatches name source type, declared/snapshot type, field, location, and both fixes.
- Generic remediation text is replaced for source-experience failures.
- `cdf validate --deep` resolves globs, probes schemas, reconciles types, normalizes identifiers, and checks destination sheet compatibility without extraction or writes.

## Evidence expectations

Error snapshot tests, construction-site coverage for error codes/remediation, deep-validate fixtures, redaction adversarial checks, and command-name regression tests.

## Explicit exclusions

This ticket does not redesign the whole P1 renderer or non-source error catalog.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-09: Split executable child `.10x/tickets/2026-07-09-p2-ws-g1-source-diagnostics-and-deep-validate-foundation.md` for the first source-specific diagnostics and `cdf validate --deep` doorway.

## Blockers

Depends on enough WS-A/B/C/D shape to make deep validate meaningful; early catalog work can proceed independently.
