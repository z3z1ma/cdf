Status: done
Created: 2026-07-08
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-data-onramp-program.md
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
- 2026-07-09: Split executable child `.10x/tickets/done/2026-07-09-p2-ws-g1-source-diagnostics-and-deep-validate-foundation.md` for the first source-specific diagnostics and `cdf validate --deep` doorway.
- 2026-07-09: G1 closed with evidence `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md` and review `.10x/reviews/2026-07-09-p2-e2-g1-b4-batch-review.md`. `cdf validate --deep` now runs current no-write compiler-front-end checks and resource-not-compiled errors include compiled ids, origins, mapping status, likely causes, suggestions, and source-specific error code/remediation. Remaining WS-G scope: deeper type-mismatch remediation, full command-name sweep across future source failures, cloud/compression/Python/WASM deep-validate coverage, and final source-experience catalog closure.
- 2026-07-09: Split executable child `.10x/tickets/done/2026-07-09-p2-ws-g2-type-mismatch-diagnostics.md` for supported Parquet/JSON reconciliation mismatch specificity, command-context correctness, no-write deep validation, and redaction.
- 2026-07-09: G2 preflight marked the child blocked before implementation because bounded JSON probe limits, row-local warning semantics, and the missing Tier-0 type-policy override syntax require ratification. Parquet-only work was not split out because its required "both fixes" remediation depends on the same missing allowance surface.
- 2026-07-10: G2 closed with evidence `.10x/evidence/2026-07-10-p2-g2-deep-type-diagnostics.md` and review `.10x/reviews/2026-07-10-p2-g2-deep-type-diagnostics-review.md`. Deep validation now uses the production file transport and shared reconciler, emits exact physical/constraint diagnostics and stable codes, honors runtime-effective Tier-0 allowances, preserves governed row mismatch warnings, and treats malformed input as an error. Remaining WS-G scope is final cloud/compression/source-archetype catalog coverage after the corresponding P2 source surfaces settle.
- 2026-07-10: G3 closed with `.10x/evidence/2026-07-10-p2-g3-s6-drift-rendering.md` and `.10x/reviews/2026-07-10-p2-g3-s6-drift-rendering-review.md`. Successful governed runs now render exact typed terminal quarantine verdicts from plan authority without CLI artifact reinterpretation, promoting S6 to covered. Final WS-G closure now depends only on the aggregate friction/catalog audit.
- 2026-07-10: Workstream closed after the aggregate friction registry reached eighteen tested historical rows and zero open owners. Deep validation uses production source front ends and command-correct typed remediation; S6 renders exact drift authority.

## Blockers

None.
