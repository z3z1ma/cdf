Status: blocked
Created: 2026-08-03
Updated: 2026-08-03

# REST `records_transform` contract repair

## Scope

Repair the REST source's accepted-but-inert `records_transform` option so CDF never reports a
successfully compiled/executed resource whose declared transform did not run.

Recommended smallest repair: remove the option from the active REST source schema/compiled plan and
fail old configuration with direct remediation until the shared plan-declared batch hook contract
is active. Do not implement a REST-only Python loader or hook host.

## Non-goals

- implementing Python or WASM hooks;
- changing ordinary REST pagination, records extraction, schema, cursor, rate, retry, or package
  behavior;
- adding a compatibility no-op, warning-only path, or runtime string import;
- modifying the VISION example without explaining the current capability boundary;
- repairing unrelated REST source configuration.

## Acceptance Criteria

- The published REST option schema no longer advertises an executable transform that does not
  exist.
- Supplying `records_transform` fails during configuration/compile with a stable Contract error
  explaining that inline transforms are not yet supported and pointing to the explicit supported
  alternative, if any.
- No compiled source plan or runtime struct retains inert transform identity.
- The VISION/example/documentation surface is marked future or updated so it cannot be copied as a
  working current configuration.
- Focused source-schema, compile, generated-schema, documentation/example, and REST tests pass.
- The repair does not create an adapter-specific hook abstraction that conflicts with
  `.10x/specs/batch-transform-hooks.md`.

## References

- `.10x/research/2026-08-03-cdc-semantic-dsl-core-readiness-audit.md`
- `.10x/specs/batch-transform-hooks.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `VISION.md` around the current `records_transform` example
- `crates/cdf-source-rest/src/driver.rs`
- `crates/cdf-source-rest/src/lib.rs`

## Assumptions

- Record-backed: the option is declared and copied into `RestResourcePlan` but has no runtime
  consumer at revision `b7b3eb72db88c19fcc65ca456c8e517201e794ae`.
- Record-backed: no active user-facing hook contract exists; current project hooks are test/
  orchestration callbacks.
- Recommended but unratified: fail/remove now rather than preserve an inert compatibility field.

## Journal

- 2026-08-03: Source audit found only five repository occurrences: one VISION example, the driver
  JSON schema, option decode, compiled-plan copy, and the runtime plan field. No execution read
  exists. Ticket created as a correctness owner rather than folding an ad hoc fix into the future
  hook program.

## Blockers

- User must ratify the recommended compatibility behavior: reject/remove the currently inert
  option until shared hooks exist. Implementing it now would require the unresolved Python/WASM
  runtime, sandbox, schema, determinism, and authority decisions and is not a bounded repair.

## Evidence

Pending execution after ratification.

## Review

Pending.

## Retrospective

Pending.
