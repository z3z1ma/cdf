Status: done
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
- User-ratified on 2026-08-03: fail/remove now rather than preserve an inert compatibility field.

## Journal

- 2026-08-03: Source audit found only five repository occurrences: one VISION example, the driver
  JSON schema, option decode, compiled-plan copy, and the runtime plan field. No execution read
  exists. Ticket created as a correctness owner rather than folding an ad hoc fix into the future
  hook program.
- 2026-08-03: The user said “Proceed” after the record handoff and requested batched validation and
  review. The recommended reject/remove behavior is activated. Execution will run one focused REST/
  schema/doc validation batch and one consolidated review; no workspace-wide suite is authorized or
  useful for this leaf repair.
- 2026-08-03: Removed the option from the REST schema, decoded options, portable physical plan, and
  runtime plan. Replaced the working VISION example with the governed future hook boundary.
- 2026-08-03: The consolidated review correctly found that closed-schema validation precedes
  adapter decoding. Added a registry-enforced compatibility preflight that can only narrow a
  driver's closed schema, then moved the regression to `SourceRegistry::compile` and gave the
  retired field a stable remove-it diagnostic. Corrected VISION from runtime to compiler rejection.

## Blockers

None.

## Evidence

- Published schema and compile rejection: the focused REST regression asserts that
  `records_transform` is absent from the registered driver schema and that
  `SourceRegistry::compile` returns `ErrorKind::Contract` with the exact unsupported/remove-it
  diagnostic. `cargo test -p cdf-source-rest` passed 9 tests, failed 0, and retained 1 explicitly
  ignored release performance-envelope test.
- Generated schema composition: `cargo test -p cdf-declarative
  generated_schema_merges_common_and_driver_fields_into_closed_objects` passed 1 focused test with
  21 filtered out. Combined with the registered REST schema assertion, this supports that generated
  declarations cannot reintroduce a driver field absent from the closed schema; it does not claim a
  byte-for-byte release artifact snapshot.
- Plan/runtime removal and documentation inventory: repository search for `records_transform`
  found only the compatibility rejection, its regression, and the VISION future-capability note;
  no compiled-plan or runtime-plan field remains.
- Formatting and patch hygiene: `cargo fmt --all` completed and `git diff --check` passed.

## Review

- Consolidated independent red-team verdict before repair: **concerns**. One significant finding
  identified the wrong tested boundary and missing remediation in the actual registry path; one
  minor finding identified “runtime” instead of compile-time wording in VISION. The reviewer found
  no remaining production consumer or adapter-specific hook host.
- Reconciliation: both findings were repaired directly. The actual registry boundary now owns the
  regression and stable Contract diagnostic, and VISION names compiler rejection. The compatibility
  preflight cannot widen a schema because ordinary closed-schema validation always follows it.
- Closure verdict: **pass**. Residual risk is limited to the generated-schema evidence being a
  composition test rather than a release-artifact snapshot; the driver-schema assertion and absence
  inventory directly cover the changed adapter surface.

## Retrospective

The initial adapter-level decode test was insufficient because the composition root validates the
closed schema first. Testing through the public registry boundary exposed the actual user-visible
error and produced a small reusable compatibility seam without weakening schema closure or creating
a transform abstraction. Future option removals should begin at the registry path, not the adapter's
private decoder.
