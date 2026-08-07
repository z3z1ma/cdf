Status: active
Created: 2026-08-06
Updated: 2026-08-06

# Schema drift dispositions and evidence

## Purpose

This specification defines total compiled behavior for physical observations that differ from an
active state-backed logical schema. It replaces automatic schema evolution, coupled
`Evolve | Freeze` switches, and global quarantine enablement with explicit actions at the smallest
grain where correctness can be proven.

It refines `.10x/specs/residual-variant-capture.md`,
`.10x/specs/schema-discovery-and-stream-admission.md`, and
`.10x/decisions/state-backed-schema-authority.md`.

## Policy model

The contract compiler MUST produce total typed dispositions equivalent to:

```text
unknown field / incompatible nullable value:
  capture_variant | quarantine_row | fail_run

missing required / row rule:
  quarantine_row | fail_run

isolated malformed record:
  quarantine_record | fail_run

unresynchronizable input:
  quarantine_partition | fail_run
```

Lossless coercion is a separately compiled admissible transformation, not a drift disposition.
Missing nullable fields materialize typed null and record an observation. Neither changes the
active schema head.

There is no `quarantine.enabled`. If no policy selects quarantine, no execution path may quarantine.
No violation may silently admit, drop, widen, or substitute a default because an evidence path is
unavailable.

PII redaction belongs to shared evidence policy and MUST apply consistently to variant and
quarantine evidence. A redacted, hashed, masked, or omitted variant MUST be marked
non-reconstructable and cannot support historical promotion.

## Field roles and allowed actions

The compiler MUST assign each observed/expected field one role or compatible set of roles:

- ordinary data;
- required output;
- destination identity or merge key;
- source progress/cursor;
- CDC operation;
- transaction boundary.

It MUST derive an allowed-disposition set from those roles. One `control_critical` Boolean is
insufficient.

Safe field-level variant capture requires all of:

- the offending value is reliably isolatable;
- the remaining row is meaningful;
- the typed field is nullable or absent from the active output schema;
- source progress, operation, transaction, and identity correctness remain intact;
- the original value has an exact canonical residual representation or an explicitly marked
  privacy transformation.

A merge-key violation may be quarantined only when excluding that row preserves source advancement
and destination semantics. Cursor, CDC-operation, or transaction-boundary violations MUST fail
unless a source-specific compiled proof establishes safe isolation and advancement.

## Observation matrix

Ordinary plan/run reconcile observations as follows:

| Observation | Required behavior |
|---|---|
| Expected field, identical type | Admit |
| Expected field, compiled lossless coercion | Coerce and record evidence |
| New field | Apply field disposition; never invent typed output |
| Missing nullable field | Materialize typed null and record |
| Missing required field | Apply row disposition |
| Incompatible nullable ordinary value | Apply field disposition |
| Identity/progress/operation/transaction mismatch | Apply only compiler-proven safe action, otherwise fail |
| Reliably isolated malformed record | Apply record disposition |
| Broken framing/encoding | Quarantine partition only with safe advancement proof, otherwise fail |

This is not set intersection. Missing active fields retain type, nullability, requiredness, role,
and evidence semantics.

## Variant residual

`_cdf_variant` remains the exact framework field defined by
`.10x/specs/residual-variant-capture.md`: final nullable UTF-8, `cdf.variant@1`, canonical
`residual-json-v1`, null for clean rows, and part of canonical accepted package data.

For a nullable mismatch, the typed output field becomes null and the exact original value is stored
at its source JSON pointer. For a new field, no typed output field appears and the exact field/value
is stored in the residual envelope. Unrelated paths in the same envelope remain independent.

Variant capture is an accepted-row outcome:

| Outcome | Main destination | Quarantine evidence | Variant authority |
|---|---:|---:|---:|
| Accepted clean | yes | no | null |
| Accepted with variant residual | yes | no | exact or explicitly non-reconstructable |
| Quarantined | no | yes | not authoritative |
| Failed | no commit | no committed row | none |

If any compiled policy can capture a variant, `_cdf_variant` MUST already exist in the active
logical output schema. Enabling capture where the field is absent requires explicit schema
promotion.

An adapter may preserve only fields it actually observes. Explicit typed projection remains output
authority; wider record fetch needed for unknown-field capture MUST be plan-visible as a pushdown/
I/O cost. Catalog-only diff evidence does not imply run-time capture ability.

Telemetry and package summaries MUST distinguish accepted, accepted-with-residual, residual
fields/bytes, quarantined rows/records/partitions, and failed partitions. Residual rows are never
counted as quarantined.

## Quarantine

Quarantine is a durable data outcome:

1. Exclude the offending row/record/partition from the main accepted stream.
2. Durably package canonical source position, rule, error code, physical observation, and redacted
   value evidence.
3. Continue other accepted work when the source advancement proof permits it.
4. Bind accepted and quarantine counts into package and receipt evidence.
5. Advance checkpoints only after quarantine evidence and the applicable destination/package
   settlement proof are durable.

Destination quarantine tables are optional mirrors; package quarantine evidence is canonical.
Configuration, authentication, egress, unsupported capability, corrupt state, internal invariant,
destination ambiguity, and unsafe source-position/transaction failures cannot be quarantined.

When every input row is quarantined, CDF MUST obtain an empty/metadata destination receipt or an
equally strong package/state quarantine-only settlement before advancing. It cannot orphan the only
copy of quarantined evidence.

## Fail

`fail_run` aborts the affected resource before destination mutation, publishes no successful
package, advances no checkpoint, preserves only bounded failed-attempt diagnostics, and reports the
exact violation plus configured disposition. Independent resources already admitted past the
shared preparation barrier may continue under `continue_independent`.

Internal reject-batch scope may remain for execution control, but public output states plainly that
the resource did not commit.

## Trust presets

The ratified defaults are:

| Violation | Experimental | Governed | Financial | Serving |
|---|---|---|---|---|
| Safe unknown/incompatible field | capture variant | capture variant | fail | capture variant |
| Missing required / row rule | fail | quarantine row | fail | quarantine row |
| Reliably isolated malformed record | fail | quarantine record | fail | quarantine record |
| Safely isolatable partition | fail | quarantine partition | fail | quarantine partition |
| Cursor/CDC/transaction or unsafe framing | fail | fail | fail | fail |

Compiled lossless coercion and missing-nullable typed-null materialization remain admissible for all
presets. Serving's sampled fast path is governed separately and does not change dispositions.

## Plan and report behavior

Plan MUST show active schema version/generation, observation strength, each drift class/count, the
compiled disposition, whether wider source observation is required, and zero schema migrations for
source-discovered drift. A locked plan never presents new source fields as pending DDL.

Human and JSON reports share one typed redacted authority. On failure, output names the disposition
that stopped the resource. On success, accepted-with-residual and quarantine remain distinct.

## Acceptance scenarios

For each supported source family, tests cover extra fields, nullable mismatch, lossless coercion,
missing nullable/required, merge key, cursor, CDC operation/transaction, malformed record, broken
framing, nested unknown values, redacted residuals, and multiple residual paths under every allowed
action.

Assertions cover main rows, exact `_cdf_variant` bytes, quarantine artifacts, package counts,
receipts, checkpoints, exit/result, and an unchanged schema head.

## Acceptance criteria

- Current policy types contain no evolution mode or global quarantine switch.
- Every compiled observation class has one total typed disposition.
- Compiler field-role tests prevent unsafe capture/quarantine of progress and transaction facts.
- Variant codec tests preserve exact values and promotability/redaction metadata.
- Quarantine-only tests prove durable settlement before advancement.
- Plan/run reports preserve JSON/human/redaction parity and distinguish every outcome count.
- No ordinary drift path changes state schema authority or destination schema.

## Explicit exclusions

- automatic widening of governed output;
- silent destination migration;
- raw residual values in state;
- source-specific semantics in generic project/runtime orchestration;
- nested residual promotion, future-only promotion, or arbitrary business-key correction.

## Ratification status

The user ratified the total disposition model and exact four trust preset mappings on 2026-08-06.
