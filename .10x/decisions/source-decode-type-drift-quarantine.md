Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Source decode type-drift quarantine

## Context

P0 Workstream E and P1 E6 require a drift-quarantine conformance scenario that freezes a resource, drifts a fixture type, quarantines the offending rows, and lets accepted rows continue to package, destination commit, receipt verification, and checkpoint gating.

The current live contract evaluator starts from Arrow `RecordBatch` values. `.10x/decisions/contract-live-verdict-execution-semantics.md` correctly makes `cdf-contract` the pure Arrow row-verdict owner and keeps unsupported/incompatible Arrow evaluator cases fail-closed. That decision does not define what happens when a structured source decoder sees a per-row scalar type mismatch before it can construct a `RecordBatch`.

During E6 implementation, a numeric JSON value in a field that the frozen contract expects as string failed in `cdf-formats` before `ContractExec`:

```text
Json error: whilst decoding field 'event_type': expected string got 42
```

That behavior is safe but cannot satisfy the book's and P0 directive's "type drift quarantines offenders while accepted rows continue" beat.

## Decision

Structured source decoders with a declared schema MUST distinguish localized row-field decode drift from unlocalizable malformed input.

A localized row-field decode drift is a mismatch where the source format parser can identify:

- the source row ordinal;
- the source field name;
- the offending source value, redacted according to the compiled field redaction policy when available;
- the declared or frozen Arrow type expected for that field;
- the source position for the batch or file.

For localized row-field decode drift, the decoder/runtime MUST emit a pre-contract quarantine fact and omit the offending row from the accepted Arrow batch. Accepted rows MUST preserve package order relative to one another and continue through `ContractExec`, normalization, package writing, destination commit, receipt verification, and checkpoint gating.

Pre-contract quarantine facts MUST be folded into package quarantine artifacts and package verdict/quarantine summaries using the same side-channel contract as row-rule quarantine. Their stable error code is `source_type_mismatch`. Their rule id MUST be deterministic and field-scoped, with the shape `source-decode:<field>:type-mismatch` unless a later active spec supersedes this string. The observed value MUST be redacted at least as strictly as row-rule quarantine for the field's semantic tags.

Malformed JSON lines or documents, unsupported declared Arrow types, schema-wide inference failures, row shape errors that cannot identify one offending field, and decoder errors that cannot preserve accepted-row order remain fail-closed before package finalization. This decision does not weaken `cdf-contract::evaluate_record_batch`: once an Arrow batch reaches the evaluator, its schema must still match the compiled `ValidationProgram` exactly.

This is a source/runtime side-channel into package evidence, not a DataFusion multi-output plan and not a second contract evaluator.

## Alternatives considered

Treat all source decode type errors as fail-closed.

- Rejected because it contradicts the P0 E6 and MVP acceptance demo requirement that type drift quarantines offenders while accepted rows continue.

Coerce mismatched scalar values into the declared Arrow type and rely on row-domain or regex rules.

- Rejected because silent coercion hides the original failure mode. If a value is quarantined because its source type drifted, the package evidence must say so.

Teach `cdf-contract::evaluate_record_batch` to accept mismatched Arrow field types.

- Rejected because the evaluator deliberately owns Arrow-batch semantics. Pre-Arrow source decode failures belong at the source/runtime boundary.

Drop the row without quarantine.

- Rejected because it would advance source state without preserving the rejected row as evidence.

## Consequences

P1 E6 needs a lower-layer implementation ticket before it can close with a literal type-drift fixture.

The kernel or engine needs a narrow fact shape for pre-contract quarantine that does not make `cdf-package` a dependency of source crates and does not put DataFusion in the source path.

Conformance must prove both sides: localized scalar type drift is quarantined with accepted-row progress, while unlocalizable malformed input still fails closed before package finalization.
