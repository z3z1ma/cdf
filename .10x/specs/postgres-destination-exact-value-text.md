Status: active
Created: 2026-08-03
Updated: 2026-08-03

# PostgreSQL destination exact-value text

## Purpose and scope

This specification governs PostgreSQL destination reconstruction of canonical exact-value text
produced by the PostgreSQL source. It is governed by
`.10x/decisions/exact-value-text-fallbacks.md` and complements the existing PostgreSQL binary COPY
destination contract in `.10x/specs/destination-bulk-path-runtime.md`.

## Contract

The PostgreSQL destination MUST reconstruct native JSON, JSONB, or NUMERIC only from the exact
versioned PostgreSQL semantic tags `postgres_json_value_text_v1`,
`postgres_jsonb_value_text_v1`, and `postgres_numeric_value_text_v1`, respectively. Ordinary
`Utf8`, `cdf:physical_type` alone, a field name, or a semantic tag owned by another source MUST
remain PostgreSQL text.

Tagged reconstruction MUST validate the complete field before target mutation. JSON and JSONB
parsing MUST use PostgreSQL's native input semantics. NUMERIC parsing MUST preserve finite values
exactly and accept PostgreSQL special values only where the resolved target declaration admits
them. Parse failure, incompatible target type, or an inadmissible value MUST fail with typed field-
level remediation and no partial package. The destination MUST NOT round through floating point.

The compiled mapping and destination receipt/schema evidence MUST retain the semantic tag and
resolved PostgreSQL target declaration so replay cannot reinterpret a field differently.

## Scenarios and acceptance criteria

- Tagged PostgreSQL JSON and JSONB text reconstruct native values and compare equal to their source
  stored values; no claim is made about a JSONB input lexeme PostgreSQL had already normalized.
- Tagged PostgreSQL NUMERIC text round trips finite wide, unconstrained, negative-scale, `NaN`, and
  admitted infinity values without floating-point conversion.
- Untagged, foreign-tagged, or physical-type-only `Utf8` remains PostgreSQL text.
- Invalid tagged text and target declarations that cannot admit a value fail before payload
  mutation and preserve receipt atomicity.
- Append, replace, merge, replay, binary COPY, schema inspection, and focused live round-trip tests
  prove the tagged behavior without changing ordinary string handling.

## Explicit exclusions

This contract does not infer native types from arbitrary text, provide cross-database numeric
coercion, or claim lexical JSONB preservation.
