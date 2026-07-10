Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Residual variant capture

## Purpose and scope

This specification governs field/path-level preservation of values that cannot enter the pinned typed projection. It extends the existing nested `_cdf_variant` behavior in `.10x/specs/types-contracts-normalization.md` and `.10x/decisions/contract-live-verdict-execution-semantics.md` to unknown fields, scalar type mismatches, and isolated parse/coercion failures.

## Verdict behavior

The validation program MUST emit exactly one verdict for every nonconforming value:

- admit/coerce into the typed field;
- capture as a residual while admitting the rest of the row;
- quarantine the row;
- quarantine the file/partition;
- fail before destination mutation when no safe total verdict exists.

Discover/evolve resources compile safe residual capture by default unless an explicit contract chooses quarantine or failure. Freeze resources quarantine schema drift by default and capture only when explicitly allowed. Declared and Hints resources follow their compiled contract rather than source-format-specific behavior.

Residual capture is safe only when the decoder can isolate the original value and all control fields remain valid. A violation of a cursor, merge/primary key, contract-required non-null field, source-position field, operation field, or other field named by the validation program as control-critical MUST quarantine rather than replace the typed value with null. Corrupt framing or an encoding failure that prevents reliable row/path boundaries MUST quarantine the containing row/file.

For an isolated mismatch of an otherwise nullable typed field, the output typed field is null and the original value is captured. For an unknown field, no typed output field is invented during the run; the value appears only in the residual. Other conforming values in the row continue unchanged.

## Output column

When residual capture is compiled, the normalized output schema MUST contain exactly one nullable UTF-8 `_cdf_variant` field as the final field, with:

- `cdf:semantic = json`;
- `cdf:variant_encoding = residual-json-v1`;
- source-name and nullability provenance where applicable.

Rows without residuals carry null, not `{}`. User fields that normalize to a reserved `_cdf_*` name fail at plan time.

## Canonical residual envelope

Each non-null cell is canonical JSON with this logical shape:

```json
{
  "v": 1,
  "fields": {
    "/source/path": {
      "arrow_type": { "kind": "..." },
      "encoding": "...",
      "value": "..."
    }
  }
}
```

Paths are RFC 6901 JSON Pointers over original source names, not normalized destination names. Object keys are canonically sorted. `arrow_type` uses the canonical structured Arrow snapshot type descriptor rather than an implementation display string.

Value encodings are exact and versioned:

- booleans and UTF-8 strings use JSON boolean/string;
- signed/unsigned integers and decimal values use canonical base-10 strings;
- finite floats use shortest round-trippable decimal strings; NaN and positive/negative infinity use named symbolic strings;
- binary values use unpadded base64url;
- temporal, duration, and interval values use canonical signed storage-unit integer strings, with unit/timezone in `arrow_type`;
- lists and structs recurse by Arrow child type;
- maps are ordered arrays of `{key, value}` entries so non-string keys remain representable;
- null is represented as JSON null with its Arrow type.

The encoder MUST round-trip supported Arrow scalar/nested values exactly back into an array of the recorded type. An unsupported type or failed exact encoding produces the named `cdf.residual_encode_unsupported` quarantine verdict; it MUST NOT stringify lossily or crash as an internal error.

## Evidence, redaction, and replay

The validation program and package contract-evolution artifact MUST record source path, observed physical type, expected/effective type when present, verdict rule, residual encoding version, baseline/effective schema hashes, and whether the typed field was nulled or absent.

PII/secret semantic tags apply before artifact rendering. Residual values subject to redaction MUST retain a typed redacted/hash envelope sufficient to identify the rule and compare repeated observations without exposing plaintext.

The package's canonical Arrow IPC data contains `_cdf_variant`; replay therefore needs no source contact. Promotion MUST consume verified package residuals or a destination readback capability that reproduces the same canonical envelope.

## Scenarios

Given a sampled pin with `fare_amount: int64` and an unprobed row containing `fare_amount: "unknown"`, when `fare_amount` is nullable and not control-critical, then the output contains null `fare_amount`, the other typed columns, and an exact residual at `/fare_amount`.

Given the same mismatch on a merge key or cursor, when validation runs, then the row quarantines and no partially addressable destination row is admitted.

Given an extra field not present in the pin, when evolve residual capture runs, then the field is preserved only in `_cdf_variant` until explicit promotion and the pin remains unchanged.

## Acceptance criteria

- Property tests round-trip supported generated Arrow values through `residual-json-v1`.
- Required/control-field violations quarantine; safe nullable mismatches preserve the rest of the row.
- `_cdf_variant` is last, nullable, semantically tagged, and null for clean rows.
- Package verification/replay preserves residual bytes and evolution evidence.
- Redaction tests prove sensitive residual plaintext is absent.

## Explicit exclusions

This specification does not authorize implicit schema promotion, cross-row type inference, user-defined parser execution, destination update semantics, or mutation of historical packages.
