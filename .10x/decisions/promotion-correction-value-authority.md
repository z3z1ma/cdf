Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Promotion correction values use the exact residual codec as execution authority

## Context

RP3 introduced `DestinationCorrectionRequest.promoted_value_json` before correction execution existed. The field is sufficient as a planner/display value but does not define whether integer, decimal, temporal, binary, nested, or non-finite values use ordinary JSON, the canonical residual representation, or destination literals. RP6 and RP7 cannot safely execute that ambiguity: Postgres and DuckDB would otherwise need separate parsers and could commit different values from one promotion plan.

`.10x/specs/residual-variant-capture.md` already defines `residual-json-v1` as CDF's exact, type-aware, round-trippable value representation. `.10x/specs/schema-promotion-corrections.md` requires promotion to compile residual extraction/coercion before destination settlement and forbids destination-specific shortcuts. `.10x/knowledge/source-destination-extension-invariant.md` requires shared semantics to remain outside adapters.

## Decision

The correction package/request boundary carries a compiler-produced canonical `residual-json-v1` one-field envelope as the sole execution authority for each promoted value. The envelope path MUST equal the promoted JSON pointer and its recorded Arrow type MUST equal the compiled nullable output field type after promotion coercion. Generic validation decodes the envelope exactly and rejects path/type/value disagreement before destination mutation.

The compiled destination field name and canonical Arrow field travel with the correction operation. A destination consumes the validated exact value through the shared codec and its ordinary Arrow-to-destination mapping; it MUST NOT parse `promoted_value_json`, infer normalization, reinterpret residual strings, or accept a destination-native SQL literal as authority.

The existing `promoted_value_json` field remains readable for RP3 artifact compatibility and human inspection but is not execution authority. New executable batch correction requests MUST fail closed when the canonical envelope is absent or invalid. Plan/package/receipt evidence binds the exact operation set so replay verifies the same values.

## Alternatives considered

- Treat `promoted_value_json` as ordinary JSON: rejected because JSON cannot represent the closed Arrow vocabulary exactly and does not distinguish storage-unit temporals, decimals, binary, maps with non-string keys, or non-finite floats.
- Let each destination parse the string using its native types: rejected because identical plans could commit different values and every destination would reproduce compiler semantics.
- Carry destination SQL literals: rejected because they are non-portable, unsafe as an interchange authority, and bypass canonical package evidence.
- Invent a second correction-only scalar codec: rejected because `residual-json-v1` already supplies the exact versioned representation promotion consumes.

## Consequences

- Correction execution has one portable value truth from retained residual through package, destination settlement, replay, and receipt verification.
- Adding a destination requires only the shared decoded Arrow value plus its existing type mapping, not a promotion parser.
- Legacy RP3 artifacts remain inspectable, but they cannot be executed until replanned with exact value envelopes.
- The generic correction protocol must validate envelope/path/type bindings and serialize a deterministic digest of the correction operation set into receipt evidence.
