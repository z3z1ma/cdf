Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/specs/residual-variant-capture.md, .10x/tickets/done/2026-07-08-p1-e4-variant-capture-evolution-event.md

# P2 RP1 canonical residual envelope codec

## Scope

Implement the pure `residual-json-v1` model/codec over Arrow scalar and nested values, canonical JSON Pointer paths, structured Arrow type descriptors, and exact decode back to Arrow. Replace the current ad-hoc variant JSON conversion only at the codec boundary; do not change live verdict selection.

## Acceptance criteria

- Envelope serialization matches `.10x/specs/residual-variant-capture.md` for booleans, all integers/floats including non-finite symbols, decimals, UTF-8, binary, temporal/duration/interval, list/struct/map, and null.
- Integer/decimal/temporal precision and binary bytes round-trip exactly.
- RFC 6901 original-source paths escape deterministically; canonical field ordering and bytes are stable.
- Unsupported Arrow values return a typed residual-encoding error suitable for quarantine, never lossy display strings.
- Existing nested variant fixtures migrate through the new codec without implicit promotions.

## Evidence expectations

Property tests over generated Arrow arrays/scalars, canonical golden vectors, adversarial path/type/value cases, cross-version decode rejection, and no-I/O review.

## Explicit exclusions

No validation compiler, batch routing, package artifact, destination, CLI, or promotion behavior.

## Progress and notes

- 2026-07-10: Opened as the pure foundation lane.
- 2026-07-10: Added the lower-layer `cdf-contract` residual-owned Arrow descriptor and strict `residual-json-v1` codec without importing `cdf-project` or moving the project snapshot artifact model. The codec canonicalizes RFC 6901 paths over original source segments; sorts envelope fields and nested object keys; records structured types including child field metadata; encodes integer/decimal/temporal/interval storage exactly as canonical strings, finite and symbolic non-finite floats, unpadded base64url binary, and ordered map entries; and reconstructs one-value Arrow arrays of the recorded type. Decode rejects noncanonical bytes, malformed paths, mismatched encodings, invalid descriptors, and non-v1 envelopes. Unsupported Arrow types return typed `cdf.residual_encode_unsupported` errors.
- 2026-07-10: Replaced only `cdf-engine::variant_capture`'s ad-hoc nested JSON conversion with the new codec. `NestedAction::CaptureVariant` selection, source-column removal, contract-evolution evidence, quarantine interaction, and zero implicit promotions are unchanged. The live `_cdf_variant` field now records `cdf:variant_encoding = residual-json-v1`; the migrated E4 fixture asserts canonical envelope bytes and decodes all three captured nested values back to their original Arrow types.
- 2026-07-10: Focused codec coverage passed 13/13: canonical golden ordering/path escaping, complete scalar vocabulary including float16 and non-finite symbols, decimal256, binary/view/fixed binary, every temporal/duration/interval unit, null typed values, list/fixed-list/list-view, struct, non-string map keys, duplicate/invalid paths, canonical byte enforcement, encoding/type mismatch, unsupported version/type, and generated signed/unsigned integer, finite float16/float64, decimal, binary, list, and pointer cases. Full `cdf-contract` + `cdf-engine` nextest passed 92/92; all-target check and warnings-denied Clippy passed; workspace formatting and scoped diff checks passed. No blocker remains; parent owns independent review and closure.
- 2026-07-10: Parent integration verification and adversarial review passed. Evidence: `.10x/evidence/2026-07-10-p2-a10a-a10b-rp1-integration.md`. Review: `.10x/reviews/2026-07-10-p2-a10a-a10b-rp1-integration-review.md`.

## Blockers

None.
