Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/reviews/2026-07-10-parquet-promotion-identifier-policy-independent-review.md
Verdict: pass

# Parquet identifier-policy independent-review repair response

## Target

This review assesses only the two blocking findings in `.10x/reviews/2026-07-10-parquet-promotion-identifier-policy-independent-review.md` against `.10x/decisions/destination-protocol-capabilities-extension-seam.md` and `.10x/decisions/parquet-column-and-object-key-identifier-rules.md`.

## Findings resolved

### Resolved — significant: public `DestinationSheet` construction compatibility

`object_key_rules` was removed from `DestinationSheet`, including every downstream literal added by the first implementation. `ObjectKeyRules` now occupies an optional, default-`None`, omitted-when-absent field on the existing non-exhaustive `DestinationProtocolCapabilities` aggregate. The aggregate provides `with_object_key_rules` and `object_key_rules` accessors and validates the nested versioned rules. `DestinationSheetArtifact` and `LockedDestination` already snapshot this aggregate; kernel serialization and project lock round-trip tests now cover the object-key claim explicitly.

`cargo semver-checks check-release -p cdf-kernel --baseline-rev HEAD` passes all 196 checks and reports no semver update required.

### Resolved — significant: declared policy did not govern construction

Parquet constructs an `ObjectKeyEncoder` only from validated `DestinationProtocolCapabilities`. Its policy dispatch is exhaustive over `ObjectKeyPolicy`; all ordinary segment/manifest/replace-pointer paths and all correction object/manifest/receipt/version paths require that encoder. There is no destination-name branch and no untyped fallback.

The negative regression passes default capabilities without object-key rules to the encoder and requires a fail-closed error. The same test passes Parquet's declared capabilities and asserts exact component-v1 bytes for characters requiring escaping. Existing ordinary/correction parity, receipt verification, and 100-run golden tests prove the production paths retain current bytes.

## Review checks

- Public sheet literals no longer contain `object_key_rules`; repository search finds the field only on protocol capabilities and its consumers/tests.
- Legacy sheets and default protocol capabilities serialize without a new slot; non-default Parquet artifacts and locks serialize `object_key_rules` under `protocol_capabilities`.
- Static Parquet lock generation uses `destination_sheet_artifact()` and therefore snapshots the same validated claims as the runtime protocol without materializing destination storage.
- `ObjectKeyEncoder::from_capabilities` rejects missing rules and validates rule versions before an exhaustive policy match.
- Every key helper accepts `ObjectKeyEncoder`; direct component-v1 encoding is private behind its exhaustive dispatch.

## Verdict

Pass. Both blocking findings are resolved at the ratified extension seam, with source compatibility restored and object-key declarations made executable and falsifiable.

## Limits

- The enum currently has one valid policy. The negative missing-declaration test plus exhaustive match makes a future variant a compile-time integration obligation; no speculative second wire policy was invented solely for testing.
- External object-store service coverage remains outside this ticket. The filesystem and in-memory stores exercise the same key encoder and receipt verification paths.
