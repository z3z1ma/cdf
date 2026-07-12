Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# FX1 external codec and transform law evidence

## What was observed

Registry unit tests proved descriptor collisions but did not prove an implementation unknown to production orchestration could traverse the live file path.

## Procedure

Added test-only external implementations of `FormatDriver` and `ByteTransformDriver`. The fixture:

- registers both into otherwise empty injected registries;
- resolves `format = "external_mock"` and auto-detects the `.mt` transform;
- streams the transform into the checksum/publication spool;
- discovers the fixed physical schema through `FormatDriver::discover`;
- plans and decodes through `stream_registered_format`;
- verifies one row, file-manifest source position, and zero retained memory after drop.

The focused test passed, followed by all-target `cdf-source-files` Clippy with warnings denied.

## What this supports or challenges

This proves the neutral contracts are executable rather than descriptor-only and that adding a codec/transform requires no production runtime dispatch edit for local explicit resources.

## Limits

The implementation is a conformance fixture, not a shipped format. It does not yet prove remote native `ByteSource`, project `cdf add`/pin, malformed/cancellation matrices, or build-domain isolation; those remain active FX1/G1 work.

