Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# FX1 registered format discovery evidence

## What was observed

Project discovery still selected concrete Parquet and Arrow IPC probe helpers even after both formats executed through `FormatDriver`. That split prevented external format ids from reaching discovery and allowed discovery/run semantics to drift.

## Procedure

- Reworked the source-owned bounded local binary probe to resolve the injected format registry and call `FormatDriver::discover` over `LocalByteSource`.
- Recorded driver id/version plus byte-source generation/checksum evidence.
- Replaced project Parquet/Arrow discovery variants with one `Registered(FileFormatDeclaration)` adapter.
- Routed remote registered formats through the same driver after verified local spool, retaining the existing uncompressed Parquet ranged probe as the bounded-selective optimization.
- Preserved the legacy row adapters explicitly; they remain the next deletion owner.
- Ran affected checks, Clippy, and Parquet/compressed discovery fixtures.

## What this supports or challenges

Registered binary formats now use one discovery interpretation and project code does not name their implementation crates or parser APIs. A new registered binary driver can reach local and spooled-remote discovery without a project match arm.

## Limits

Format confirmation still has first-party magic signaling, row formats still bypass `FormatDriver`, and native remote `ByteSource` is G1 work. This is not the full external-codec law or FX1 closure.
