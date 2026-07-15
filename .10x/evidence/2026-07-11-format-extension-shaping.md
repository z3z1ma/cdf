Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/native-format-driver-and-byte-source-boundary.md, .10x/specs/native-format-codec-runtime.md, .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md

# Format extension shaping evidence

## What was observed

Format/compression identity is duplicated in closed enums and match trees; discovery and runtime are separate dispatch families; `FormatRead` and codec readers collect batches; compressed row formats buffer full decoded bytes; remote non-Parquet spools fully; remote Arrow IPC is rejected; all parser dependencies share `cdf-formats`.

## Procedure

Traced declarations, compilation, detection, discovery, local/remote runtime, transport, reader functions, return types, and Cargo manifests, then compared them to the active extension invariant and P3 memory/host graph.

## What this supports

A neutral byte-source/transform/format contract, registry-driven configuration, driver-owned physical decode/discovery, shared reconciliation, logical-file decode units, and dependency-isolated codec crates.

## Limits

This is shaping evidence. FX1/WS-B must prove build graph improvement, compatibility, bounded decoding, and per-codec performance.
