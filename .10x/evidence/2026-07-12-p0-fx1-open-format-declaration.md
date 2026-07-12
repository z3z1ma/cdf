Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Relates-To: .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md

# FX1 open format declaration evidence

## What was observed

`FileFormatDeclaration` was a closed Rust enum containing CSV, JSON, NDJSON, Parquet, and Arrow IPC even though execution already selected `FormatDriver` values from an injected registry. An external driver therefore could not be named in declarative source options.

## Procedure

- Replaced the enum with a transparent string-backed declaration validated by neutral `FormatId` rules.
- Kept standard names as convenience constructors only.
- Updated compiler, source runtime, discovery dispatch, diagnostics, deep validation, and tests to compare the runtime id rather than pattern-match a closed type.
- Made source resolution require that the injected registry contains the compiled id.
- Restored remote Arrow IPC routing through the shared binary discovery path while touching the now-open dispatch.
- Ran focused serialization/validation and format-inference tests, affected workspace checks, and all-target Clippy with warnings denied.

## What this supports or challenges

An external format id can now cross TOML/JSON, compiled source-plan serialization, and resolution without changing a common enum. Invalid ids fail early and unregistered valid ids fail at composition with the exact id.

## Limits

Project schema discovery still maps the standard formats to private adapters. This change alone does not prove external discover/preview/run or delete row-format fallbacks; FX1 remains open.

