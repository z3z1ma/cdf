Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/neutral-foreign-stream-boundary.md, .10x/specs/foreign-stream-interop.md, .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md

# Foreign interop shaping evidence

## What was observed

Python C-data detection exists but production returns materialized batch vectors and has no real local PyArrow zero-copy proof. Subprocess buffers complete stdout/stderr with `wait_with_output`. WASM has no runtime implementation.

## Procedure

Traced current Python/subprocess producer, decoder, ownership, supervision, memory, and evidence paths and compared them with runtime/source/memory contracts.

## What this supports

A neutral incremental foreign producer contract, separate Python/subprocess migrations, falsifiable copy taxonomy, and prospective-only WASM model.

## Limits

No performance measurement was run; H1 owns the first comparable evidence.
