Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/kernel-owned-stream-epoch-policy.md, .10x/specs/stream-epochs-watermarks.md, .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md

# Stream epoch shaping evidence

## What was observed

The engine owns an incomplete boundedness enum; drain has no policies and live uses optional primitives/free-form watermark text. Kernel batches/checkpoints already provide the lower-level provenance/state seams, but no typed epoch/watermark policy or executor exists.

## Procedure

Compared VISION 6.5–6.6 and 25.3 with active specs/tickets and searched production consumers/types.

## What this supports

Kernel artifact correction, policy compilation, deterministic drain epochs, and focused conformance before later resident supervision.

## Limits

This is shaping evidence, not runtime proof.
