Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/adaptive-microbatch-canonical-segmentation.md, .10x/specs/canonical-segmentation-adaptive-batching.md, .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md

# Canonical segmentation shaping evidence

## What was observed

Formats default to 1,024 rows, REST follows response-page size, and the engine maps each accepted batch to a global encounter-order segment. Source chunking and future scheduler order therefore determine fixed costs and package hashes. Package trace is identity-participating, so pressure telemetry cannot be recorded there safely.

## Procedure

Traced source batch options, engine output writing, segment ids, positions, manifest identity, package trace, dedup order, and P3 determinism requirements.

## What this supports

Separating pressure-adaptive internal microbatches from plan-deterministic canonical segments, with partition-scoped ids and typed position algebra.

## Limits

This is shaping evidence. L5 selects initial targets; A3 must prove rechunking/jobs invariance and perform the artifact/golden migration explicitly.
