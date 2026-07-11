Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/spillable-package-order-dedup.md, .10x/specs/spillable-package-dedup.md, .10x/tickets/2026-07-11-p3-a6-spillable-package-dedup.md

# Spillable dedup shaping evidence

## What was observed

Current package dedup retains all accepted payload batches, owned keys, every row reference, retained masks, and a dropped-row vector. Exact-row evaluation happens before residual/variant materialization and output normalization despite the active decision requiring complete normalized output-row identity.

## Procedure

Inspected compiler/evaluator data structures, engine ordering, exact-row/keyed decisions, tests/evidence, and package summary shape; traced which allocations grow with rows and keys.

## What this supports

An explicit final-output-row barrier, in-memory-to-spill transition, external winner/ordinal join, collision-safe typed equality, and sharded provenance artifact migration.

## Limits

This evidence does not select the external algorithm or prove Arrow edge-case equality. A6 must benchmark and property-test both.
