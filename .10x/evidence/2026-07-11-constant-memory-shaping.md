Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/process-tree-constant-memory-proof.md, .10x/specs/constant-memory-proof.md, .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md

# Constant-memory shaping evidence

## What was observed

The active memory model correctly distinguishes process budget and managed pool but has not selected headroom/enforcement. Current production has confirmed input/package/listing/dedup/destination/metadata vectors, and native/allocator/child allocations can escape pool accounting.

## Procedure

Reviewed memory/envelope records, production buffering scan, P3 owner graph, and host measurement capabilities; classified managed/native/external/OS/child memory and enforceable versus observational hosts.

## What this supports

A process-tree cgroup/RSS/ledger proof, calibrated headroom, allocation owner audit, broad stress matrix, and clean-failure laws.

## Limits

This is shaping evidence. F1 selects exact values; F3/F4 supply actual completion/high-water proof.
