Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/compiled-fused-streaming-operator-graph.md, .10x/specs/streaming-operator-graph.md, .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md

# Streaming operator graph shaping evidence

## What was observed

Ordinary engine batches persist promptly, but package-global dedup retains all accepted batches; package commit readers return all decoded commit segments in a vector; segment hashing rereads persisted bytes; and identity/control metadata accumulates in resident collections.

## Procedure

Traced production extraction, contract/dedup, package write/read, replay, and destination commit paths and compared them against active memory, segmentation, staged-ingress, host, and commit-gate contracts.

## What this supports

A compiled fused graph, accounted ownership-transfer edges, explicit stateful barriers, one durable segment handoff, bounded canonical metadata sinks, and one run/replay destination path.

## Limits

This is source-backed shaping evidence, not constant-memory or throughput proof. L5 and A5 children must record measured before/after behavior.
