Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a5e-streaming-graph-integration.md, .10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/specs/streaming-operator-graph.md

# Segment encode/registration frontier

## What was observed

`PackageBuilder` previously combined IPC encode, hash, file sync/rename/directory sync, and receipt/segment journal registration in one mutable call. That made safe parallel encoding impossible because completion order would become manifest segment order.

The package crate now exposes a cloneable `PackageSegmentEncoder` that owns only the package directory and returns an opaque durable encoded-segment receipt. `PackageBuilder::register_encoded_segment` is the sole journal publication point. The existing direct segment writer delegates to encode then register, so there is one implementation rather than a compatibility path.

## Procedure

- A focused test encodes segment 2 before segment 1, registers 1 then 2, finalizes, and proves manifest segment order is 1 then 2.
- Focused package test passed.
- Strict all-target/all-feature package Clippy passed after removing mutability made obsolete by the thread-safe boundary.

## What this supports or challenges

This establishes the destination/source-neutral concurrency seam for parallel segment encoding. Durable file completion may be out of order; package identity and downstream visibility remain controlled by the canonical registration frontier.

## Limits

The engine has not yet scheduled these encoders concurrently in this milestone. Orphan durable segment files after a failed worker remain inside an unfinalized package draft and cause finalization to fail closed.
