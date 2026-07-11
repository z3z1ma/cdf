Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/injected-execution-host-runtime-ownership.md, .10x/specs/execution-host-structured-runtime.md, .10x/tickets/2026-07-11-p3-a4-injected-execution-host.md

# Execution host shaping evidence

## What was observed

Production CLI commands use `futures_executor::block_on`; file transport owns a global multi-thread Tokio runtime; Parquet destination owns a current-thread runtime and blocks object-store calls. No shared CPU/lane/cancellation authority exists.

## Procedure

Searched production/test source and Cargo graphs for runtimes, blocking bridges, threads, Tokio/Rayon, and execution entry points; inspected the concrete transport/destination implementations and CLI composition.

## What this supports

An injected runtime-neutral host contract, one standalone composition root, distinct I/O/CPU/blocking classes, driver-declared lanes, CPU-slot admission, and structured child ownership.

## Limits

This is shaping evidence. L5/A4 must measure and select the default CPU executor and prove embedded/cancellation/oversubscription behavior.
