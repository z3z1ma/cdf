Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a4-injected-execution-host.md

# DuckDB commit executes on its declared pinned lane

## What was observed

Registry-resolved DuckDB destinations retain injected execution services. Data and empty-package commit bodies move their owned inputs plus a cloned destination onto `duckdb.connection`, the adapter-declared pinned lane. Direct library construction retains an explicit no-host compatibility path for tests/embedding callers that did not request managed execution.

The synchronous typed lane bridge now joins CPU-slot release with standard channels rather than nesting an async executor, so it is safe when the orchestration root is already being polled by the host compatibility executor.

## Procedure

- focused full CLI local-file-to-DuckDB commit/mirror/checkpoint test — passed
- strict Clippy across runtime, engine, DuckDB, and CLI targets — passed
- prior pinned-affinity and dynamic lane admission test — passed

## What this supports

The production DuckDB hot commit body is confined to a single host-owned pinned worker and participates in global CPU admission. The orchestration and registry contain no DuckDB branch.

## Limits

Postgres session operations remain declared but not yet moved to their shared lane; that work is paired with the binary COPY session rewrite.
