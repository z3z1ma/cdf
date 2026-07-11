Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md

# Adapter-declared blocking lanes

## What was observed

Destination runtime capabilities now carry neutral blocking-lane declarations. `DestinationRegistry::resolve` validates and installs those declarations through injected execution services, so the registry/scheduler contains no destination-id branch. The standalone host dynamically creates compatible pools, rejects conflicting declarations, and accounts native/internal parallelism against the global CPU-slot authority.

DuckDB declares a single pinned cooperative lane; Postgres declares a bounded shared cooperative lane; Parquet declares none because object-store work uses async host I/O.

## Procedure

- mock-adapter pinned lane registration/typed execution/affinity/conflict test — passed
- `cargo test -p cdf-runtime --lib` — 10 passed
- strict Clippy for runtime, engine, and destination crates — passed
- `cargo check -p cdf-cli --all-targets` — passed
- focused CLI Parquet destination run — passed

## What this supports

A new destination can declare execution needs in its own capability sheet and immediately use typed blocking execution without scheduler edits. Pinned tasks reuse one worker thread; all pools share the same CPU-slot budget.

## Limits

Existing DuckDB/Postgres commit implementations declare their lanes but have not yet moved their hot operations into the typed lane bridge; that migration is coordinated with their P3 bulk-path work.
