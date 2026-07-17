Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md

# Postgres first-party source driver

## What was observed

`PostgresSourceDriver` owns its source/resource option contract and compiles it into the neutral `CompiledSourcePlan`. Unknown options fail during compilation. Connections must be `secret://` references; compiled redacted options and physical plans retain the reference but never a resolved credential.

The driver declares a bounded shared `postgres-source.sync` lane plus poll/decode working sets, useful/max concurrency, retry granularity, read idempotency/reopen behavior, attestation strength, ordering, and telemetry authority. `SourceRegistry::resolve` installs any declared source lane generically before invoking the driver. Resolved Postgres resources run blocking client/query work through the injected execution host.

## Procedure

- focused Postgres driver strict-option/plan/lane test — passed.
- strict Clippy across runtime, shared Postgres protocol, Postgres source/destination, declarative, and project targets — passed.

## What this supports

The scheduler can admit and execute the Postgres adapter entirely from neutral declared capabilities, without a Postgres id branch or adapter-created thread pool.

## Limits

The existing declarative compatibility path has not yet been switched to registry compilation/resolution, so its direct resource wrapper remains until that migration lands. Source discovery/add/doctor hooks and driver schema composition remain open.
