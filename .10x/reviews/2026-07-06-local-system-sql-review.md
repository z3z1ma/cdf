Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-local-system-sql.md
Verdict: pass

# Local System SQL Review

## Target

Review of local read-only `firn sql` implementation in `crates/firn-cli/src/system_sql.rs`, CLI wiring, and tests.

## Findings

No blocking findings.

A first worker attempt was shut down without integration. A second worker implemented a mostly CLI-local surface. Parent review removed an unnecessary public `firn-state-sqlite` history API so the lower checkpoint crate surface did not widen.

The read-only boundary is conservative: lexical filtering rejects non-SELECT/WITH and obvious mutating keywords outside comments and string literals, while SQLite's prepared-statement `readonly()` check must also pass. Persistent data is copied into an in-memory database before query execution, and tests prove empty-history and rejected mutating queries do not create the configured state DB or package root.

## Verdict

Pass. The local SQL child satisfies its scoped acceptance criteria and is appropriately narrower than the observability parent.

## Residual risk

The lexical gate is intentionally conservative and may reject harmless read-only SQL that uses a mutating keyword as an identifier or function name. That is acceptable for this first system-history surface. Future destination mirror or DataFusion-backed querying should get its own ticket rather than weakening this local gate.
