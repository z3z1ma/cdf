Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-postgres-destination.md
Verdict: concerns

# Postgres destination blocked review

## Target

Review of `crates/firn-dest-postgres/**`, `.10x/evidence/2026-07-06-postgres-destination.md`, and `.10x/tickets/done/2026-07-05-postgres-destination.md` against `.10x/specs/destination-receipts-guarantees.md`, `.10x/specs/package-lifecycle-determinism.md`, and `.10x/specs/project-cli-observability-security.md`.

## Findings

- Significant: The implementation exposes deterministic destination sheets, SQL plans, xid-bearing receipt construction, verify clauses, and drift hooks, but it does not execute commits against a live Postgres destination. The crate currently has no Postgres driver dependency and no commit-session implementation equivalent to the DuckDB destination. This is weaker than the ticket's acceptance criterion that append, transactional replace, and merge work with deterministic dedup.
- Significant: Live integration evidence is unavailable in the current environment. `pg_isready` reports `/tmp:5432 - no response`, `docker` is not installed, and `TEST_DATABASE_URL`/`DATABASE_URL` are unset. Without a server or test container, receipt verification, transaction rollback, driver row counts, DDL application, and `ON CONFLICT` behavior are not witnessed.
- Resolved: The earlier PyO3 advisory blocker was not owned by this ticket and is now resolved by the Python bridge dependency change. Parent integration revalidation passed `cargo audit`, `cargo deny check advisories`, and OSV.

## Verdict

Concerns raised. Keep the ticket blocked. The deterministic planning surface is useful and tested, but it is not enough to close a destination implementation ticket that requires real Postgres commit behavior and live receipt verification.

## Residual risk

The SQL strings are unit-tested for shape, but SQL syntax, transaction boundaries, idempotency race behavior, and mirror/receipt queries can still differ under a real Postgres server. Closure requires either a reachable Postgres-backed implementation/test path or an active superseding decision that narrows this child ticket to planning-only behavior.
