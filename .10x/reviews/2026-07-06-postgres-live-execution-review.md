Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-postgres-live-execution.md
Verdict: pass

# Postgres live execution review

## Target

Review of the live Postgres execution patch in `crates/cdf-dest-postgres`, dependency changes in `Cargo.lock` and `crates/cdf-dest-postgres/Cargo.toml`, cargo-vet changes in `supply-chain/config.toml`, and evidence in `.10x/evidence/2026-07-06-postgres-live-execution.md`.

## Findings

- Resolved: The initial worker patch could return `Err` after a successful Postgres commit if appending the receipt to the package failed. The final API records package receipt append failure in `PostgresCommitOutcome::package_receipt_error` while preserving a committed, verifiable DB receipt. The live permission regression test proves this behavior.
- Resolved: The sheet declared exact decimal support while live row encoding initially did not support Decimal128/Decimal256. `rows.rs` now maps decimals to `NUMERIC(p,s)` and emits exact numeric text; unit and live tests cover Decimal128. Decimal256 schema mapping is covered by unit test; its text formatting uses Arrow's decimal array formatter.
- Resolved: Mirror tables initially used unqualified names while tests used schema-qualified targets. The transaction sets `search_path` to the target schema, fresh receipt verification applies the stored `target_schema`, and live tests query through the isolated schema.
- Resolved: Duplicate lookup initially used `(target, package_hash, idempotency_token)` while the mirror uniqueness key was `(target, package_hash)`. The final lookup matches the uniqueness key and live tests cover same package replay with a different token returning duplicate/no-op behavior.
- Accepted with evidence: The Postgres crate now depends on `cdf-package`, which carries an existing DuckDB transitive dependency. This is required to read canonical package IPC segments and is not new DuckDB behavior in the Postgres crate. The dependency expansion is explicit in `Cargo.lock`, cargo-vet exemptions, `cargo deny`, `cargo audit`, OSV, cargo machete, and semver-checks evidence.

## Verdict

Pass. The live child ticket can close, and the prior Postgres destination blocker is resolved.

## Residual risk

The live tests are local single-writer integration tests and do not stress concurrent writers against the same target. The destination sheet already declares `max_writers: Some(1)`, so this is consistent with the current contract rather than an open blocker.
