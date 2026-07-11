Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-a5-generic-discover-autopin-postgres-run.md
Verdict: pass

# P2 WS-A5 generic discover auto-pin for Postgres run and plan review

## Target

Review of A5: generic first-use discovery auto-pin for CLI plan/preview/run and declarative Postgres table execution from pinned discovered schemas.

## Findings

- Pass: CLI `run`, `plan`, and `preview` no longer call the local-Parquet-only preparer. They route through the generic discover preparer and then use the pinned compiled resource for planning and runtime opening.
- Pass: local single-file Parquet auto-pin behavior is preserved through the generic path and remains covered by project/CLI regression tests.
- Pass: declarative Postgres table resources in `SchemaSource::Discover` mode now auto-pin through the A4 catalog probe before plan/preview/run. The CLI test proves plan writes the schema snapshot, preview reads through the pinned schema without runtime artifacts, and run commits rows/checkpoint with the discovered snapshot hash.
- Pass: Postgres source execution now accepts `SchemaSource::Discovered` as a pinned execution schema source while unpinned `Discover`, `Hints`, and `Contract` remain rejected.
- Pass: source-name-aware Postgres reads are implemented at the SQL-generation boundary. SELECT, filter predicates, ordering, and stored-predicate validation use `cdf:source_name` for physical source columns while emitted Arrow fields remain normalized.
- Pass: secret handling is covered. The CLI integration uses `secret://file/sql-dsn`, asserts the resolved DSN/password do not appear in output or snapshot JSON, and the source debug path still redacts the connection string.
- Pass: parent-observed focused tests, affected-crate tests, full workspace clippy/test, Semgrep, Gitleaks, CodeQL, jscpd, rust-code-analysis, and supply-chain gates were run. Reported residuals are known/ratified or test-harness duplication, not A5 implementation failures.

## Residual Risk

- Discovery as a whole is still not complete. REST sample-page discovery, Python generator discovery, WASM boundary discovery, future Avro-like file discovery, CSV/JSON/NDJSON sampling, remote Parquet ranged discovery, multi-file schema union/variance, `cdf schema pin|show|diff`, `cdf add`, ad-hoc mode, and S4/S5 conformance remain open under WS-A/P2.
- Broad touched-file `jscpd` continues to report duplication in existing test harnesses. Implementation-only `jscpd` is clean; large test-harness consolidation should be handled by a separate owner if it becomes worth the churn.
- CodeQL still reports the three pre-existing CLI test-fixture hard-coded cryptographic values owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.
- OSV still reports only `RUSTSEC-2024-0436`, covered by the active ratified exception.

## Verdict

Pass. A5 closes the Postgres table plan/preview/run auto-pin slice and removes the immediate "catalog discovery doorway to nowhere" gap without overstating product-wide discovery completion.
