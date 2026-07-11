Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-08-p1-product-ws6c-runnable-examples-conformance.md, .10x/tickets/done/2026-07-08-p1-product-ws6-docs-onboarding.md

# P1 runnable examples and docs closure

## What was observed

`examples/rest-fixture/` is a complete local REST-to-DuckDB project with checked-in JSON data and exact server/CLI commands. `examples/postgres/` is a complete Postgres-to-DuckDB project with exact setup and CLI commands. The Postgres DSN is represented only by `secret://file/postgres-dsn`; the runtime value is ignored and never committed.

Conformance copies each checked-in project without synthesizing its structure, supplies only the documented local service endpoint/secret, and executes `cdf validate --deep`, `cdf plan`, and `cdf run`. Both produce the configured DuckDB destination.

## Procedure

```text
cargo test -p cdf-conformance --locked run_matrix::examples --no-fail-fast
  rest_fixture_example_executes_as_a_project ... ok
  postgres_example_executes_as_a_project ... ok
  2 passed; 0 failed
cargo clippy -p cdf-conformance --all-targets --locked -- -D warnings
  passed
gitleaks detect --no-git --source examples ...
  no leaks found
git diff --check
  passed
```

Both invocation helpers also assert that the live Postgres DSN is absent from stdout and stderr.

## What this supports

WS6C and the final WS6 parent acceptance criteria are complete. Generated references/freshness are evidenced separately in `.10x/evidence/2026-07-10-p1-generated-command-error-reference.md`; topology, quickstart, operator guides, and scaffold README are owned by completed WS6A/WS6D records.

## Limits

The Postgres test requires `TEST_DATABASE_URL` or local `initdb`/`pg_ctl`, matching the documented conformance harness prerequisite. The public TLC quickstart remains network-dependent and explicitly provides deterministic fixture commands when the public CDN denies access.
