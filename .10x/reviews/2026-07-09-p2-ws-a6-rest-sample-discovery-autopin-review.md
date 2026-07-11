Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-a6-rest-sample-discovery-autopin.md
Verdict: pass

# P2 WS-A6 REST sample discovery auto-pin review

## Target

Review of the A6 REST sample discovery and auto-pin implementation covering `cdf-declarative`, `cdf-project`, and `cdf-cli`.

## Findings

- Significant, fixed before closure: REST schema inference originally marked fields nullable only when explicit `null` appeared, not when a field first appeared after earlier sampled records. This would have made sample-order-dependent schemas and could reject valid later-nullable data. The implementation now tracks records seen before each field is introduced and marks late-appearing fields nullable.
- Significant, fixed before closure: REST discovery originally sampled `build_request_url` directly and ignored paginator `first_request` behavior. Cursor/page/offset resources could therefore sample a different URL shape than runtime execution. Discovery now applies `Paginator::first_request` when pagination is configured.
- Minor, fixed before closure: the shared REST send helper introduced to eliminate duplication tripped clippy's `too_many_arguments`. The request pieces are now grouped in a local `RestSendContext` without suppressing the lint.
- Minor, fixed before closure: snapshot construction duplicated Postgres and REST normalization/metadata wrapping. `build_schema_discovery` now owns the common normalization and snapshot artifact handoff.

## Verdict

Pass. The bounded A6 behavior is implemented and verified: REST sample discovery uses the existing runtime request path, CLI schema discovery does not write project artifacts, first-use plan/preview/run auto-pin snapshots, REST execution accepts discovered snapshot hashes, and secret values remain out of outputs and records.

## Residual risk

The main residual risk is intentionally out of scope: one-page sampling is not a complete S5 implementation. Pagination-wide sampling, drift/quarantine conformance, cursor inference, and `cdf add` remain owned by later P2 child work. CodeQL still has three pre-existing CLI test fixture findings owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`; A6 did not introduce new CodeQL findings.
