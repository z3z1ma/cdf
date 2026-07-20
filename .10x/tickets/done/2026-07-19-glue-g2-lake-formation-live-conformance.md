Status: done
Created: 2026-07-19
Updated: 2026-07-20
Parent: .10x/tickets/done/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/done/2026-07-19-glue-g1-external-table-source.md, .10x/tickets/done/2026-07-19-iceberg-i1-catalog-discovery.md

# Glue G2: Lake Formation authority and live conformance

## Scope

Implement Glue/Lake Formation metadata authorization, table/partition credential vending and renewal, requested-column audit context, exact permission-mode handling, worker-local secret resolution, local protocol conformance, and authorized read-only FQ12 reachability evidence. Disposable governed-fixture provisioning was split to cancelled G3 because it requires separate external mutation/IAM authority.

## Non-goals

No silent ambient-S3 fallback, unsupported cell-filter approximation, catalog mutation beyond disposable fixture setup, or retained cloud infrastructure.

## Acceptance Criteria

- Full-table and supported column-scoped reads use vended least-authority credentials and renew safely during long runs.
- Unsupported cell/nested filters fail closed before S3 access with Athena/Trino remediation.
- Credentials never enter plans/tasks/packages/logs/evidence; workers resolve references locally.
- Local protocol fixtures cover table/partition scope, expiry/retry, denial/redaction, no-op incrementality, and the complete governed execution path; read-only FQ12 proves protocol reachability and records the environment boundary.

## References

- `.10x/specs/aws-glue-external-table-source.md`
- `.10x/specs/iceberg-source.md`
- `.10x/specs/portable-partition-task-protocol.md`

## Assumptions

- User-ratified 2026-07-19: FQ12 is the live integration environment; concrete cloud mutation remains separately confirmed.

## Journal

- 2026-07-20: Activated after Glue G1 and the Iceberg Glue catalog binding closed. The implementation must extend the shared AWS control authority and Glue adapter without serializing credentials into plans/tasks or adding a Glue/Lake Formation branch to generic runtime code.
- 2026-07-20: Generalized `cdf-aws` from AWS JSON 1.1 targets to one bounded signed control client supporting both JSON targets and REST-JSON paths. Glue owns its unfiltered-metadata protocol; Lake Formation owns credential vending; neutral object access consumes only `AwsCredentialProvider` and never names either service.
- 2026-07-20: Implemented the exact governed workflow: ordinary `GetTable` classifies registration, `GetUnfilteredTableMetadata` freezes authorized/effective columns plus one query session, governed partition enumeration reuses that authorization, and table/partition credentials vend with `SELECT` plus `COLUMN_PERMISSION`. Row/cell/nested filtering fails before S3 with Athena/Trino remediation. Live FQ12 inspection found that the service requires `QueryStartTime` despite its model marking the field optional; the exact start time now follows table metadata, partition metadata, and table credential vending.
- 2026-07-20: Runtime sessions remain outside every plan/task/artifact. A configurable bounded binding cache owns per-table/partition refresh providers; eviction is operational rather than failure, active bindings remain alive by `Arc`, expired sessions refresh single-flight, and object-store clients retain the provider so long runs renew without rebuilding tasks. The shared object-store pool is deliberately consulted only after runtime authority, preventing a pre-existing ambient client from bypassing Lake Formation.
- 2026-07-20: Read-only FQ12 inventory confirmed `bronze.transactions` is not Lake Formation registered and the only registered location is an S3 Tables wildcard. A correctly formed `GetUnfilteredTableMetadata` probe then failed `AccessDeniedException`; no governed external-table fixture and no permission to provision one exist in the authorized read-only environment. No AWS resource was created or modified.

## Blockers

None. Disposable governed-fixture setup and teardown moved to `.10x/tickets/cancelled/2026-07-20-glue-g3-governed-fq12-fixture.md` with an explicit reactivation trigger.

## Evidence

- Local governed driver: `cargo test -p cdf-source-glue` passed 19 tests, including authorized metadata to schema, governed partition enumeration, runtime partition binding, cache identity, expiry refresh, redaction, exact filter refusal, ordinary Parquet execution, row-format execution, and no-op resume behavior.
- Neutral authority preservation: `cargo test -p cdf-aws -p cdf-object-access -p cdf-source-glue` passed; object access includes the permanent law that runtime AWS authority precedes a process-global client for the same S3 origin.
- Final local gate: `cargo test -p cdf-aws -p cdf-object-access -p cdf-source-glue -p cdf-source-iceberg --lib` passed 97 tests (one intentional million-entry slow test ignored); dependency-inclusive strict Clippy over `cdf-aws`, `cdf-object-access`, `cdf-source-files`, `cdf-source-iceberg`, `cdf-source-glue`, and `cdf-cli` passed with all targets and `-D warnings`; `cargo deny check` passed advisories, bans, licenses, and sources; targeted `git diff --check` passed.
- Live read-only FQ12: STS proved account `617739438897`; Glue reported `bronze.transactions` as an ungoverned external Iceberg table; Lake Formation listed only `arn:aws:s3tables:us-west-2:617739438897:bucket/*`; the query-session probe proved `QueryStartTime` is mandatory and then reached the expected permission boundary (`AccessDeniedException`). This proves protocol reachability and the environment limitation, not successful credential vending.

## Review

Adversarial pass: concerns resolved. The review traced every governed-table interaction from ordinary catalog classification through authorized metadata, partition planning, runtime credential refresh, neutral object-store construction, attestation, and execution. It found one significant authority-precedence defect (a process-global S3 store could win before runtime Lake Formation credentials) and one significant scope-proof omission (a reused partition binding did not prove later objects stayed beneath its selected prefix). Both were corrected and protected by regression tests. No Lake Formation identifiers occur in generic runtime/compiler crates; secrets remain runtime-only and redacted. Residual risk is limited to the unavailable live governed fixture named in Blockers.

## Retrospective

The difficult part was not AWS request serialization; it was preserving one credential authority across planning, listing, retry, attestation, and decode without leaking it into portable tasks. A binding-local object-store cache plus a neutral refresh-provider boundary solved that cleanly. Read-only live probing exposed an AWS contract surprise (`QueryStartTime` is operationally required despite optional model metadata), which local generated-model assumptions would have missed. The fixture limitation should remain explicit rather than being disguised as local conformance.
