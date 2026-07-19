Status: blocked
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/2026-07-19-iceberg-i2-scan-execution.md

# Iceberg I3: snapshot incrementality, product parity, and full conformance

## Scope

Implement fixed-snapshot/time-travel and append-only snapshot ancestry/no-op semantics; complete preview/run/replay/product diagnostics; close local REST/filesystem and authorized FQ12 Glue/S3 performance/conformance for the ratified v1/v2 Parquet matrix.

## Non-goals

No changelog/tailing approximation, catalog writes, ORC/Avro/v3/encryption silent support, or persistent AWS fixture after testing.

## Acceptance Criteria

- Append ancestry selects only appended files and rejects rewrite/delete/divergent/missing history with exact remedies.
- Preview/run/replay after catalog advancement preserve the pinned snapshot and package identity.
- All source-extension conformance laws and local Iceberg matrix pass.
- Authorized FQ12 Glue/S3 run meets P3 network/Parquet overhead targets with resource setup/teardown and reproducible evidence.
- Unsupported capability matrix is explicit in plan/doctor/docs; every follow-up has a durable owner.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/specs/source-extension-runtime-contract.md`

## Assumptions

- User-ratified 2026-07-19: local and FQ12 live testing are required; external provisioning is confirmed separately when concrete resources are known.

## Journal

None yet.

## Blockers

I2. AWS external writes require confirmation at execution time.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
