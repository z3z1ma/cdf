Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-12-p3-ws-j-datafusion-currency-bridges.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/2026-07-11-p3-g1-streaming-transport-byte-sources.md

# P3 J2: DataFusion object-store registry bridge

## Scope

Compose DataFusion session object-store registration from CDF's injected, secret-resolved, egress-checked transport providers while retaining one credential, retry, generation, and memory authority.

## Acceptance criteria

- DataFusion sessions install the existing shared finite memory pool and CDF-authorized object stores.
- Secrets are resolved only through CDF providers and never serialized or logged.
- Native and DataFusion access enforce the same egress, generation/precondition, retry, cancellation, and redaction policy.
- Adding a transport provider does not add an engine match branch.
- Duplicate clients/pools and superseded independent credential paths are deleted.

## Evidence expectations

Mock local/HTTP/S3/GCS/Azure registry conformance, credential/redaction adversaries, generation-change tests, pool/connection reuse measurement, dependency checks, and review.

## Explicit exclusions

No codec migration or DataFusion datasource adoption.

## Blockers

G1 must establish the final injected provider contract.

