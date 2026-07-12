Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: .10x/tickets/done/2026-07-12-p0-object-store-quick-xml-advisory.md
Verdict: pass

# Object-store quick-xml advisory review

## Assumptions tested

- The fixed parser is actually selected by the cloud-enabled object-store edge, not merely added alongside the vulnerable version.
- S3, GCS, and Azure remain provider-constructible through the shared transport dependency rather than acquiring provider-specific runtime branches.
- The dependency change does not introduce an unpinned git source or silently exempt the advisory.
- CDF does not retain a direct 0.13 declaration or a Reqwest 0.12 runtime client.

## Findings

No critical or significant implementation finding remains. `cargo tree -i quick-xml` has one 0.41.0 parser edge; Cargo Deny passes advisories and the exact Apache git source is allowlisted; all CDF-owned manifests share one exact object-store revision. Provider construction and the existing transport/destination suites pass.

The DataFusion pin still compiles a featureless object-store 0.13.2 package. This is explicit in `.10x/decisions/object-store-security-pin.md`; it does not select quick-xml, cross the CDF transport boundary, or provide a legacy execution path. Its removal belongs to the upstream DataFusion tuple advance and is not a reason to maintain a CDF fork.

## Verdict

Pass. The vulnerable executable path is removed without vendoring or a provider-specific shim.

## Residual risk

Live cloud behavior still depends on provider services, credentials, and emulators not exercised by the hermetic suite. Cargo Vet uses explicit exemptions for most of the repository graph; its passing status proves policy consistency, not independent source audit coverage.
