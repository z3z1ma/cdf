Status: open
Created: 2026-07-12
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: None

# P0: remove object-store quick-xml namespace allocation vulnerability

## Scope

Remove RUSTSEC-2026-0195 from the locked production graph. `object_store 0.13.2` currently resolves `quick-xml 0.39.4`; upgrade the pinned object-store tuple or apply a ratified dependency override that resolves `quick-xml >= 0.41.0`, then revalidate S3/GCS/Azure transport behavior and the dependency supply-chain gates.

## Acceptance criteria

- `cargo deny --locked check` reports no RUSTSEC-2026-0195 vulnerability.
- All first-party `object_store` consumers use one ratified pinned tuple with no stale 0.13.2 declaration.
- S3, GCS, Azure, Parquet destination, discovery, and transport conformance remain green.
- Dependency review records API/behavior changes, feature graph, licenses, sources, audit/vet status, and any residual advisory risk.

## Evidence expectations

Lockfile path before/after, `cargo tree -i quick-xml`, deny/audit/vet output, affected transport/destination tests, and adversarial dependency review.

## Explicit exclusions

No transport feature work or XML codec implementation.

## Blockers

None. The advisory was observed during the P3 B1 Snappy dependency gate on 2026-07-12.

## References

- https://rustsec.org/advisories/RUSTSEC-2026-0195
- `.10x/decisions/cdf-book-decision-register.md` (D-28)
