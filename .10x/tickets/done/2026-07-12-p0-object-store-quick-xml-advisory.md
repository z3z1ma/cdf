Status: done
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

## Progress and notes

- 2026-07-12: Moved every CDF-owned object-store consumer to exact Apache revision `c7316d29face118e7409eead0cda098f38589428`, selecting object-store 0.14.1 and quick-xml 0.41.0.
- 2026-07-12: Aligned CDF CLI's direct HTTP client to Reqwest 0.13.4 and ratified the temporary featureless DataFusion 0.13.2 edge in `.10x/decisions/object-store-security-pin.md` without adding a fork, vendor patch, or type bridge.
- 2026-07-12: Added provider-construction regression coverage for S3, GCS, and Azure; affected tests, strict Clippy, Cargo Deny, and Cargo Vet pass. Evidence: `.10x/evidence/2026-07-12-object-store-quick-xml-advisory-removal.md`. Review: `.10x/reviews/2026-07-12-object-store-quick-xml-advisory-review.md`.

## Retrospective

Security upgrades at a shared transport boundary must be evaluated as dependency tuples, not leaf-version edits. The clean path was to advance every CDF-owned consumer together, preserve the shared facade, and record the upstream-only duplicate honestly rather than manufacture a CDF compatibility fork.

## References

- https://rustsec.org/advisories/RUSTSEC-2026-0195
- `.10x/decisions/cdf-book-decision-register.md` (D-28)
