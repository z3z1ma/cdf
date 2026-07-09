Status: done
Created: 2026-07-09
Updated: 2026-07-09

# P2 dlt filesystem, naming, and schema-contract study

## Question

Which field-tested dlt source-experience behaviors should inform P2 without weakening CDF's pinned discovery, plan artifact, total verdicts, commit gate, or replay determinism?

## Sources and methods

Read the current official dlt documentation on 2026-07-09:

- Filesystem tutorial: https://dlthub.com/docs/tutorial/filesystem
- Verified filesystem source: https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem
- Naming conventions: https://dlthub.com/docs/general-usage/naming-convention
- Schema contracts: https://dlthub.com/docs/general-usage/schema-contracts
- Schema evolution: https://dlthub.com/docs/general-usage/schema-evolution

Compared those behaviors with `VISION.md` sections 2.1, 7.4, 8.6, 9.2, 11, and 13.3 plus the active P2 decisions and specifications.

## Findings

- dlt separates file listing/metadata filtering from file-content reading and lets local, S3, GCS, Azure, and public CDN locations share one filesystem-source shape. Incremental file filtering is framework-managed state rather than user-authored extraction logic.
- dlt applies destination-aware naming during normalization, persists the naming convention with schema state, shortens identifiers deterministically, and detects case/collision failures before data is mangled.
- dlt treats declared columns and hints as refinements over inferred schemas, and its default `evolve` contract admits columns while routing incompatible types to variants; stricter modes can freeze or discard at row/value grain.
- dlt's continuous inference and mutable schema posture is intentionally not CDF's posture. CDF MUST infer through a bounded compiler probe, pin the output into a hash-addressed snapshot, and treat later drift as a serialized contract event.
- dlt does not preserve source identifiers as completely as CDF requires. CDF's `cdf:source_name` and normalizer-version evidence remain differentiators, not compatibility details.

## Conclusions

P2 should import the transport facade, managed file incrementality, transparent compression, automatic boundary normalization, collision detection, and hints-over-inference ergonomics. It must not import perpetual schema mutation or a convenience path that bypasses plan/package/receipt/checkpoint evidence.

The active P2 decisions already encode these conclusions. This research record supplies durable provenance for the directive's mandatory dlt study; it does not independently ratify unresolved syntax, credential payload, HTTP enumeration, or dedup semantics.

## Limits

The study used public documentation, not dlt source-code experiments or performance measurements. The cited pages are temporally mutable and should be rechecked if a future decision depends on a specific dlt implementation detail rather than the product behaviors summarized here.
