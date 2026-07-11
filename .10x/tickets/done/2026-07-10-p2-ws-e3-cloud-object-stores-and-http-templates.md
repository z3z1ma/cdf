Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-ws-e-remote-transports.md
Depends-On: .10x/tickets/done/2026-07-08-p2-ws-e1-file-transport-facade-local-http.md, .10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md

# P2 WS-E3 — Cloud object stores and finite HTTP templates

## Scope

Complete the shared file-transport facade for S3, GCS, and Azure; preserve secret and egress boundaries; plan recursive cloud globs as deterministic file partitions; make ranged Parquet reads and multi-file discovery use the same facade; and support finite HTTP numeric templates.

## Acceptance criteria

- S3, GCS, and Azure URLs list, head, and range-read through `FileTransport` with no provider branch in format readers.
- `credentials` compiles as a secret reference, is resolved only inside the transport adapter, and is redacted from diagnostics.
- Recursive object-store globs produce stable, sorted per-file partitions.
- Multi-file Parquet discovery reconciles all selected files into one pinned snapshot with file identities recorded.
- HTTP `{NN..MM}` expansion plans one partition per existing object and rejects unbounded wildcard enumeration precisely.
- Deterministic in-memory conformance tests cover listing, range reads, glob partitioning, credentials/egress ordering, and discovery. Network live-tier evidence remains owned by WS-I.

## Explicit exclusions

- Zip member enumeration.
- Provider-specific credential fields in resource declarations.
- A bespoke HTTP directory scraper.

## References

- `.10x/decisions/object-store-credentials-and-http-enumeration.md`
- `.10x/specs/data-onramp-file-sources-transports.md`
- `VISION.md` §§8.2, 8.6, 17.2

## Evidence expectations

Targeted crate tests, workspace check/clippy, generated schema verification, adversarial review, and a reproducible evidence record.

## Progress and notes

- 2026-07-10: User granted autonomous ratification authority. Selected the shared `object_store` facade and finite HTTP range grammar described by the governing decision.
- 2026-07-10: Added S3/GCS/Azure resolution behind `FileTransport`, secret-ref provider options, pre-network egress enforcement, recursive cloud glob partitioning, bounded remote Parquet footer probes, and finite width-preserving HTTP numeric templates.
- 2026-07-10: Refactored resource discovery so local and remote binary candidates share selection, schema aggregation, normalization, manifest generation, quarantine classification, and effective-schema runtime evidence. Remote pinned resources now re-observe through injected transport dependencies.
- 2026-07-10: Acceptance evidence is `.10x/evidence/2026-07-10-p2-ws-e3-cloud-object-stores.md`; adversarial review is `.10x/reviews/2026-07-10-p2-ws-e3-cloud-object-stores-review.md`.

## Blockers

None.
