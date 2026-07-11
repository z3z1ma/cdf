Status: done
Created: 2026-07-08
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/decisions/data-onramp-file-source-transport-manifest.md, .10x/specs/data-onramp-file-sources-transports.md

# P2 WS-E remote transports

## Scope

Integrate a file-source transport facade over local files, HTTP(S), S3, GCS, and Azure Blob with secret references, egress allowlists, listing/template enumeration, ranged reads, streaming reads, and spool behavior.

Split executable child tickets before code for facade traits, local/HTTP implementation, object-store-backed cloud transports, secret/egress enforcement, doctor probes, and remote discovery/read conformance.

## Acceptance criteria

- `https://` public Parquet sources support ranged footer discovery and streaming reads.
- `s3://`, `gs://`, and `az://` sources resolve credentials through `secret://` references and obey egress allowlists.
- Remote listing and HTTP template enumeration feed the same file partition model as local globs.
- Non-seekable formats spool only under explicit memory/disk budgets and do not bypass package evidence.
- `cdf doctor` can probe configured transports without leaking secrets.

## Evidence expectations

Unit tests with in-memory or fixture transports, HTTP ranged-read tests, secret redaction tests, egress denial tests, doctor output snapshots, and live-tier evidence where credentials/network are available.

## Explicit exclusions

Arbitrary web directory scraping is out of scope. HTTP glob support is limited to ratified template/range enumeration.

## Progress and notes

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-08: Split first executable child `.10x/tickets/done/2026-07-08-p2-ws-e1-file-transport-facade-local-http.md` for the local/HTTP facade and ranged-read foundation.
- 2026-07-09: WS-E1 closed with local/HTTP(S) facade, deterministic metadata records, bounded HTTP ranged-read tests, explicit HTTP listing rejection, and allowlist/auth API hooks. Remaining WS-E scope still owns production integration, cloud transports, credential resolution, doctor probes, HTTP template enumeration, compression, and full remote conformance.
- 2026-07-09: Parent verification for WS-E1 added debug redaction for URL-bearing public transport/request metadata surfaces and recorded full quality evidence, including reusable-DB CodeQL. No WS-E1 blocker remains.
- 2026-07-09: Split executable child `.10x/tickets/done/2026-07-09-p2-ws-e2-http-file-runtime-and-discovery.md` for production HTTPS single-file runtime integration and remote Parquet ranged discovery.
- 2026-07-09: E2 closed with evidence `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md` and review `.10x/reviews/2026-07-09-p2-e2-g1-b4-batch-review.md`. Deterministic HTTPS single-file Parquet now supports bounded discovery and plan/preview/run through the file transport facade. Remaining WS-E scope: HTTP template/glob enumeration, S3/GCS/Azure object-store transports, cloud credential handling, doctor probes, remote multi-file manifest behavior, and live-tier evidence.
- 2026-07-10: Closed WS-E3 at `.10x/tickets/done/2026-07-10-p2-ws-e3-cloud-object-stores-and-http-templates.md`. S3/GCS/Azure now share the file facade; recursive cloud globs and finite HTTP numeric templates plan per-file partitions; multi-file remote Parquet discovery and pinned drift observation share the local reconciliation/manifest engine. Remaining parent scope is transport doctor probes, budgeted remote row-format streaming/spool behavior, and WS-I live-provider evidence.
- 2026-07-10: Closed WS-E4 at `.10x/tickets/done/2026-07-10-p2-ws-e4-transport-doctor-probes.md`. Doctor now preflights configured remote resources through production partition resolution and reports isolated redacted checks. Remaining parent implementation scope is WS-E5 remote row-format/compression streaming; live-provider evidence remains WS-I.
- 2026-07-10: Closed WS-E6 at `.10x/tickets/done/2026-07-10-p2-ws-e6-http-date-glob-and-s1-s2-conformance.md`. Canonical year-month HTTP wildcards now enumerate deterministically, typed 404 candidates are skipped without hiding auth/transient failures, and S1/S2 are covered by production-path deterministic conformance. Public TLC footer discovery succeeded; its GET endpoint independently returned 403 during the live session. Remaining implementation is E5's channel-runtime handoff and final S3/cloud parity.
- 2026-07-10: S3 and S8 cloud parity are now covered: the in-memory object-store fixture recursively resolves compressed NDJSON, performs bounded discovery and pinning, previews through the shared engine, executes 10,000 rows, and preserves remote FileManifest identity. E5 remains open only for the P3 channel-runtime elimination of whole-input/materialized-batch residency.
- 2026-07-10: Workstream closed for P2. Local/HTTP/S3/GCS/Azure share the transport facade, credentials/egress/doctor behavior is typed, ranged discovery and recursive listing compose with manifests and compression, and S1-S3/S8 are conformance-owned. E5's remaining channel-level residency optimization was transferred to the existing P3 performance-triage owners rather than creating a second decoder/runtime seam.

## Blockers

Cloud live tests may be credential-gated; deterministic fixtures must still cover ordinary CI.
