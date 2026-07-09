Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
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

## Blockers

Cloud live tests may be credential-gated; deterministic fixtures must still cover ordinary CI.
