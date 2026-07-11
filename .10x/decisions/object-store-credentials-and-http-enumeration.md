Status: active
Created: 2026-07-10
Updated: 2026-07-10

# Object-store credentials and HTTP enumeration

## Context

P2 WS-E requires one file-source facade across local files, HTTP(S), S3, GCS, and Azure while preserving secret redaction, egress policy, ranged reads, and multi-file discovery. Object stores can list prefixes; generic HTTP servers cannot.

## Decision

- `s3://`, `gs://`, and `az://` resources MUST execute through the shared `FileTransport` abstraction backed by `object_store`; format readers and schema discovery MUST NOT contain provider-specific branches.
- `credentials = "secret://..."` resolves to a JSON object of provider options accepted by the pinned `object_store` builder. When absent, the provider's ambient credential chain applies. Secret values MUST never enter plans, errors, debug output, snapshots, or packages.
- Egress authorization MUST run before provider construction or network I/O. Object-store URLs are mapped to their provider HTTPS authority for that check.
- HTTP multi-file resources MUST use finite, deterministic template expansion. The supported initial grammar is one inclusive numeric range `{start..end}` with width preservation (for example `{01..12}`). A single resource expansion is bounded at 1,000,000 objects so accidental ranges fail cleanly rather than exhausting plan memory. Unbounded `*`, `?`, and `**` over HTTP remain contract errors because HTTP has no portable LIST operation.
- Discovery over a multi-file resource MUST reconcile the selected files into one pinned snapshot and record the complete discovered file set and identities. Sampling is allowed only when explicitly recorded by the discovery policy; it never masquerades as exhaustive discovery.

## Alternatives considered

- Provider-specific source implementations were rejected because they duplicate schema, partition, and evidence semantics and create the leaky architecture prohibited by the program.
- Embedding credential fields in TOML was rejected because it bypasses the secret provider chain.
- Guessing HTTP wildcard matches by probing open-ended names was rejected because it is non-terminating, non-deterministic, and cannot prove completeness.

## Consequences

Adding a transport requires implementing the transport facade, not modifying each format reader. HTTP users must express a finite range when the server has no index or listing API. Provider option names remain an adapter concern and can evolve behind the secret reference without changing resource declarations.
