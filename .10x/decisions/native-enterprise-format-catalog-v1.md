Status: active
Created: 2026-07-25
Updated: 2026-07-25

# Make the native enterprise format catalog demand-activated

## Context

The superseded decision at
`.10x/decisions/superseded/native-enterprise-format-catalog-v1.md` correctly classified the
enterprise format families and established native codec laws, but it made every catalog entry a
P3 closure requirement. That converted the finite performance-architecture program into an
indefinite connector catalog. The generic format-driver and byte-source boundaries now exist, and
the high-throughput Parquet, Arrow IPC, delimited, JSON/NDJSON, and Protobuf paths have terminal
evidence. The user ratified removing unprioritized enterprise codecs from P3 closure while
preserving native breadth as product direction.

## Decision

The catalog and `.10x/specs/native-enterprise-format-catalog.md` remain the classification and
semantic authority for native enterprise formats. Catalog membership does not make a codec
immediately executable work.

Parquet, Arrow IPC file/stream, supported delimited text, JSON/NDJSON, and Protobuf constitute the
terminal P3 core-format set. Avro, ORC, XML, spreadsheets, MessagePack/CBOR, additional byte
transforms, and archive containers are demand-activated future capabilities indexed by
`.10x/knowledge/active-backlog-and-future-roadmap.md`.

A future codec activates only from a current product need, current dependency/supply-chain
evidence, and one focused ticket behind the existing registry/byte-source contract. No codec may
extend generic compiler/runtime match trees or weaken streaming, memory, discovery, provenance,
preview/run-parity, malformed-input, or performance laws.

## Alternatives Considered

- Keep every catalog entry in P3. Rejected because P3's performance architecture can close without
  implementing an unbounded connector catalog.
- Delete the catalog. Rejected because its type/framing/security classification remains valuable
  authority for future implementations.
- Add placeholder drivers. Rejected because unsupported stubs would increase product surface and
  build cost without ingesting data.

## Consequences

P3 WS-B may close over its implemented core engines and extension boundary. Future format work is
smaller and evidence-driven, while the semantic bar remains unchanged. A pending coverage row may
point to the roadmap rather than an open reminder ticket. Reactivation must revalidate temporal
dependency constraints, especially Avro's dependency-owned buffering and ORC's resource authority.
