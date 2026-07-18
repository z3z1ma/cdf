Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/done/2026-07-11-p3-b2-parquet-codec.md, .10x/tickets/done/2026-07-11-p3-b3-arrow-ipc-codecs.md, .10x/tickets/2026-07-11-p3-b4-delimited-fixed-width-codecs.md, .10x/tickets/done/2026-07-11-p3-b5-json-codecs.md, .10x/tickets/2026-07-11-p3-b6-avro-codecs.md, .10x/tickets/2026-07-11-p3-b7-orc-codec.md, .10x/tickets/2026-07-11-p3-b8-xml-codec.md, .10x/tickets/2026-07-11-p3-b9-spreadsheet-codecs.md, .10x/tickets/2026-07-11-p3-b10-protobuf-codec.md, .10x/tickets/2026-07-11-p3-b11-messagepack-cbor-codecs.md, .10x/tickets/2026-07-11-p3-b12-archive-containers.md

# P3 B13: native format catalog conformance and docs matrix

## Scope

Execute the complete catalog-v1 cross-product over detection/discovery/preview/run, local/HTTP/object stores, applicable transforms/archives, schema policies, malformed/quarantine behavior, jobs, memory, and reference performance; publish capability/options docs from registry data.

## Acceptance criteria

- Every catalog entry/transform/container has a green or explicitly unsupported capability cell justified by the spec; no missing cell is omitted.
- Generated CLI/project schema/docs and `cdf inspect formats` derive from registry data.
- Mock external codec law remains green after the full catalog lands.
- Codec build domains and ordinary fast checks remain lean.
- WS-B envelope/reference rows, jobs invariance, constant-memory cases, fuzz corpora, and preview/run parity are permanent gates.

## Evidence expectations

Generated matrix/docs, full reports, package goldens, architecture/build graph checks, slow-tier stress, fuzz inventories, and adversarial “embarrassing format” review.

## Explicit exclusions

No table/database/document-interpretation protocols outside catalog v1.

## Blockers

Depends on B1-B12.

## References

- `.10x/decisions/native-enterprise-format-catalog-v1.md`
- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
