Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/native-enterprise-format-catalog-v1.md, .10x/specs/native-enterprise-format-catalog.md, .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md

# Native enterprise format catalog shaping evidence

## What was observed

The user explicitly broadened the active performance mandate to enterprise terabyte-scale operation across all file inputs/formats CDF may encounter and required native handling without leaky abstractions. Current source supports only CSV, JSON, NDJSON, Parquet, Arrow IPC, gzip, and zstd through closed enums/eager paths.

## Procedure

Classified common enterprise data encodings into byte transforms, archive containers, record/columnar codecs, and source/table protocols; reconciled that classification with VISION's Arrow boundary, P2 file partitions, the new format-driver contract, and P3 performance/constant-memory laws.

## What this supports

A closeable catalog-v1 spanning columnar, delimited/fixed text, JSON/XML, Avro/ORC, schema-bound/self-describing binary, spreadsheets, compression/character transforms, and ZIP/TAR, with explicit exclusions for table/database/document-interpretation protocols.

## Limits

This record proves scope/authority, not parser viability or throughput. Every codec ticket still requires dependency, correctness, security, and reference-decoder evidence.
