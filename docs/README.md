# CDF docs

This tree is the in-repository documentation surface for CDF operators and
contributors.

- [Quickstart](quickstart.md) loads public NYC TLC Parquet into DuckDB, expands
  to an incremental monthly manifest, explains governed drift, and replays a
  verified package without source contact.
- [Architecture](architecture.md) gives the short map and links to the book and
  active specs for the full contract.
- [Iceberg source](iceberg.md) documents catalog bindings, snapshot selectors,
  append-only incrementality, and the explicit capability boundary.
- [SQLite table source](sqlite.md) documents portable local configuration, discovery, snapshot
  isolation, stable cursors, explicit temporal encodings, failure ownership, and the measured
  direct-library roofline.
- [Performance envelope](performance-envelope.md) is generated from the P3
  host-labelled reconciliation manifest and exact destination-path report. It
  is the only authority for performance claims and keeps partial, accepted,
  and unproven targets visibly non-green.
- [Pre-optimization performance baseline](performance-baseline.md) preserves
  the immutable before picture used by the P3 program.
- [Foreign source boundaries](interop-boundaries.md) documents Python and
  subprocess transfer, copy, memory, and host-labelled performance evidence.
- [Operators](operators/README.md) contains scoped operational guides for
  recovery, replay, backfill, doctor/status, installation, and troubleshooting.
- [Commands](commands/README.md) is reserved for generated command reference
  output owned by
  [WS6B](../.10x/tickets/done/2026-07-08-p1-product-ws6b-generated-reference-freshness.md).
- [Errors](errors/README.md) is reserved for the generated error catalog owned
  by
  [WS6B](../.10x/tickets/done/2026-07-08-p1-product-ws6b-generated-reference-freshness.md)
  after WS4 defines the catalog.

The source of truth for the product shape remains [VISION.md](../VISION.md) and
the active specs under [`.10x/specs/`](../.10x/specs/). These docs should explain
current usage without getting ahead of parser, renderer, runtime, or release
behavior.
