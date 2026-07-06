Status: done
Created: 2026-07-05
Updated: 2026-07-05

# Book ingestion for initial implementation graph

## Question

What authoritative behavior, constraints, and execution boundaries must be preserved from `firn-the-book-of-the-system.md` before implementation begins?

## Sources and methods

- Inspected repository contents on 2026-07-05: repository contained `.git` and untracked `firn-the-book-of-the-system.md`.
- Read `firn-the-book-of-the-system.md` fully by line chunks and extracted its heading outline. The file is 1,093 lines.
- Confirmed there was no existing `.10x/` directory and therefore no conflicting active records.

## Findings

- The book defines firn as a Rust-native, DataFusion-powered, contract-governed data movement kernel with strict layer direction, Arrow-native batches, package evidence, receipt-gated checkpoints, and CLI/project surfaces.
- The book contains an explicit decision register D-1 through D-28 with revisit triggers. The register is copied into `.10x/decisions/firn-book-decision-register.md`.
- The book's MVP is a large but bounded milestone: kernel, engine, contracts, package builder/replayer, SQLite ledger, authoring tiers 0/1/2/4, HTTP toolkit, DuckDB/Parquet/Postgres destinations, selected sources, append/replace/merge dispositions, the CLI except package archive, conformance suites, chaos layer, golden packages, and dlt shim preview.
- The book's "cutline" items are not out of overall scope for the user's goal; they are sequenced after MVP or fast-follow: WASM, Singer/Airbyte, log CDC, streaming supervisor, distributed execution, warehouse/lakehouse destinations, signing, non-SQLite ledgers, vault-class secrets, and UI exclusion.
- The book's examples are normative for shape and semantics, but implementation identifiers may vary when specs and tests preserve behavior.

## Conclusions

The safe next action is to create active focused specifications and a parent/child ticket graph. Implementation should not begin until those records exist because the requested system is net-new, multi-surface, persisted, user-facing, and explicitly intended to outlive the book.

