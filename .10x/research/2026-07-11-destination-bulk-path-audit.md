Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Destination bulk-path audit

## Question

How should CDF make every destination Arrow-bulk by default while keeping physical strategy, type fallback, staging, concurrency, and tuning inside the driver rather than proliferating in generic runtime code?

## Sources and methods

Inspected kernel commit segment/session types, DuckDB/Postgres/Parquet sheets and commit paths, package readers, project replay/preparation, destination extension/staged-ingress contracts, Cargo features, and P3 envelope/graph tickets.

## Findings

`CommitSegment` owns `Vec<RecordBatch>`, so the kernel session boundary itself permits segment-sized collection and cannot yield one accounted batch at a time. Package readers collect all commit segments before replay. This is already owned by A5 but is the substrate every bulk driver currently consumes.

DuckDB advertises an Arrow-IPC package-row bulk path but converts every batch into `Vec<RowValues>` and calls the scalar appender row by row. It also retains staged `CommitSegment`s until all expected segments arrive. The workspace does not enable/use an Arrow-native append path in this code.

Postgres retains all segments, converts every Arrow cell to `Option<String>`, serializes CSV lines, and uses text `COPY` into a staging table. The sheet lists `copy_binary` before `copy_csv`, but the implementation is CSV. This is a declaration/implementation truthfulness gap.

Parquet retains all segments/batches until finalize, then constructs package data and writes destination output. It cannot overlap row-group encoding/persistence or multipart upload with upstream durable segments, and memory grows with the package.

Bulk path identifiers and capability lists are destination-specific structs/strings. There is no shared descriptor for accepted Arrow types, semantic mapping preconditions, batch/byte/concurrency ranges, staging/finalization requirements, fallback, executor lane, or measured evidence. A generic scheduler therefore cannot join destination pressure without learning each driver.

Physical path selection is tuning, not package data semantics. If a host chooses Arrow append versus vtab or binary versus text COPY under the same exact mapping/transaction guarantees, package identity should not change. The actual path/version/settings still need run/receipt/performance evidence. A path cannot fail after partial mutation and silently switch; fallback must be schema-planned or restart from an aborted attempt.

## Conclusion

Add a neutral destination `BulkPathDescriptor`/prepared writer contract. Drivers enumerate paths, validate the already-compiled semantic type mapping, and prepare a deterministic eligible/fallback ladder. The runtime streams bounded durable segment batches and joins generic concurrency/memory/lane declarations without matching destination names.

Physical tuning/path choice stays outside package identity but is recorded in run/receipt evidence. Destination drivers own vectorized encoding, transactions/staging, and exact fallback. DuckDB, Postgres, and Parquet receive focused implementations and shared conformance.

## Limits

The audit does not select DuckDB Arrow appender versus vtab, Postgres binary encoder implementation, Parquet output file/row-group sizes, or transaction/staging thresholds. WS-L and destination children must measure those choices.
