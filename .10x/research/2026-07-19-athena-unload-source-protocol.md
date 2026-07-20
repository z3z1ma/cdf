Status: active
Created: 2026-07-19
Updated: 2026-07-19

# Athena UNLOAD source protocol

## Question

What exact control-plane, schema, identity, cleanup, and execution architecture lets CDF expose AWS Athena as a first-class source while reusing the existing object-access, Parquet, scheduler, memory, package, and evidence data plane at roofline performance?

## Sources and Methods

- Inspected CDF's registered source boundary, compiled source plan, prepared-payload reuse, external task store, portable worker protocol, source positions, Iceberg/Glue adapter, and neutral object-access/Parquet path at revision `dcd97964`.
- Read the current AWS Athena API and user-guide contracts for [UNLOAD](https://docs.aws.amazon.com/athena/latest/ug/unload.html), [StartQueryExecution](https://docs.aws.amazon.com/athena/latest/APIReference/API_StartQueryExecution.html), [GetQueryExecution](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryExecution.html), [GetQueryResults](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryResults.html), [GetQueryRuntimeStatistics](https://docs.aws.amazon.com/athena/latest/APIReference/API_GetQueryRuntimeStatistics.html), [StopQueryExecution](https://docs.aws.amazon.com/athena/latest/APIReference/API_StopQueryExecution.html), [query result files](https://docs.aws.amazon.com/athena/latest/ug/querying-finding-output-files.html), [result reuse](https://docs.aws.amazon.com/athena/latest/ug/reusing-query-results.html), [compression](https://docs.aws.amazon.com/athena/latest/ug/compression-formats.html), [ZSTD levels](https://docs.aws.amazon.com/athena/latest/ug/compression-support-zstd-levels.html), and [service quotas](https://docs.aws.amazon.com/general/latest/gr/athena.html).
- Used the authenticated FQ12 `PowerUser-617739438897` profile for read-only `ListWorkGroups`, `GetWorkGroup`, query-history, query-status, and prior-result-manifest inspection. No query was submitted and no S3 object or AWS configuration was created, changed, or deleted.

## Findings

### Service contract

- `UNLOAD` is a DML statement that writes a `SELECT` result in parallel. It supports Parquet, ORC, Avro, JSON, and text output. Parquet supports GZIP, Snappy, and ZSTD writes; Athena engine v3 supports ZSTD levels 1 through 22. Athena documents Parquet GZIP as the `UNLOAD` default, but that default is not accepted as CDF's performance default without an end-to-end compression sweep.
- Non-partitioned output prefixes must be empty and are never overwritten. Partitioned `UNLOAD` may append into an existing location and is limited to 100 partitions. CDF should not use Athena output partitioning for its execution grain: the service manifest already exposes natural files, and unique non-partitioned attempt prefixes have safer retry/cleanup semantics.
- Result files have no documented global order. An `ORDER BY` orders rows within files but not files relative to one another. CDF must therefore derive canonical child partition order from frozen object identity, never service response order or filename folklore.
- Athena writes a metadata file and data manifest into the Athena query-results location. `QueryExecution.Statistics.DataManifestLocation` names the manifest, and `GetQueryResults(QueryResultType=DATA_MANIFEST)` streams its file locations with pagination. The manifest is the authority for files written; prefix listing is recovery/cleanup evidence only.
- Athena does not delete orphaned output after failure. Failed and cancelled queries can also leave incomplete multipart uploads. Cleanup is a CDF-owned staging responsibility whenever CDF chose the unique output prefix.
- `ResultConfiguration.ExpectedBucketOwner` protects only the Athena query-results location. AWS explicitly says it does not protect the `UNLOAD TO` destination. CDF therefore needs its own pre-submit destination bucket-owner/region validation and exact-prefix ownership policy.
- `StartQueryExecution.ClientRequestToken` is the retry idempotency key. Reusing the same token with identical parameters returns the same query execution; changing any parameter is rejected. CDF must derive and record a 32-to-128-character token before submission so a crash after service acceptance can recover the same execution instead of duplicating it.
- Query states are `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, and `CANCELLED`; a transient service retry can move `RUNNING` back to `QUEUED`. Terminal failures expose category, type, message, and provider `Retryable` authority. Cancellation requires `StopQueryExecution` followed by polling to terminal state.
- Query result reuse supports only `SELECT` and `EXECUTE`, not `UNLOAD`. It also deliberately permits staleness and does not verify reused result integrity. It cannot serve as CDF's no-op or replay mechanism.
- `GetQueryExecution` supplies the evidence needed for a source receipt: effective engine, workgroup, catalog/database, statement kind, data scanned, queue/planning/engine/service/total timings, manifest location, and provider error classification. `GetQueryRuntimeStatistics` can add rows and stage metrics but AWS documents those fields as asynchronously populated and sometimes absent, especially with Lake Formation filters; they are telemetry, not identity authority.

### Live read-only FQ12 observation

- FQ12 has several enabled workgroups. The two metadata workgroups enforce their configured result locations and run Athena engine v3. `primary`, `test-query-api`, and `testing-secondary` do not enforce a result location; the latter two carry very large scan cutoffs and therefore do not provide a useful benchmark safety rail. No workgroup is selected by this research.
- Existing query history contained one successful engine-v3 `UNLOAD`. Without reading or recording its SQL or locations, the API reported 305,031 output rows, 3,216,216 bytes scanned, 2,597 ms total, 2,435 ms engine, and 273 ms planning. `DATA_MANIFEST` returned one row containing one S3 URI and no column metadata. The referenced result object had already been removed while the Athena query/manifest record remained. This proves that replay must revalidate every recorded object generation and cannot treat a historical query execution or manifest as durable payload retention.

### Schema and discovery lifecycle

- A final CDF plan still requires a fixed output schema before `UNLOAD` begins. The Athena `UNLOAD` result API does not expose the selected columns' schema; `DATA_ROWS` for an `UNLOAD` exposes only an update-count shape. Therefore cold discovery needs a separate bounded schema query, recommended as a recorded `SELECT * FROM (<compiled user SELECT>) AS __cdf_schema LIMIT 0`, whose `GetQueryResults.ColumnInfo` is converted to Arrow and pinned before the final plan.
- This is bounded plan-time discovery, not a duplicate extraction. A pinned run skips it. Physical Parquet footer observations during execution are ordinary schema admission against the fixed pin and never mutate the epoch.
- Athena type strings require a complete parser and conformance matrix for integers, real/double, decimal precision/scale, char/varchar, binary, date/time/timestamp variants, arrays, maps, and rows. Nullability is frequently `UNKNOWN` and must compile conservatively nullable. Pseudo-types and output combinations that Athena cannot write to Parquet must fail during discovery/planning with the exact field/type.
- The live spike must verify that the zero-row wrapper scans zero payload bytes, preserves names and nested/decimal/timestamp types exactly, and agrees with the actual emitted Parquet footer. Until then, wrapper support and type fidelity are retained hypotheses rather than accepted semantics.

### Required CDF execution seam

- Running `UNLOAD` from `SourceDriver::resolve`, synchronous `QueryableResource::negotiate`, or one giant source partition would be architectural debt. It would make inspection resolution side-effectful, conceal child files from the scheduler, destroy per-file retry/portable-worker scaling, and create an Athena-owned parallel runtime.
- The needed source-neutral abstraction is runtime partition materialization. The final plan records a fixed schema plus a compiled, deterministic materializer program. At run time and before payload decode, generic orchestration executes that program through the registered source, freezes its result receipt and canonical external task-set artifact, and streams the resulting ordinary partitions into the existing scheduler. Query services, snapshot enumerators, and future dynamic bounded sources can use the seam; generic orchestration never branches on Athena.
- For Athena, the coordinator control task performs `StartQueryExecution`/poll/cancel, reads the paginated data manifest, resolves each S3 object through neutral object access, canonically spill-sorts object identities, and writes the ordinary external partition task set. Isolated workers receive only the compiled source binding plus canonical Parquet object tasks. They need no Athena credentials or API client when object credentials are independently resolvable.
- The materialization receipt and task-set hash are identity-bearing runtime evidence bound to the compiled plan and package. The static plan survives unchanged; exact result files are observations produced by executing its total program, analogous to pages or bounded dynamic partitions rather than a re-planned source.
- Empty results produce a successful materialization receipt with update count zero and no child tasks. They are a visible fast no-op, not an invalid empty `FileManifest` and not a fabricated package.

### Identity, replay, and incrementality

- The recommended typed position is a source-neutral materialized-query result position, not an Athena blob hidden in `ForeignState`. It should bind protocol, endpoint/region, catalog/database/workgroup, compiled query hash, parameter-binding hash/reference authority, query execution id, effective engine/workgroup configuration hash, service manifest identity, canonical result task-set hash, and row/update count. Each child additionally carries its ordinary file generation position.
- Package replay consumes canonical package segments and never resubmits SQL. A source execution retry first recovers the same query through its recorded client token. A genuinely new provider execution requires a new deterministic attempt prefix/token and only after the previous failed attempt's exact output set is reconciled.
- Arbitrary fixed queries do not possess incremental semantics. Re-running them under `append` can duplicate the entire result. A safe product contract must either require `replace`, or require an explicit checkpoint/cursor parameter binding that makes each append window disjoint. Inferring query incrementality from SQL text is not credible.

### Performance and format direction

- Athena's value is not that it beats direct Iceberg on an identity scan. Direct Iceberg now executes the 3.51-million-row FQ12 projection in about 2.22 seconds against a 2.01-second isolated S3 transfer roofline. Athena adds queue, distributed planning, execution, and result materialization. It should be measured on both an identity projection and a pushdown-heavy join/filter/aggregation where managed distributed execution avoids transferring irrelevant source data.
- CDF's timed region must separate provider queue/planning/engine/publish, manifest/object preparation, S3 transfer, Parquet decode, validation/package, and destination commit. `DataScannedInBytes` is billed input, not transport bytes; output object bytes and useful decoded bytes remain separate.
- Snappy and ZSTD level 1 are the first retained compression candidates. The default must come from end-to-end wall time and network/CPU rooflines, not compressed size alone. CDF already reads both natively through the same Parquet driver.

## Conclusions

1. Retain Athena as a first-class source. It is not a Trino source subtype: AWS authentication, workgroups, idempotency, manifests, billing, cancellation, retention, and cleanup are protocol-specific. Athena and a later Trino source should share only the compiled-query and runtime partition-materialization currencies.
2. Reuse CDF's existing object-access, Parquet, external task-store, scheduler, portable-worker, memory-ledger, package, receipt, and destination pipeline unchanged. The only new data-plane capability is the source-neutral runtime partition-materialization seam.
3. Factor the now-repeated AWS credential resolution and SigV4 signing into a minimal shared AWS protocol crate. Glue and Athena retain their own JSON bodies, targets, response/error models, and semantics; the shared crate must not create an SDK runtime, retry loop, HTTP pool, or credential chain.
4. Do not submit a live FQ12 query until the exact workgroup, CDF-owned output root, scan budget, retention, and cleanup policy are explicitly confirmed. Read-only inspection does not ratify those billing and write/delete side effects.

## Limits

- No new query was submitted, so zero-row schema discovery, actual Athena Parquet physical types, empty-result manifests, Snappy/ZSTD output performance, cancellation, and orphan cleanup remain to be falsified live.
- AWS does not document a stable global file order or durable output retention. The design intentionally depends only on the service manifest plus immediate object-generation observation.
- The proposed generic runtime materialization seam is an architectural conclusion from current CDF boundaries and the Athena protocol. Its exact API belongs in a focused active spec before implementation.
