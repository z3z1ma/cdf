Status: active
Created: 2026-08-02
Updated: 2026-08-02
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md

# ClickHouse source connector

## Scope

Implement and ship `cdf-source-clickhouse` on the official Rust client plus the Arrow 58
`clickhouse-ext-arrow` path. Add built-in enrollment, data-driven and live type/cursor fixtures,
operator documentation, and a release-mode direct-ArrowStream source roofline cell.

## Non-goals

Destination writes, CDC, arbitrary SQL, a private native/TCP protocol, string-inferred temporal
cursors, or cross-query snapshot claims.

## Acceptance Criteria

- Driver configuration, discovery, schema mapping, compile/portability, health, query generation,
  pushdown fidelity, cursor ordering, partial-stream failure, and execution implement
  `.10x/specs/clickhouse-table-source.md`.
- Supported ClickHouse types round-trip through Arrow 58; unsupported types fail during discovery
  with field/type remediation and no stringification.
- The source streams the official extension's opt-in bounded Arrow query batches through injected
  async host services with bounded buffering, cancellation, egress, and no private
  runtime/pool/retry authority.
- Built-in catalog integrity, generic source matrix, jobs invariance, package/replay/checkpoint
  laws, and `tools/certify-connector.py --kind source --id clickhouse --core-impact` pass against a
  digest-pinned security-supported ClickHouse image.
- The source macro benchmark reaches the 0.90 direct official ArrowStream roofline and records the
  server/client/compression/`max_threads` settings.
- Independent review passes after closure repair.

## References

- `.10x/specs/clickhouse-table-source.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/research/2026-08-02-sqlite-clickhouse-mongodb-connector-shaping.md`
- `docs/connector-authoring.md`

## Assumptions

- Finite snapshot/cursor behavior, current security-supported ClickHouse, official Arrow path, and
  the 90% roofline are user-ratified or record-backed.
- CDF Arrow 58.3.0 matches the official extension's current Arrow 58 matrix.

## Journal

- 2026-08-02: Ticket opened; execution waits for complete SQLite tranche closure.
- 2026-08-02: SQLite source and destination implementations have focused acceptance evidence and
  independent repair-review passes. Their fresh roofline/certificate gates are parent-owned final
  integration work under the user-ratified reduced validation cadence, so ClickHouse source is
  unblocked from clean pushed commit `57a412e6`.
- 2026-08-02: Execution started from `57a412e6c95616c2707cf80ca9b95e217f74b1e1` with only this
  ticket's parent-authored dependency/status handoff dirty. Read the complete ticket, every direct
  reference, the ClickHouse spec's resource/checkpoint authorities, concrete database-kind and
  driver-concurrency decisions, error-ownership taxonomy and audit skill, and the runtime-memory,
  remote-I/O, current-only, product-closure, and performance authorities reached by those records.
  The executable boundary is unambiguous: a dependency-isolated table-only finite source using
  the official ArrowStream path; contact-free compilation/portability; bounded discovery; one
  query stream; exact typed projection/filter/cursor semantics; injected async/memory/cancellation/
  egress/retry authority; catalog-only enrollment; and no destination, CDC, arbitrary-SQL, private
  protocol/runtime/pool, or cloud-write work.
- 2026-08-02: Authored the dependency-isolated `cdf-source-clickhouse` leaf around exact
  `clickhouse = 0.15.1` plus `clickhouse-ext-arrow = 0.1.0`: strict contact-free driver plans,
  secret-reference credentials, bounded Arrow catalog discovery, recursive source/Arrow type
  validation, exact-only typed pushdown, canonical cursor/stable-key ordering, one host-owned
  bounded asynchronous Arrow query stream, pre-admission memory authority, cancellation/egress,
  and provenance-preserving redacted client error classification. The shared cursor position
  carries only the cursor value, not the stable-key tie-breaker; therefore an equal-cursor group
  cannot express a mid-group durable frontier. This is a bounded residual of the accepted shared
  source contract, not grounds for a connector-local checkpoint representation or workaround.
- 2026-08-02: The approved focused first compile command
  `CARGO_BUILD_JOBS=12 cargo check -p cdf-source-clickhouse --tests -j 12` stopped before Rust
  compilation with exit 101 because the sandbox could not resolve `index.crates.io`; Cargo's final
  error was failure to fetch registry `config.json` while resolving `arrow-arith`. No broader,
  live, or escalated validation command was run. Exact `clickhouse 0.15.1` client API confirmation
  and the connector's compile result remain unsupported until focused dependency resolution is
  authorized and succeeds.
- 2026-08-02: After focused escalated dependency resolution populated the exact client graph, the
  compiler exposed only current-contract naming/value-shape mismatches: the test cursor authority,
  request order type, string-valued ClickHouse settings, and one unused import. Repaired only
  those narrow items. Reran `CARGO_BUILD_JOBS=12 cargo check -p cdf-source-clickhouse --tests -j
  12` from cache; it completed successfully in 1.24 seconds with no warnings. This proves the new
  leaf and its tests typecheck against exact `clickhouse 0.15.1`, `clickhouse-ext-arrow 0.1.0`,
  Arrow 58, and current CDF source/runtime contracts; it does not prove unit behavior, live server
  behavior, enrollment, conformance, or performance.
- 2026-08-02: Live ClickHouse 25.8.28.1 evidence showed raw ArrowStream UUID conversion fails with
  code 50 `UNKNOWN_TYPE`. The user superseded the earlier unsupported-UUID interpretation and
  ratified lossless canonical text instead. The source now pins physical `UUID` as CDF `Utf8`,
  generates `toString()` consistently for schema probe/projection/filter/order, validates the
  lowercase hyphenated text during Binary-to-Utf8 Arrow adaptation, and leaves ordinary
  ClickHouse `String` as arbitrary-byte Binary. Dynamic/variant/aggregate/geo types remain early
  field/type discovery failures.
- 2026-08-02: The generic matrix fixture now exercises that ratified UUID path directly: it seeds
  deterministic canonical UUID values, discovers rather than redeclares the schema so physical
  metadata survives, and uses signed `Int64` row identifiers to match the shared matrix row
  authority. Live discovery exposed and repaired two connector execution-law defects without
  weakening validation: partition observation binding now uses the exact discovery identity
  `database.table`, and execution resolves/projects the cataloged physical observation schema,
  compares its hash to the planned physical authority, validates raw batches against that schema,
  and records it as materialization evidence while emitting the separate effective schema. A
  focused unit law covers source-name remapping plus divergent effective/physical field and schema
  metadata across the UUID normalization boundary.
- 2026-08-02: The approved generic ClickHouse source-matrix command passed all twelve executed
  cells across DuckDB, Parquet, and PostgreSQL append/replace/merge capability combinations, with
  three capability exclusions, in 16.99 seconds. This proves the registered generic payload,
  replay-identity, checkpoint, persistence, and applicable disposition laws for the digest-pinned
  live fixture; it does not prove excluded destination capabilities, arbitrary schemas, or
  performance.
- 2026-08-02: The first release roofline attempt without `DUCKDB_DOWNLOAD_LIB=1` stopped at the
  known macOS DuckDB linker prerequisite and never reached the benchmark binary. With that flag,
  the approved command built and entered the binary but failed immediately at Tokio `TcpStream`
  construction with `there is no reactor running`; it produced no timing sample or report. The
  harness had incorrectly driven direct ClickHouse seed/query futures through the standalone
  root blocker instead of the injected I/O execution reactor. Repaired only the benchmark harness:
  seeding and direct reads now use the same host-owned `ExecutionServices` reactor already injected
  into the CDF source, while standalone root composition remains the CDF consumer boundary. Both
  timers start after runtime construction and schema/query-plan preparation but before source
  stream opening; seeding and the equivalence warm-up remain untimed. A non-live unit law asserts
  that direct benchmark futures enter the injected Tokio reactor.
- 2026-08-02: The single approved five-sample release rerun passed at ratio 1.038211 against the
  direct official ArrowStream baseline. Across 1,000,000 rows per sample, the CDF median was
  35,456,167 ns and the direct median was 36,811,000 ns. The report records exact
  `clickhouse 0.15.1`, `clickhouse-ext-arrow 0.1.0`, digest-pinned ClickHouse server image,
  default official-client LZ4 compression, `max_threads=4`, `max_block_rows=65,536`, and one
  buffered batch at
  `.10x/evidence/.storage/2026-08-02-clickhouse-source-roofline.json`. After evidence capture, the
  named digest-pinned test container was stopped and removed.
- 2026-08-02: Refroze the error-ownership inventory after all physical/effective-schema repairs.
  The eleven-file scope contains 118 `CdfError` constructors: 104 production sites (59 Contract,
  37 Data, five Internal, two dynamic classifiers, one Auth) and fourteen test fixtures. Twelve
  production direct-kind branches and six test assertions bring the classified TSV to 136 rows.
  Physical observation/catalog/hash failures remain source Data, while compiled effective-schema
  projection contradictions remain caller Contract; the Internal inventory did not grow.
- 2026-08-02: Executor handback. Implementation, focused/live/matrix/performance evidence,
  operator documentation, benchmark report, and frozen error ledger are recorded. No active
  execution blocker remains. Independent red-team review and parent-owned certification/closure
  judgment remain outside this handback.
- 2026-08-02: Closure-repair execution began after rereading this ticket, every referenced active
  authority, the complete error-ownership taxonomy, and the independent review. Source inspection
  confirmed the review's checkpoint-eligibility premise: the engine retains the original
  `ScanRequest.limit`, marks a truncated partition incomplete, and the project commit path rejects
  checkpoint advancement when execution evidence is ineligible. The connector can therefore keep
  cursor limits out of ClickHouse SQL while leaving the generic engine limit in force and failing
  the partial project run closed. Inspection also confirmed that declared-schema execution
  currently synthesizes physical evidence, bare `DateTime` is omitted, nanosecond cursor values are
  truncated, and raw nested `std::io::Error` ownership is not classified recursively.
- 2026-08-02: Investigated the official ClickHouse/Arrow allocation boundary before changing the
  memory claim. `clickhouse-ext-arrow` calls the official client's public `BytesCursor`, then feeds
  arbitrary HTTP chunks into Arrow `StreamDecoder`; `ArrowCursor` exposes neither the IPC body
  length nor received-byte counters before `next()` decodes. Official ClickHouse documentation
  states that `max_result_bytes` is checked only after a block and may be exceeded by the final
  block, so that setting alone cannot substantiate a pre-decode hard ceiling. The repair must bound
  the official byte cursor and IPC message before decoder admission (or fail closed/downgrade the
  capability); post-decode retained-byte rejection is not accepted as proof.
- 2026-08-02: Closure repairs now keep cursor limits out of ClickHouse SQL while retaining the
  original engine limit, require catalog-backed physical evidence before resource resolution,
  reject sub-microsecond cursor schemas, recursively preserve typed and raw host-I/O ownership,
  reuse the official client pool, and bound the no-compression ArrowStream path with a 4,096-node
  schema ceiling, a server-enforced 16 MiB variable-row ceiling, and a schema-derived 32 MiB Arrow
  body row bound before the existing 64 MiB retained-batch admission. ClickHouse 25.8 exposes
  narrow `Date`/`DateTime` as UInt16/UInt32 in ArrowStream; validated source expressions now promote
  only those two cases to Arrow `Date32`/second `Timestamp`, retaining physical metadata and
  timezone. Native `Date32`/`DateTime64` remain untouched.
- 2026-08-02: The first post-repair leaf run compiled 15 tests and failed two stale expectations;
  after fixing the pre-secret physical-evidence check and generated UUID row-bound SQL assertion,
  the focused non-live leaf passed 14 tests with the digest-pinned law ignored. Benchmark and
  conformance test-target checks also passed. The first live attempt inside the managed sandbox
  failed at connection construction because localhost networking was blocked; this was a harness
  permission limit, not connector evidence.
- 2026-08-02: The exact approved live law then falsified three successive test-harness assumptions
  without weakening production checks: direct leaf open omitted the engine-planned physical hash;
  generic Arrow display renders temporal storage as epoch counts; and the pinned server's narrow
  `Date` storage is not Arrow `Date32`. The helper now binds the exact admitted physical hash when
  deliberately bypassing engine planning, temporal assertions inspect exact typed arrays/values,
  and production query generation removes the two narrow temporal storage leaks. The fourth exact
  rerun passed one live law, zero failed, fourteen filtered in 0.24 seconds. It proves catalog and
  execution schema identity, representative values across the full supported type matrix,
  canonical UUID text, equal-cursor ordering/frontier, deterministic post-batch failure, and
  predecode wide-row rejection against the digest-pinned local server; it does not generalize to
  other server versions or remote networks.
- 2026-08-02: The first repaired generic source-matrix run passed all twelve applicable registered
  ClickHouse cells with three capability exclusions in 16.23 seconds, then its new project-level
  partial-stream law failed during fixture CREATE. The invalid fixture had combined UInt8
  `throwIf` and UUID branches inside one UUID alias. The repair uses a constant UUID alias and a
  separate UInt8 delayed-fault alias, preserves the real emit-then-error path, and maps fixture
  query rejection to source Data with only a stable numeric server code; foreign server text and
  generated SQL remain redacted. A focused conformance test-target check passed after the repair.
- 2026-08-02: The single generic ClickHouse source-matrix rerun passed one scheduled shard test:
  all twelve applicable registered cells passed, three capability exclusions remained explicit,
  and the added equal-cursor limit and real post-batch server-error project/package laws both left
  destination publication and durable checkpoint state unchanged. Runtime was 16.23 seconds. The
  observation covers the registered local DuckDB/Parquet/PostgreSQL disposition matrix and the two
  added DuckDB atomicity laws; excluded destination capabilities, other schemas, remote networks,
  and performance remain outside it.
- 2026-08-02: Refroze the exact error-ownership scope after every closure repair. Eleven Rust files
  contain 126 constructors: 110 production (62 Contract, 40 Data, five Internal, two dynamic
  classifiers, one Auth) and sixteen test-only fixtures/assertions. Seventeen production and
  thirteen test direct-kind sites bring the classified ledger to 156 rows plus its header. The
  durable manifest, semantic AWK classifier, and TSV reproduce without temporary inputs; no direct
  `CdfError` struct literal bypass exists.
- 2026-08-02: Pending closure repair discovered before the governed sweep snapshot: leaf
  validation admits configured Int8/16/32 and UInt8/16/32 cursor fields, but shared cursor
  aggregation admits only Int64/UInt64/Date32/Timestamp arithmetic. Without a configured-field
  promotion, a narrow integer cursor would fail late after source execution. The bounded repair is
  discovery/query promotion of only the configured cursor expression to Int64/UInt64 while
  preserving its physical ClickHouse metadata, plus focused and live cursor laws. No source file
  is changed while the in-flight governed sweep hashes and measures its frozen executable.
- 2026-08-02: The governed release sweep stopped before its first warm-up or timed cell because the
  standalone benchmark opened the leaf partition without the generic planner's exact
  `cdf:physical_schema_hash` binding. It produced zero samples and did not overwrite the existing
  764-byte schema-version-1 JSON, which is explicitly stale and not closure evidence. The benchmark
  now mirrors the direct live helper by binding the partition's admitted effective-schema
  observation hash before open.
- 2026-08-02: Closed the pending narrow-integer frontier gap without widening the source schema
  generally. Discovery promotes only a configured `Int8`/`Int16`/`Int32` cursor to Arrow `Int64`
  and only a configured `UInt8`/`UInt16`/`UInt32` cursor to Arrow `UInt64`, preserving the original
  `cdf:physical_type` plus explicit signed64/unsigned64 cast metadata. Projection, predicate,
  resume bound, row bound, and ordering all consume that same marker; identically typed non-cursor
  fields remain narrow. Leaf validation now admits only the shared durable canonical integer
  domains, and extraction/binding no longer retain unreachable narrow branches. The exact unit law
  passed one test with fifteen filtered. A combined all-target leaf plus roofline-bin check passed
  in 5.65 seconds; formatting and whitespace checks are clean. The live law now uses a physical
  `UInt32` cursor plus a non-cursor `UInt32` control. Its one authorized exact rerun passed one
  test with fifteen filtered in 0.26 seconds, proving effective `UInt64` cursor values and original
  `UInt32` physical metadata without widening the control field.
- 2026-08-02: Refined the raw-I/O ownership repair at the network/source boundary. A nested raw
  `InvalidData` under `Error::Network` is host/TLS construction Environment, while the same kind
  under `Error::Other` remains malformed source Data; timeout/socket failures remain Transient and
  typed nested CDF provenance still wins. The exact wrapper-chain unit law passed one test with
  fifteen filtered.
- 2026-08-02: Refroze the error ledger after the cursor promotion and final raw-I/O split. The
  eleven-file crate now contains 131 constructors: 115 production (62 Contract, 45 Data, five
  Internal, two dynamic classifiers, one Auth) and sixteen test fixtures. Seventeen production
  direct-kind branches plus fifteen test assertions yield 163 classified rows plus the header.
  The file-list and semantic-AWK hashes remain unchanged; the regenerated TSV hash is
  `e1b2aea281761f9543a81840a8f8e0605e864cd02ab2898c8d139c014d0bba08`, and no direct
  `CdfError` struct literal bypass exists.
- 2026-08-02: The next governed sweep executed all eight local cells with correct server/version/
  image attestation and ratios above 0.957, selecting ratio 1.018907 (CDF median 50,207,500 ns;
  direct median 51,156,792 ns), but correctly returned overall fail because every cell's payload
  equivalence predicate failed. Inspection showed a report-metric bug: `useful_arrow_bytes` held
  retained allocator capacity, which legitimately varied by batch allocation even though both
  fixed-width paths read the same one million logical rows. This failed report is not closure
  evidence; the symmetric production timing path itself remained valid and no tuning change is
  indicated.
- 2026-08-02: Repaired benchmark truthfulness by defining useful payload for the exact
  three-by-eight-byte fixture as `rows * 24`, retaining allocator capacity only under the explicit
  `maximum_batch_retained_bytes` name, and bumping the report schema to version 3. Both timed paths
  now validate every `id`, `metric = id * 17`, and `updated_at = id` value and record a deterministic
  content checksum that is invariant to batch boundaries. Cell equivalence requires identical
  rows, logical bytes, and content checksum; equal row counts can no longer hide payload
  corruption, and allocator reuse can no longer create a false mismatch.
- 2026-08-02: Focused post-repair benchmark validation passed. The release-bin target check
  completed in 3.12 seconds. The exact logical-payload/content law passed one test with 28 library
  tests filtered and all unrelated binary/integration targets executing zero selected tests; it
  proves 72 useful bytes for three logical rows, checksum invariance across batch boundaries, and
  Data rejection for a corrupted metric value. It does not prove live timing or the roofline ratio.
- 2026-08-02: The final governed release sweep passed all eight cells in report schema version 3.
  The selected fastest exact bounded cell uses `max_threads=4`, `max_block_rows=65,536`, one reused
  connection, one buffered batch, and no compression. Five samples of 1,000,000 rows selected CDF
  median 50,156,667 ns against direct median 50,181,666 ns, ratio 1,000,498 ppm. Every cell passed
  at ratios from 0.986189 through 1.049246. The report records 24,000,000 logical useful bytes,
  checksum 14451934671319010625, ClickHouse 25.8.28.1, the exact pinned image digest, host/
  comparability data, and workspace/executable hashes. This establishes the ratified 0.90 roofline
  for the recorded local fixed-width fixture; it does not generalize to variable-width schemas,
  remote/cloud networks, other hosts/server versions, excluded unsafe compression, or concurrency
  the source contract forbids.
- 2026-08-02: Final strict lint passed after all source and benchmark repairs:
  `cdf-source-clickhouse --all-targets` in 3.13 seconds and the
  `cdf-benchmarks --bin clickhouse-source-roofline` target in 5.57 seconds, both with
  `-D warnings`. Formatting and `git diff --check` remain clean. The frozen error scope remains
  exact because only the benchmark/report and ticket changed after its final freeze.
- 2026-08-02: Authorized closure repair resolved all five findings from the independent re-review.
  CDF now carries narrow, source-compatible path patches for the pinned official `clickhouse`,
  `clickhouse-ext-arrow`, and Arrow IPC crates. Every discovery and execution query supplies
  lease-derived finite response/IPC limits before its first lazy poll: 64 KiB HTTP/1 transport,
  1 MiB raw/decoded error body, 2 MiB Arrow metadata, and 30 MiB Arrow body. Declared LZ4 frame and
  ZSTD input/output sizes are checked before allocation. The bounded Arrow path also rejects
  dictionary messages because dictionary IDs can otherwise accumulate across messages before a
  record batch is emitted; the pinned live `LowCardinality(String)` fixture proves the admitted
  ClickHouse path materializes exact non-dictionary Binary values. The 64 MiB decode lease covers
  one body, one possible alignment copy, metadata, and remaining Arrow container/chunk headroom;
  a separately named transport lease and 16 MiB discovery-model lease make every retained owner
  explicit.
- 2026-08-02: Temporal and recursive-type closure is exact rather than display-only. `Date32`
  checkpoints now resume through `addDays(toDate32('1970-01-01'), ?)` and the live law starts at a
  negative pre-epoch position. Top-level UUID remains the user-ratified canonical CDF `Utf8` text
  mapping. Containers wrapping UUID, narrow Date, or narrow DateTime fail discovery with exact
  field/type remediation until a complete recursive normalization exists; native Date32 and
  DateTime64 remain admitted. The pinned live fixture now asserts exact Arrow datatype, nested
  field names/types/nullability, physical metadata, and representative values for Array, Tuple,
  Map, Nullable, LowCardinality, Enum8, IPv4, IPv6, and UUID, plus a real `Array(UUID)` rejection.
- 2026-08-02: Removed the concrete ClickHouse identity from generic conformance. The project
  checkpoint/publication law is now a fixture-local ignored test, so adding another source does not
  edit shared runner logic. The exact fixture-owned project law passed against the pinned local
  ClickHouse and ephemeral Postgres service, proving a limited equal-cursor run changes neither
  destination publication nor checkpoint state before the complete resume.
- 2026-08-02: Corrected the memory/performance evidence boundary. `stream_buffer_batches` remains
  queue capacity; the report and owner ledger now derive retained overlap as queue + producer +
  consumer, giving three batches by default and 66 at the configurable maximum. The direct
  benchmark uses the same finite official-client limits as CDF, and workspace content identity now
  covers the connector memory module plus every patched dependency source. The regenerated
  schema-v3 sweep passed all eight cells. The final content-hash refresh recorded CDF median
  44,935,875 ns, direct median 44,914,042 ns, and ratio 999,514 ppm; report SHA-256 is
  `e6a2c383d87280bd98bd49996d1d37b4d10485c0fcd8a3f8766ee759f0898dea`.
- 2026-08-02: Final focused closure validation passed without a workspace-wide suite: source leaf
  17 passed/one live ignored; exact pinned live law 1/1; Arrow IPC stream-limit tests 5/5;
  clickhouse response/LZ4/ZSTD library tests 65/65; benchmark content laws 2/2; fixture-local
  project atomicity 1/1; strict source all-target and benchmark/conformance lib/bin Clippy with
  `-D warnings`. The isolated sandbox failure of the project law was solely local-port permission;
  its authorized rerun passed. The first roofline invocation was likewise unable to reach the
  localhost port from the sandbox; the already-built authorized binary passed without code change.
- 2026-08-02: Refroze error ownership after the memory module and typed response-limit boundary.
  The twelve-file crate contains 142 constructors: 126 production (62 Contract, 50 Data, eleven
  Internal, two dynamic classifiers, one Auth) and sixteen test fixtures. Eighteen production
  direct-kind branches plus eighteen test assertions yield 178 classified rows plus the header.
  File-list SHA-256 is `e936abf959d8c40c8d23cd570e636e1f0b9f0d3f7ec3b1085432dab569253a98`;
  TSV SHA-256 is `3437ce9d29f7361bb14930dd8abfca620b5da1d0ff2903615160450826068335`;
  the classifier remains `d19c916ed1f272f56defe6fc9309e71f67ec3e077fe6023e7816e24e8aeb8bc9`.
- 2026-08-02: A further independent static review found four closure defects after the preceding
  freeze: the 64 MiB decoder calculation used logical body length instead of allocator capacity,
  the pooled client outlived its transport lease, recursive catalog type parsing had no hostile
  text/depth/token preflight, and admitted bare `DateTime` lacked pinned live coverage. The repair
  lowers the body ceiling to 25 MiB and explicitly budgets 32 MiB split-body capacity, one 25 MiB
  alignment copy, 2 MiB metadata, a 64 KiB HTTP frame, and 4 MiB schema/container headroom inside
  the 64 MiB lease. The `OnceLock` now owns the client and transport lease together. An iterative
  64 KiB text/64-level/4,096-token preflight runs before bounded recursive semantic matching.
  The live matrix now contains both bare and UTC-qualified `DateTime` columns with exact Arrow
  timestamp/value assertions.
- 2026-08-02: Focused repair validation passed without a workspace suite: source leaf 19 passed,
  one pinned live test ignored; the exact pinned live law passed 1/1 with bare `DateTime`; the new
  Arrow split-body allocator-capacity pressure law passed 1/1; the generated memory-owner matrix
  is closed; and strict source all-target plus benchmark-bin Clippy passed with `-D warnings`.
  The first release link omitted the repository's required `DUCKDB_DOWNLOAD_LIB=1` setting and
  failed only at `-lduckdb`; the correctly configured incremental release build passed. The first
  roofline execution was sandboxed from localhost; the authorized identical binary then passed.
- 2026-08-02: Refreshed schema-v3 roofline evidence after the 25 MiB limit repair. All eight cells
  pass; the selected ratio is 999,872 ppm from CDF median 44,600,833 ns and direct median
  44,595,167 ns. Queue capacity remains one, the end-to-end in-flight bound remains three, and all
  31 implementation inputs hash to
  `sha256:90ba4990506107d546d278ee1c9fc1500c82ce16edcaec40abfbadda8921f173`.
  Report SHA-256 is `3922d89702847be1d0870e37bcf653c3cefabe80677fa2635587a6a4cd62feef`.
  These figures supersede the earlier schema-v3 content-hash refreshes in this append-only journal.
- 2026-08-02: Refroze error ownership after the final memory, client-lifetime, and parser repairs.
  The twelve-file crate contains 152 constructors: 136 production (62 Contract, 56 Data, fifteen
  Internal, two dynamic classifiers, one Auth) and sixteen test fixtures. Eighteen production
  direct-kind branches plus 22 test assertions yield 192 classified rows plus the header.
  File-list SHA-256 remains `e936abf959d8c40c8d23cd570e636e1f0b9f0d3f7ec3b1085432dab569253a98`;
  TSV SHA-256 is `2a4907c883c81126b4e3862b57039facd3bc2ca2a7515eb5d186c019a744f44a`;
  the classifier remains `d19c916ed1f272f56defe6fc9309e71f67ec3e077fe6023e7816e24e8aeb8bc9`.
- 2026-08-02: Tightened the transport invariant once more so it is independent of current local
  drop order: the cached client and lease share one `Arc`, and every returned Arrow cursor retains
  that authority. A cursor therefore cannot keep a pooled-client clone alive after its transport
  lease is released. The source leaf (19 passed/one ignored), exact pinned live law (1/1), and
  strict source Clippy all passed after this structural repair.
- 2026-08-02: The final release roofline refresh after the cursor-lifetime repair passed all eight
  cells at a selected ratio of 1.014144 (CDF median 44,173,542 ns, direct median 44,798,334 ns).
  The 31 implementation inputs hash to
  `sha256:db50503e55d9381b00022086077aba72c71a29c860ea54b8d10fa6d69b4901eb`; report SHA-256 is
  `de335f6a9b33643298db8383eb5b07468043127bd61270dcff6c0df022ae811e`.
  This is the final authority and supersedes all earlier roofline figures in this append-only
  journal. The final error ledger retains the same 152-constructor/192-row classifications; its
  line-sensitive TSV hash is now
  `fd10730a2317802b174eb72ae6978e3bcd276b8d1ccf4f348a39cf62657c25ec`.
- 2026-08-02: The final independent static reviewer returned fail with four significant defects:
  raw `DateTime(...)` metadata could reach SQL rendering; per-poll decode authority was transferred
  to batches while persistent cursor state remained alive; malformed Arrow schema semantics could
  reach panic-based upstream conversion; and nested fixed-size row estimates omitted child
  validity/padding. One minor evidence defect left an old roofline result unlabeled. Root-owned
  closure repairs now parse and whitelist timezone literals, require exact catalog/effective
  physical metadata before query construction, attach a distinct 4 MiB lease to each live cursor,
  validate Arrow schema semantics fallibly before conversion, and route nested fixed-size shapes
  through the one-row variable-width guard. Focused source test-target compilation passed after
  these repairs; runtime, lint, live, roofline, ledger, and final review evidence remain pending.
- 2026-08-02: Focused closure validation passed after all four repairs: source leaf 20 passed with
  the one pinned live law ignored; the two exact vendored Arrow malformed-schema/header laws
  passed; source all-target and vendored Arrow library Clippy passed with `-D warnings`; the exact
  pinned live law passed 1/1; and the generated memory-owner matrix is closed. No workspace suite
  or broad product matrix was rerun.
- 2026-08-02: The focused release roofline refresh passed all eight cells at ratio 1.012174 (CDF
  median 44,199,791 ns; direct median 44,737,917 ns). Its 32 implementation inputs now include the
  fallible Arrow converter and hash to
  `sha256:98fbe118a455ee7130002c1bbb5bb8c9468caf3c4667d6a5968285b08e1bda8e`;
  executable SHA-256 is
  `sha256:d1af9a09a2f5108b35ac0813ea7cbc4e07b6c15fc3cdea0e979a7faa4beade12`,
  and report SHA-256 is `6a446759ba0d6728192cb8e9faaaf596340bf9293696569ff6a41edc1c00d488`.
  Queue capacity remains one and the truthful in-flight bound remains three.
- 2026-08-02: Refroze the twelve-file error ledger after the closure repairs: 158 constructors,
  comprising 142 production and sixteen test fixtures, plus eighteen production direct-kind
  branches and 24 test assertions. The 200-row TSV SHA-256 is
  `b62bd9a0cd2fa2262e47120911e39e65fbce508bee80806de3749cfbcd876415`;
  file-list and classifier hashes remain unchanged.
- 2026-08-02: The next independent decoder-focused review returned fail with five significant
  defects and one minor evidence omission: hostile record-batch ranges/union prefixes/variadic
  counts still had panic paths; inner Arrow decompression could escape the body authority; the
  4 MiB cursor-state lease did not prove schema conversion ownership; a zero-row batch reserved a
  second 64 MiB lease while the first remained live; flat fixed-width sizing omitted exact
  validity/alignment and could force one oversized row; and the roofline allowlist omitted the
  active reader/compression files. The reviewer explicitly accepted the DateTime authority,
  transport/cursor lifetime structure, nested fixed-size routing, historical evidence labeling,
  crate layout/naming/visibility, conformance isolation, and then-current hashes.
- 2026-08-02: Root-owned closure repair made every record-batch offset, length, node, variadic
  count, union prefix, and union alignment path fallible; bounded cumulative decoded logical
  buffers; capped actual LZ4 output at its declaration; and made the bounded ClickHouse path reject
  any contradictory inner-compressed batch before decoded/alignment-copy overlap. Schema
  conversion now preflights 4,096 nodes, 4,096 metadata entries, depth 64, and a conservative 4 MiB
  owned-size estimate. The persistent cursor authority is 32 MiB, covering one retained 25 MiB
  response chunk plus bounded schema/message/decoder state. Empty batches reuse their current
  decode lease. Flat fixed-width sizing now uses each field's 64-byte-aligned validity and value
  buffers, binary-searches the largest safe block, and rejects a one-row-over-body projection.
  `reader.rs` and `compression.rs` are now roofline identity inputs.
- 2026-08-02: The first live run under the new row ceiling correctly rejected a four-row Arrow
  batch because the test configured `max_block_rows = 2`; this established that ClickHouse may
  coalesce small internal blocks during ArrowStream serialization. The repair separates the
  throughput setting from the safety contract: execution admits at most 1,000,000 rows per record
  batch, catalog discovery at most 16,384, and the zero-row schema probe at most one, while the
  25 MiB message-body limit remains the hard allocation fence. The exact pinned live law then
  passed 1/1, including catalog, type/value/metadata, cursor, equal-cursor, and partial-stream laws.
- 2026-08-02: Focused validation after the decoder repairs passed without a workspace suite: all
  previously passing source tests remained compiled under strict all-target Clippy; the repaired
  fixed-width law passed exactly; the exact pinned live law passed; hostile record-range/union,
  schema/row, declared decompression, actual LZ4 output, and bounded compressed-batch rejection laws
  passed; and strict source, vendored Arrow IPC, and benchmark-bin Clippy passed with `-D warnings`.
  The standalone excluded `clickhouse-ext-arrow` manifest cannot see the workspace's patched Arrow
  and ClickHouse crates, so its direct manifest lint resolves incompatible upstream APIs; the root
  source/live/release builds compile the patched trio together. The generated memory-owner matrix
  check and `git diff --check` pass.
- 2026-08-02: Final schema-v3 roofline evidence from the rebuilt release binary passes all eight
  cells at ratio 1.019339 (CDF median 44,624,750 ns; direct median 45,487,792 ns). Queue capacity is
  one, truthful in-flight bound three, and all 34 active implementation inputs hash to
  `sha256:ce048c46c1dbe602c8128590250bf5828c13650754eaba24f9e4cc0e9bf3884e`.
  Executable SHA-256 is
  `sha256:2c817f89345f22e11c4e6e33b55672be97cde311d7ca048e69f6a96a387eebe5`;
  report SHA-256 is `6615403e99cba2c923915201c16b51fe52ab3203f0846ecc9f7822432f369e4b`.
- 2026-08-02: Final error ownership freeze contains 162 constructors: 146 production (63 Contract,
  61 Data, nineteen Internal, two dynamic classifiers, one Auth) and sixteen test fixtures;
  eighteen production direct-kind branches and 25 test assertions produce 205 classified rows.
  File-list SHA-256 remains
  `e936abf959d8c40c8d23cd570e636e1f0b9f0d3f7ec3b1085432dab569253a98`, TSV SHA-256 is
  `d216d7889d2372d04db0c13d5f78f05b0ce38b46f6e298c854aecded907e0204`, and classifier SHA-256
  remains `d19c916ed1f272f56defe6fc9309e71f67ec3e077fe6023e7816e24e8aeb8bc9`.
- 2026-08-02: Final independent review found one remaining significant malformed-input panic:
  `create_struct_array` passed a remotely sized Struct validity buffer directly to
  `BooleanBuffer::new`, whose short-buffer contract is infallible. The repair checks the exact
  `ceil(rows / 8)` validity prefix through the existing typed IPC error path before construction.
  A focused one-row nullable Struct law proves a zero-byte validity buffer now returns
  `Ipc error: IPC struct validity buffer requires 1 bytes but its buffer contains only 0 bytes`
  instead of panicking. That exact law passed 1/1, and strict vendored Arrow IPC library Clippy
  with `lz4` passed under `-D warnings`.
- 2026-08-02: The exact pinned live ClickHouse source contract passed 1/1 after the Struct repair.
  The identity-sensitive release roofline was rebuilt once and all eight cells passed at ratio
  1.008401 (CDF median 44,669,459 ns; direct median 45,044,750 ns). The 34 active inputs now hash
  to `sha256:682058a35f397b5348ebbfe29d73b9490a91a99a0b2250b03203b9952c6e7337`, the executable hashes
  to `sha256:df6291747412a559856bfa9aec72c93f1c942be800b4857dcab0de7ab74a85b6`, and the report SHA-256
  is `7e96544367e3b0b7a3358d93f87b17fb99dcfbb119a8793e758f324f7df3722a`. The source-only error
  ledger remained byte-identical because the final repair touched only vendored Arrow IPC.
- 2026-08-02: Final independent static closure review passed with no critical, significant, minor,
  or nit findings. The reviewer independently recomputed the 34-input workspace hash, executable
  hash, report hash, eight-cell ratio, and 205-row error ledger and found every value consistent
  with current bytes. Parent-owned fresh certification and product/core closure gates remain
  deferred to the connector-program integration pass.

## Blockers

- None within this child implementation. Parent-owned fresh connector certification and
  product/core closure gates remain deferred to the connector-program integration pass.

## Evidence

- Focused compile: `CARGO_BUILD_JOBS=12 cargo check -p cdf-source-clickhouse --tests -j 12` — pass,
  dev profile completed in 1.24 seconds after dependency resolution; scope limited to the leaf and
  its test targets.
- Focused source leaf after the physical/effective schema repair:
  `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-clickhouse --lib --locked -j 12` — 14 passed, zero
  failed, one digest-pinned live test ignored at that checkpoint. The later exact configured-cursor
  widening law passed 1/1; these prove the selected non-live leaf laws, not live server behavior.
- Final strict affected leaf lint:
  `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-clickhouse --all-targets --locked -j 12 -- -D
  warnings` — pass in 3.13 seconds after every source repair.
- Digest-pinned exact live law after the configured-cursor repair:
  `CDF_CLICKHOUSE_ENDPOINT=clickhouse://127.0.0.1:18123 CARGO_BUILD_JOBS=12 cargo test -p
  cdf-source-clickhouse tests::live_clickhouse_type_cursor_and_partial_stream_contract --locked -j
  12 -- --ignored --exact --nocapture` — 1 passed, 15 filtered, 0.26 seconds. Scope is the pinned
  local server's type/value/metadata, narrow-cursor, equal-cursor, partial-stream, and wide-row laws.
- Registered matrix catalog/shard guard after the fixture repair:
  `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance
  registered_run_matrix_shards_cover_source_catalog --locked -j 12` — 1 passed, 106 filtered.
- Digest-pinned generic ClickHouse source matrix — 12 executed cells plus equal-cursor and real
  post-batch project/package atomicity laws passed, with three capability exclusions, in 16.23
  seconds. Scope is the registered local matrix and asserted DuckDB atomicity laws; excluded
  capabilities, other schemas, remote networks, and performance remain outside this observation.
- Focused benchmark harness check after routing direct futures through injected I/O authority:
  `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo check -p cdf-benchmarks --bin
  clickhouse-source-roofline --locked -j 12` — pass in 7.75 seconds. This proves the repaired
  benchmark target compiles with its linker prerequisite; it does not prove live reactor behavior
  or the roofline ratio.
- Exact benchmark logical-payload/content law — 1 passed, 28 library tests filtered; proves
  `rows * 24` useful-byte accounting, content-checksum invariance across batch boundaries, and Data
  failure on payload corruption. Final strict benchmark-bin Clippy passed with `-D warnings` in
  5.57 seconds.
- Historical/superseded release ClickHouse source roofline:
  `.10x/evidence/.storage/2026-08-02-clickhouse-source-roofline.json` — schema v3 pass; all eight
  cells pass; selected ratio 1.000498 from CDF median 50,156,667 ns and direct median 50,181,666 ns
  across five 1,000,000-row samples, with 24,000,000 logical bytes and checksum
  14451934671319010625. Exact server/image, host/comparability, workspace/executable identity,
  settings, raw samples, CPU/RSS, retained batch maxima, sweep, and exclusions are recorded. Scope
  is the recorded local fixed-width fixture and does not generalize to excluded or remote cells.
- Frozen error ownership:
  `.10x/evidence/2026-08-02-clickhouse-source-error-ownership.md` and its `.storage` inventories —
  exact for the final twelve-file Rust crate; taxonomy coverage is not exhaustive live fault
  injection.
- Official dependency allocation guards: isolated Arrow IPC stream tests 5/5 and ClickHouse
  response/LZ4/ZSTD library tests 65/65. These prove declared metadata/body/frame and cumulative
  dictionary-path guards in the patched sources; they do not prove arbitrary Hyper/platform
  allocator retention beyond the separately leased fixed 64 KiB transport buffer.
- Historical/superseded schema-v3 roofline:
  `.10x/evidence/.storage/2026-08-02-clickhouse-source-roofline.json` — all eight cells pass;
  selected ratio 1.014144 (CDF 44,173,542 ns, direct 44,798,334 ns), queue capacity one,
  end-to-end in-flight bound three, 31 hashed implementation inputs, SHA-256
  `de335f6a9b33643298db8383eb5b07468043127bd61270dcff6c0df022ae811e`.
- Historical/superseded post-closure schema-v3 roofline:
  `.10x/evidence/.storage/2026-08-02-clickhouse-source-roofline.json` — all eight cells pass;
  selected ratio 1.012174 (CDF 44,199,791 ns, direct 44,737,917 ns), queue capacity one,
  end-to-end in-flight bound three, 32 hashed implementation inputs including the fallible Arrow
  converter, SHA-256 `6a446759ba0d6728192cb8e9faaaf596340bf9293696569ff6a41edc1c00d488`.
- Historical closure-focused validation: ClickHouse source leaf 20 passed/one live ignored; the exact
  pinned live law passed 1/1; two vendored malformed-Arrow error laws passed; strict source
  all-target and vendored Arrow library Clippy passed with `-D warnings`; the generated memory
  owner check passed closed. Scope excludes the workspace suite and unrelated connector matrices.
- Historical/superseded decoder-closure schema-v3 roofline:
  `.10x/evidence/.storage/2026-08-02-clickhouse-source-roofline.json` — all eight cells pass;
  selected ratio 1.019339 (CDF 44,624,750 ns, direct 45,487,792 ns), queue capacity one,
  end-to-end in-flight bound three, 34 hashed implementation inputs including Arrow reader and
  compression ownership, SHA-256
  `6615403e99cba2c923915201c16b51fe52ab3203f0846ecc9f7822432f369e4b`.
- Current focused decoder closure: the repaired flat fixed-width law and exact pinned live law pass;
  hostile record-range/union/alignment, schema/row, declared/actual decompression, and bounded
  compressed-batch rejection laws pass; strict source all-target, vendored Arrow IPC, and
  benchmark-bin Clippy pass with `-D warnings`; the generated memory-owner matrix is closed.
  Scope deliberately excludes the workspace suite and unrelated connector matrices.
- Current Struct-closure schema-v3 roofline:
  `.10x/evidence/.storage/2026-08-02-clickhouse-source-roofline.json` — all eight cells pass;
  selected ratio 1.008401 (CDF 44,669,459 ns, direct 45,044,750 ns), queue capacity one,
  end-to-end in-flight bound three, and 34 hashed inputs. Workspace SHA-256 is
  `682058a35f397b5348ebbfe29d73b9490a91a99a0b2250b03203b9952c6e7337`, executable SHA-256 is
  `df6291747412a559856bfa9aec72c93f1c942be800b4857dcab0de7ab74a85b6`, and report SHA-256 is
  `7e96544367e3b0b7a3358d93f87b17fb99dcfbb119a8793e758f324f7df3722a`.
- Current focused Struct closure: the exact hostile Struct-validity decoder law passes 1/1;
  vendored Arrow IPC library Clippy with `lz4` passes under `-D warnings`; and the exact pinned
  live source contract passes 1/1. These observations cover the repaired constructor, linted
  vendored library, and configured live server path without rerunning the workspace suite.

## Review

### Independent red-team review — 2026-08-02

Method: deterministic review scope/rule resolution through `open-code-review-delegate`, followed by
an independent read of the complete ticket, every referenced active authority, the current diff,
the eleven-file source crate, the conformance fixture, benchmark implementation, frozen error
ledger, operator documentation, and the recorded roofline JSON. Per the review commission, no
validation command was rerun and no implementation file was changed.

#### Findings

1. **critical — A pushed `LIMIT` can permanently skip rows in a cursor window.**
   `crates/cdf-source-clickhouse/src/resource.rs:283` advertises limits as supported and
   `resource.rs:358-363` carries the limit into the compiled scan. `query.rs:210-222` resumes with
   `cursor > checkpoint`, while `query.rs:255-258` appends the limit to the cursor-ordered SQL; the
   unit assertion at `tests.rs:370` freezes that combination. If the limit cuts an equal-cursor
   group, the emitted checkpoint contains only the cursor value, so the next run excludes the
   unconsumed rows in that group. This violates the shared window-close law and the incomplete-
   window requirement in `clickhouse-table-source.md:43-53`. Required repair: never push a limit
   across a cursor frontier unless the durable position also represents the stable-key component;
   with the current shared cursor shape, reject/retain the limit for engine evaluation and add a
   project-run regression where the requested limit bisects an equal-cursor group.

2. **significant — The declared 64 MiB preaccounted batch bound is not an allocation bound.**
   `execution.rs:79-94` reserves 64 MiB and then lets `cursor.next()` allocate/decode an Arrow batch;
   only after allocation does `execution.rs:101-108` measure and reject a batch larger than 64 MiB.
   `max_block_rows` is a row bound, not a byte bound, so a wide `String`, array, tuple, or map row can
   exceed the reservation before the post-fetch check. Nevertheless `driver.rs:671-676`,
   `docs/memory-allocation-owners.md`, and `docs/clickhouse.md:88-93` describe the path as bounded and
   preaccounted. This contradicts `clickhouse-table-source.md:65-68`. Required repair: establish and
   enforce an upstream decoded-byte ceiling (or account a proven worst case) before the official
   client allocation is admitted, then prove the bound with wide-variable-value, slow-consumer,
   pressure, and cancellation cases before restoring the `Preaccounted`/64 MiB claims.

3. **significant — Declared-schema execution relabels logical schema as physical observation.**
   `execution.rs:148-155` returns the projected effective/output schema as the physical schema when
   no effective-schema runtime exists. That schema is then hashed as observed and marked as
   materialized physical output at `execution.rs:69-70,119-126`. Declared schemas remain executable,
   including the roofline resource, so the exact Arrow schema can be neither discovered nor frozen
   before execution as required by `clickhouse-table-source.md:22-28`. Required repair: require a
   catalog observation for every execution (including declared-schema mode) or fail closed; never
   synthesize physical evidence from the logical declaration. Add a declared-schema drift test that
   proves physical and effective authorities remain distinct.

4. **significant — The advertised temporal cursor/type surface is not exact.** Bare ClickHouse
   `DateTime` is absent from the leaf allowlist at `types.rs:17-39`; only parenthesized
   `DateTime(...)` reaches the handler at `types.rs:58`. The live fixture avoids the case by using
   `DateTime('UTC')` (`tests.rs:488`). Separately, nanosecond Arrow timestamps are silently truncated
   to microseconds at `execution.rs:421-445`, although `DateTime64` scale 7-9 is admitted and the
   spec promises typed `DateTime`/`DateTime64` cursor values (`clickhouse-table-source.md:43-46`).
   The latter causes replay/duplicate frontiers and is not an exact cursor representation. Required
   repair: admit and test bare `DateTime`; for sub-microsecond `DateTime64`, either add an exact
   durable cursor representation or fail cursor configuration closed above scale 6, with scale and
   negative-epoch tests.

5. **significant — The live “type matrix” does not prove the required round trips or ordinary
   `String` binary preservation.** `tests.rs:468-511` checks catalog physical-type labels only; the
   actual read at `tests.rs:532-547` projects/asserts only UUID text. It would still pass if ordinary
   `String` were decoded as UTF-8 or decimals, nested values, enums, IPs, temporal metadata, or
   signedness were corrupted. This falls short of the live round-trip requirement at
   `clickhouse-table-source.md:55-63`. Required repair: read the full seeded record and assert every
   supported Arrow datatype, metadata item, nullability, and representative value, explicitly
   asserting ordinary `String` is `Binary` while only physical UUID is canonical `Utf8`.

6. **significant — The partial-stream fixture proves emit-then-error, not no checkpoint commit.**
   `tests.rs:578-599` directly drains the leaf stream, asserts at least one row, and observes a Data
   error. It never executes project/package commit or inspects durable checkpoint state. The generic
   conformance fixture at `crates/cdf-conformance/src/run_matrix/clickhouse_fixture.rs:50-57` covers
   only successful cursor advancement. Therefore the acceptance scenario in
   `clickhouse-table-source.md:72-75` remains unproved. Required repair: run the deterministic
   post-batch server fault through the project/package path and assert the prior checkpoint and
   publication state remain unchanged.

7. **significant — The recorded roofline is a useful one-cell timing smoke, not valid closure
   evidence.** The report schema at `clickhouse_source_roofline.rs:17-36` and the recorded JSON omit
   dispersion, useful Arrow bytes, observable physical bytes, CPU, RSS, host/comparability key,
   batch-size observations, executable/workspace identity, and the actual server version required
   by `database-connector-roofline.md:25-40`. The harness runs one fixed setting rather than the
   required sweep over byte/row targets, compression, connection reuse, `max_threads`, and bounded
   client concurrency (`database-connector-roofline.md:47-50`). Finally,
   `clickhouse_source_roofline.rs:138-143` copies `CDF_CLICKHOUSE_IMAGE` into the report without
   interrogating the endpoint; the live and conformance fixtures likewise accept any endpoint, so
   the digest and security-supported-server claim are not attested. The alternating direct/CDF
   timing and `direct_median / cdf_median` arithmetic at lines 109-129 are symmetric and the stored
   1.038211 calculation is correct for this one cell. Required repair: implement the governed sweep
   and complete report schema, record all required counters/biases, select the fastest exact bounded
   cell, and obtain the running server version/image digest from an independently verified local
   container/endpoint rather than a caller-supplied label.

8. **significant — Untyped nested host I/O errors lose ownership through the ClickHouse wrapper.**
   `error.rs:72-103` correctly preserves embedded typed `CdfError`s, and server text is redacted.
   But after that search, `error.rs:11` classifies every raw `Error::Network` as `Transient`, while
   `error.rs:54-57` classifies every raw `Error::Other` as `Data`; it does not recursively classify a
   nested raw `std::io::Error`. A wrapped permission, descriptor, memory, resolver/TLS-construction,
   or device failure can therefore be assigned to the source/retry loop instead of the host,
   contrary to `error-ownership-taxonomy.md:29-44,68-84`. Required repair: preserve local-versus-
   remote provenance, recursively inspect `std::io::Error::get_ref()`, map host facility/resource
   failures to `Environment`, and add wrapper-chain tests for typed CDF, host I/O, malformed source,
   TLS/resolver construction, timeout, and redaction.

#### Boundaries that held

- Crate/file layout and naming are coherent: `lib.rs:3-13` keeps focused private modules and exports
  only `ClickHouseSourceDriver`; the manifest's production dependencies are downward runtime/kernel/
  memory/HTTP leaves plus the pinned official ClickHouse/Arrow clients, with engine/futures confined
  to dev dependencies (`Cargo.toml:9-25`). No generic SQL abstraction or sibling source leaks into
  the leaf.
- The concrete `clickhouse` kind is enrolled through the built-in registry
  (`cdf-builtin-drivers/src/lib.rs:14,199-205`) and the matrix/catalog changes are data-driven rather
  than new generic kind branches. Identifier validation/bindings, UUID-only `toString` mapping,
  ordinary-String intent, one official ArrowStream query, host-owned async/cancellation/egress, and
  absence of a connector-local executor, pool, queue, or retry loop all held under source review.
- Typed error preservation and message redaction held; the error finding is limited to untyped raw
  I/O provenance. Direct/CDF query work, settings, alternating order, row equivalence, and ratio
  arithmetic held for the single recorded benchmark cell; the performance finding concerns the
  missing governed sweep, counters, comparability, and immutable-server attestation.

#### Verdict

**fail.** The critical cursor-limit path admits durable data loss, and seven significant findings
leave the memory, schema-authority, temporal, live-conformance, checkpoint, roofline, image, and
error-ownership acceptance criteria unsupported. The ticket is not ready for certification or
closure; `Blockers: None` and the passing roofline/complete implementation claims are not supported
by the current implementation and evidence.

#### Residual risk

This review did not rerun tests or live cells and therefore relies on the ticket's journaled
observations only within their stated limits. It did inspect the actual assertions and recorded
JSON. After the required repairs, an independent reviewer must re-audit the equal-cursor frontier,
preallocation bound, physical/effective schema evidence, all supported live values, project-level
failure atomicity, complete roofline sweep/attestation, and raw-I/O ownership. The shared cursor
format still cannot represent a stable-key mid-group frontier; until that shared contract changes,
cursor-side server limits must remain disabled/fail-closed.

### Independent closure re-review — 2026-08-02

Method: deterministic scope and rule resolution through `open-code-review-delegate`, followed by a
fresh independent read of the complete current ticket and authorities, the current ClickHouse
source/conformance/benchmark diff, the frozen error ledger, and the schema-v3 roofline report. This
review did not rerun tests, the live matrix, benchmarks, Clippy, or workspace validation and did not
change implementation files.

#### Disposition of the original findings

1. **closed — cursor-limit data loss.** Cursor scans no longer place `LIMIT` in ClickHouse SQL, and
   the real project/package law bisects an equal-cursor group, proves no publication or checkpoint
   after the incomplete run, then proves the unbounded resume emits the complete group.
2. **not closed — preaccounted response memory.** The normal successful-result path is materially
   safer, but the pinned official client still has an unbounded response-allocation path described
   in finding 1 below.
3. **closed — physical/effective schema authority.** Execution now requires the catalog observation
   and exact planned physical-schema hash and fails closed when either authority is absent or drifts.
4. **not closed — temporal exactness.** Bare `DateTime` normalization and sub-microsecond cursor
   rejection are repaired; negative `Date32` resume remains inexact as described in finding 2.
5. **not closed — supported-type round trips.** The repaired live law establishes the top-level
   scalar, ordinary-`String`, UUID, and temporal cases, but the recursive admission surface and its
   assertions remain wider than the normalization proof, as described in finding 3.
6. **closed — partial-stream checkpoint atomicity.** The deterministic post-batch server fault now
   runs through the project/package boundary and proves destination and checkpoint state unchanged.
7. **not closed — roofline evidence.** The schema-v3 sweep, content equivalence, dispersion,
   immutable server attestation, host/workspace/executable identity, and ratio arithmetic are sound;
   the recorded in-flight bound is not, as described in finding 5.
8. **closed — raw I/O error ownership.** Typed CDF errors retain authority; nested raw I/O is
   recursively classified by provenance, and the frozen ledger/tests cover host, network, malformed
   data, TLS construction, timeout, and redaction cases.

#### Current findings

1. **significant — the official non-success response path is still unbounded before admission.**
   `client.rs:91-97`, `query.rs:200-215`, `types.rs:447-460`, and `execution.rs:83-112` disable
   compression, bound normal variable-width rows/blocks, reserve 64 MiB, and reject an oversized
   decoded batch. They do not bound the error response consumed inside the pinned dependency before
   `fetch_arrow()` returns: `clickhouse 0.15.1 src/response.rs:127-163` explicitly collects the whole
   non-2xx body into one contiguous buffer and records that it performs no length check, so a
   malicious server, proxy, or MITM can allocate arbitrary memory behind the 64 MiB lease. The
   success cursor has the same unproved frame boundary: `clickhouse-ext-arrow 0.1.0 src/lib.rs:374-428`
   accepts each opaque `BytesCursor` chunk into its decoder buffer before the adapter can inspect it.
   Therefore `driver.rs:701-702`, `docs/clickhouse.md:107-112`, and the ticket's `Preaccounted` claim
   remain stronger than the implementation. Smallest required repair: patch/upgrade the pinned
   official path to enforce hard error-body and success-frame/decode ceilings before allocation,
   thread those ceilings from the admitted lease, and retain the current row/block checks as a
   second fence; until then mark this owner open and do not advertise `Preaccounted`.

2. **significant — negative `Date32` checkpoints compile through the unsigned `Date` family.**
   `query.rs:478-481` renders every `Date32` cursor as
   `addDays(toDate('1970-01-01'), ?)`, although the advertised `Date32` domain includes days before
   1970. `tests.rs:564-573` proves only that `-3652` can be extracted; it never builds or executes
   the resume predicate. Smallest required repair: use a `Date32`-preserving expression such as
   `addDays(toDate32('1970-01-01'), ?)` and add a live negative-epoch checkpoint/resume assertion.

3. **significant — recursive type admission exceeds recursive normalization and proof.**
   `types.rs:70-116` recursively admits `Nullable`, `LowCardinality`, `Array`, `Map`, and `Tuple`
   containing normalization-sensitive `UUID`, `Date`, or `DateTime`, while `query.rs:428-445`
   transforms only exact top-level physical-type strings. Both discovery (`catalog.rs:93-110`) and
   execution therefore send wrapped values through the raw ArrowStream mapping that the top-level
   fixes were added to avoid. The live fixture at `tests.rs:671-833,1198-1219` contains only
   top-level UUID/temporal values and generic numeric/string containers; its container assertions
   are merely nonempty rather than exact datatype, metadata, nullability, and value checks.
   Smallest required repair: fail admission for normalization-sensitive wrapped types, or implement
   exact recursive SQL/Arrow normalization for every admitted wrapper, then add live exact-schema
   and representative-value assertions for that full recursive matrix.

4. **significant — generic conformance now contains a concrete connector branch.**
   `crates/cdf-conformance/src/run_matrix/tests.rs:82-85` compares the selected archetype to
   `SourceArchetype::clickhouse()` and invokes a ClickHouse-specific law. This is precisely the
   shared-layer kind branch prohibited by the source-extension invariant; adding the next source
   law now requires editing the generic runner. Smallest required repair: make fixture-owned laws a
   data/capability entry carried by the source matrix (or execute this law as a fixture-local shard
   test) so the generic runner never names ClickHouse.

5. **significant — schema-v3 understates the in-flight batch bound.**
   `clickhouse_source_roofline.rs:297,314` copies `STREAM_BUFFER_BATCHES == 1` into both the channel
   capacity and `in_flight_batch_bound`; the recorded JSON repeats `1` at line 125. Production
   creates that bounded channel at `resource.rs:123-145`, but after a successful send the producer
   immediately reserves and decodes the next batch at `execution.rs:81-131`. A capacity-one channel
   can therefore retain one queued batch while the producer owns the next admitted batch (and the
   consumer may own the delivered batch); one is a queue-capacity value, not an end-to-end in-flight
   bound. `tools/memory-owner-classifications.json:68-73` repeats the same stale one-batch authority.
   Smallest required repair: record queue capacity separately, derive and document the actual
   producer/channel/consumer overlap bound, update the owner classification, and regenerate the
   schema-v3 report with the corrected field.

#### Boundaries and layout that held

- The crate layout remains cohesive and conventional: focused private `catalog`, `client`, `driver`,
  `error`, `execution`, `identifier`, `query`, `resource`, and `types` modules sit behind the single
  public `ClickHouseSourceDriver` facade (`lib.rs:3-16`). Names describe ownership rather than
  mechanism, import direction is acyclic, and there is no speculative interface, pool, executor,
  retry loop, or generic SQL layer inside the leaf.
- Production dependencies remain downward-only (`Cargo.toml:9-25`): kernel/runtime/memory/HTTP,
  Arrow, and the pinned official ClickHouse clients; engine and futures are test-only. The concrete
  kind is enrolled at the built-in composition root (`cdf-builtin-drivers/src/lib.rs:199-207`), and
  no ClickHouse identity appears in production kernel/runtime/engine/project/CLI code.
- Identifier validation and bindings, one reusable official client, cancellation/egress injection,
  exact physical-schema binding, cursor-limit atomicity, partial-stream atomicity, error ownership,
  redaction, benchmark payload equivalence, and immutable local-server attestation held. The only
  shared-layer leakage found is the conformance test branch called out above.

#### Verdict

**fail.** The original critical cursor-loss defect and four original significant findings are
closed, but original findings 2, 4, 5, and 7 remain partially open, and the generic conformance
branch adds a new significant abstraction leak. The connector cannot yet support the ticket's
bounded/preaccounted memory, exact supported-type, complete roofline, or extension-boundary claims.
`Blockers: None` and independent-review acceptance are not supported until all five findings are
repaired and independently reviewed.

#### Residual risk

This was a source-and-evidence review only. It relies on the ticket's journaled executions within
their stated limits and makes no fresh runtime claim. Parent-owned connector certification and
product/core impact remain outside this review. Even after the five defects are repaired, the
official client's private streaming buffers require explicit upstream-bound evidence; successful
normal-result measurements alone cannot prove the hostile/error response ceiling.

### Independent allocator/lifetime closure review — 2026-08-02

Method: fresh independent static review of the repaired official-client, Arrow decoder, source,
conformance, benchmark, memory-owner, documentation, and ticket boundaries. No files were changed
and no tests were rerun by the reviewer.

#### Findings

1. **significant — allocator capacity was absent from the 64 MiB envelope.** A near-30 MiB split
   body could hold a 32 MiB `MutableBuffer`, a body-sized alignment copy, and 2 MiB metadata before
   schema/array/container allocations. The logical `body * 2 + metadata` equation left no real
   headroom.
2. **significant — the reusable client/pool outlived its transport lease.** Discovery and execution
   retained the pooled `Client` in `OnceLock` but held the 64 KiB lease only as a local variable.
3. **significant — catalog type strings reached recursive matchers without a finite parser bound.**
   Hostile nesting or fanout could exhaust the stack or cause quadratic rescanning before the Arrow
   schema-node guard applied.
4. **significant — bare `DateTime` was admitted without pinned live-server coverage.** Only the
   explicitly UTC-qualified form appeared in the exact live type matrix.

#### Verdict

**fail.** The prior five closure findings were repaired, but these four defects prevented final
bounded-memory, lease-lifetime, hostile-metadata, and complete temporal-matrix acceptance. The
review explicitly closed layout/naming, dependency direction, UUID/Date/DateTime wrapper behavior,
negative `Date32`, generic-conformance separation, dictionary accumulation, in-flight roofline
accounting, and benchmark identity.

#### Residual risk

Static review only. The repair required a capacity-pressure law, exact pooled-lifetime ownership,
pre-recursion parser limits, and a pinned live bare-`DateTime` assertion before another independent
closure judgment.

### Final closure review — 2026-08-02

Method: independent static review of the complete ClickHouse source diff, governing records,
vendored response/Arrow boundaries, conformance fixture, benchmark identity, memory-owner matrix,
and recorded evidence. The reviewer changed no files and ran no tests.

#### Findings

1. **significant — raw DateTime physical metadata reached SQL rendering.** Validation accepted an
   argument based only on surrounding quotes, and query generation interpolated the entire raw
   argument. Effective physical metadata was not required to equal catalog authority first.
2. **significant — decode authority did not cover persistent cursor state.** The per-poll lease was
   reconciled and transferred with an emitted batch while the live cursor still retained its
   schema, decoder state, and possible accepted-input remainder.
3. **significant — malformed Arrow schemas could panic.** The bounded stream called upstream's
   infallible FlatBuffer-to-Arrow converter before CDF validation, leaving invalid widths, enum
   values, child cardinalities, decimal metadata, and union IDs on panic paths.
4. **significant — nested fixed-width block sizing omitted child validity buffers and IPC padding.**
   The admitted value-width estimate could therefore conflict with the later hard body ceiling.
5. **minor — one older roofline Evidence entry was not explicitly labeled superseded.**

#### Verdict

**fail.** The reviewer explicitly found the 64 MiB transient decode arithmetic, pooled transport
lease lifetime, bounded type parser, bare `DateTime`, UUID/Date32/wrapper normalization,
dictionary rejection, generic-conformance isolation, current hashes, memory documentation, and
crate layout/naming sound. The five findings above required closure repair and another independent
judgment.

#### Residual risk

Static review only; no new runtime claim was made. The next judgment must inspect the actual
repairs and their focused evidence rather than relying on this failed checkpoint.

### Post-closure decoder red-team review — 2026-08-02

Method: independent static review of the repaired source, vendored Arrow IPC boundaries, memory
owners, benchmark identity, governing records, and evidence. The reviewer changed no files and ran
no tests.

#### Findings

1. **significant — malformed record-batch metadata retained panic paths.** Signed buffer ranges,
   union prefix slices, and variadic exhaustion were not fully fallible.
2. **significant — inner Arrow compression could bypass the decode budget.** A bounded encoded body
   could declare or produce larger decoded buffers before cumulative authority applied.
3. **significant — the 4 MiB cursor lease did not prove converted-schema ownership.** A bounded wire
   schema could expand into larger owned field names, metadata, and containers.
4. **significant — zero-row execution could self-block.** It requested a second full decode lease
   while the first remained held.
5. **significant — flat fixed-width sizing remained incomplete.** Fixed-size binary width,
   nullability, multiple fields, and per-buffer padding could cross the body ceiling after forcing
   at least one row.
6. **minor — benchmark identity omitted the active Arrow reader and compression modules.**

#### Verdict

**fail.** The reviewer explicitly accepted the DateTime authority, transport/cursor lifetime
structure apart from capacity, nested fixed-size routing, historical roofline labeling, crate
layout/naming/visibility, conformance isolation, and then-current hashes. The six findings above
required the decoder-closure repair and fresh evidence now journaled in this ticket.

#### Residual risk

Static review only. The next independent judgment must inspect the fallible record-batch paths,
compressed-batch policy, owned-schema budget, exact fixed-width estimator, live coalescing behavior,
and current 34-input roofline rather than relying on this failed checkpoint.

### Final decoder red-team review — 2026-08-02

Method: independent static review of the complete repaired tranche and byte-identity evidence. The
reviewer changed no files and reran no validations.

#### Findings

1. **significant — a short Struct validity bitmap could still panic.** A nonzero nullable Struct
   node could declare an in-body zero-length validity buffer and reach `BooleanBuffer::new`, whose
   short-buffer behavior is a panic. No other critical or significant finding remained.

#### Verdict

**fail.** The focused checked-prefix repair, hostile decoder law, live contract, and refreshed
roofline evidence recorded above supersede this checkpoint; final independent judgment remains
required.

#### Residual risk

Static review only. The next review must inspect the exact Struct validity repair and refreshed
34-input identity rather than relying on this failed checkpoint.

### Final independent closure review — 2026-08-02

Method: independent static review of the final Struct validity repair, hostile decoder law,
governing records, current diff, and refreshed byte-identity evidence. The reviewer changed no
files and reran no validations or benchmarks.

#### Findings

No critical, significant, minor, or nit findings. The reviewer confirmed that `len.div_ceil(8)` is
overflow-safe, the checked prefix proves the exact required bitmap bytes before the infallible
constructor, and the hostile one-row law covers the former panic condition. It independently
recomputed the 34-input workspace hash, executable hash, report hash, eight-cell ratio, and
unchanged 205-row error ledger; all match the current records and bytes.

#### Verdict

**pass.** No critical or significant finding remains.

#### Residual risk

Static review only. The measured roofline remains scoped to its recorded pinned server, host,
fixed-width fixture, and explicit exclusions. Parent-owned certification and product/core closure
remain outside this child review.

## Retrospective

The implementation stayed small because it reused the source runtime, memory, discovery,
effective-schema, and generic-matrix authorities rather than adding connector-local machinery.
The official Arrow extension, however, exposed three boundaries that non-live compilation could
not establish. First, ClickHouse 25.8 ArrowStream rejects raw UUID, so the durable answer required
an explicit, user-ratified canonical `toString(UUID)` mapping rather than broad stringification.
Second, discovery evidence distinguishes effective output from physical observation authority;
using the effective schema for physical hashes looked plausible until the generic matrix forced
metadata divergence. Third, the standalone root blocker is a composition boundary, not a Tokio
reactor for direct client futures; both the live fixture and roofline had to route direct I/O
through injected `ExecutionServices`.

The costliest dead end was the partial-stream fixture. Small/compressible blocks let ClickHouse and
LZ4 defer the server failure before any Arrow batch became observable. Separately inserted,
incompressible bounded blocks made the required emit-then-fail law deterministic without weakening
the assertion. The most effective technique throughout was to preserve the failing live law and
move the fixture or ownership boundary until the evidence matched the contract: exact server code
50 established UUID behavior, discovery-mode matrix coverage exposed observation and physical
schema mistakes, and alternating timed samples exposed only benchmark overhead after untimed
warm-up.

Five whys on the recurring reactor failure: direct futures panicked because no Tokio handle was
entered; they had been passed to `block_on_root`; that API was mistaken for general async runtime
authority; the distinction between root composition and injected I/O execution was implicit in
the harness; and no non-live benchmark law checked reactor entry. The repair centralizes all
direct seed/query futures on the injected I/O reactor and adds that law. Future official-client
benchmarks should establish runtime authority, physical/effective schema ownership, and untimed
preparation boundaries before their first live run.

Residual risk is bounded and explicit: the shared cursor checkpoint stores the cursor value but
not its stable-key tie-breaker, so an equal-cursor group cannot represent a durable mid-group
frontier. The connector does not invent a private checkpoint format; broader correction belongs
to the shared source contract.

The closure review added a second set of lessons. Row-count limits and byte-allocation limits are
different authorities: an official decoder cannot honestly claim a hard lease from
`max_block_rows` alone, and a server setting checked after a block is not preallocation evidence.
The bounded path required no response compression, a server-enforced variable-row ceiling, a
schema-derived Arrow-body row ceiling, and a schema-complexity ceiling together. Likewise, a
physical schema is evidence, not a convenient copy of the effective schema; declared execution
must stop until catalog authority exists.

The repeated live-law failures were valuable because they falsified harness assumptions while
leaving production checks intact. Direct leaf open had to emulate the engine-planned physical
hash explicitly; generic display strings were not temporal-value authority; and the pinned server
exposed narrow `Date`/`DateTime` storage through integer Arrow types. Promoting only those source
expressions preserved logical date/timestamp and cursor semantics without a connector-wide cast.
The project-level partial-stream law also showed that a syntactically plausible alias can fail at
DDL type unification before the intended fault path; keeping the UUID and delayed UInt8 fault as
separate columns made setup type-correct and the emit-then-error behavior auditable.

Five whys on the original cursor-limit defect: rows became unreachable because SQL `LIMIT` cut an
equal-cursor group; restart compared only `cursor > checkpoint`; the durable checkpoint has no
stable-key component; connector capability advertised limit support without asking whether the
frontier could represent truncation; and no project/package law exercised limit-induced partial
completion. The repair leaves the limit in generic engine authority and proves failed publication
and checkpoint atomicity followed by a complete unbounded resume. Future cursor adapters should
review frontier representability before advertising any source-side truncation.

The final frontier audit exposed the same authority lesson at two smaller boundaries. A physical
narrow integer and a durable cursor value are not the same domain: carrying Int8/16/32 directly
looked type-safe at the adapter but failed the shared accumulator's Int64/UInt64 contract. Making
the configured-field cast explicit in discovery metadata keeps the source width truthful and lets
every generated expression consume one canonical decision; leaving non-cursor fields untouched
prevents a convenient fix from becoming a leaky global normalization.

Likewise, retained allocator capacity is not useful payload. The first complete sweep timed valid
work but failed because the report compared capacities that may vary with buffer reuse and labeled
them useful bytes. The corrected harness separately records retained maxima, derives the exact
logical bytes from the fixed schema, and validates every value plus a batch-boundary-invariant
checksum on both timed paths. Five whys: equivalence failed because capacities differed; capacity
was compared because it was the available memory helper; that helper was reused because “Arrow
bytes” was overloaded; the report did not name logical versus retained bytes; and no deterministic
payload law challenged the label before the live sweep. Future connector benchmarks should define
useful, retained, and wire bytes independently before adding any performance threshold.
