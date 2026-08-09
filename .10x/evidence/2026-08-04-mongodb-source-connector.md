Status: recorded
Created: 2026-08-04
Updated: 2026-08-08

# MongoDB source connector closure evidence

## Observation

The finite MongoDB 7.0+ source uses the official asynchronous raw-BSON cursor, preserves the
ratified BSON-to-Arrow meanings, carries current source-position authority, and remains bounded by
the shared execution host. Its generic source matrix completes package, destination receipt,
checkpoint, and replay laws. An authorized Atlas 7.0.40 lifecycle additionally proves the current
IAM authentication, discovery, bounded full-replace, package, DuckDB receipt, checkpoint, and
verification path against real data. A fresh release lifecycle also proves default-depth discovery
keeps a top-level MongoDB document as opaque Canonical Extended JSON without producing governed
residual rows. The final clean release roofline for revision `89786e35` clears the required 0.90
ratio. A later real-Atlas throughput and portable-plan certificate reads the largest inspected
collection end to end, retains governed drift without quarantine, and proves bounded
collection-generation preflight before any run effect.

## Procedure

- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-mongodb --lib --locked` passed all 45 current
  unit tests. In addition to the existing residual-cardinality, overlapping-payload,
  duplicate/dotted/nested BSON, and physical-reconciliation boundaries, the suite now proves the
  default top-level-only schema, configured depth two, opaque Canonical Extended JSON decoding,
  heterogeneous sampled values, governed later primitive drift, and mixed-pin/homogeneous-
  observation stability. The contract suite previously passed 99 tests with two ignored. The
  engine suite previously passed 238 executable tests plus the updated package-identity golden in
  a focused rerun, with six release/performance tests ignored.
- The digest-pinned MongoDB 8.0.13 live source matrix executed 15 supported destination/disposition
  cells and recorded three destination-sheet exclusions. Every executed cell verified its package,
  receipt-gated checkpoint, duplicate no-op replay, and fresh-artifact replay.
- The error inventory was regenerated from its frozen nine-file scope with this exact command:

  ```sh
  error_test_line=$(rg -n '^#\[cfg\(test\)\]' crates/cdf-source-mongodb/src/error.rs | cut -d: -f1)
  xargs -0 rg -n --no-heading -- 'CdfError::(new|transient|rate_limited|auth|contract|data|destination|environment|internal|from)|ErrorKind::(Transient|RateLimited|Auth|Contract|Data|Destination|Environment|Internal)|std::io::Error::(other|from)|std::io::ErrorKind::[A-Za-z]+|Io::[A-Za-z]+|MongoErrorKind::[A-Za-z]+|mongodb::error::Error::custom|mongodb::error::ErrorKind::[A-Za-z]+' < .10x/evidence/.storage/2026-08-04-mongodb-source-error-files.nul | LC_ALL=C sort | awk -v error_test_line="$error_test_line" -f .10x/evidence/.storage/2026-08-04-mongodb-source-error-classify.awk > .10x/evidence/.storage/2026-08-04-mongodb-source-error-sites.tsv
  ```

  The frozen outputs in
  `.10x/evidence/.storage/2026-08-04-mongodb-source-error-files.nul` and
  `.10x/evidence/.storage/2026-08-04-mongodb-source-error-sites.tsv` classify all 320
  construction/direct-kind lines: 277 production and 43 test rows. The 50 production invariant
  rows are CDF or official-driver invariant failures; SDK and I/O failures retain typed
  provenance, retry delay, and redacted diagnostics. The file-list, classifier, and ledger SHA-256
  values are respectively `7878346fa5a01b9ebe941fb55fa6307042603af82dc0b095ce78e65ec3847ba9`,
  `f5a4a3e5a7b9ca1e31890e665e759a4989430a7fab1017c6b3274d69839fcd9f`, and
  `1375863483f3bca5db4ce980f8d9e19b50d89e8ce523d74e69ff158c5f79db9b`.
- Resource-scoped `schema_depth` now defaults to 1 and is carried through configuration schema,
  `cdf add`, redacted and physical plans, discovery candidate identity, source evidence, and
  observed-schema metadata. Depth is validated as `1..=32`. Boundary documents, arrays, and
  discovery-time heterogeneous values decode as deterministic Canonical Extended JSON UTF-8 under
  three exact MongoDB semantic tags; they remain subject to the existing structural and memory
  bounds. Later type changes against a primitive pin still produce governed residual evidence.
- The release binary at `8260bfb9` compiled the fresh sandbox resource
  `mongo_live.change_stream_collections_opaque` against Atlas 7.0.40 with `schema_depth` omitted.
  Compile established schema generation 1 with four retained fields; `startAfterToken` became
  nullable UTF-8 carrying physical `bson:document` and semantic
  `mongodb.document_extended_json@1`. Plan admitted one bounded full replacement. Run completed in
  936 ms with 25 rows and one 5.6 KiB segment. `cdf package verify` accepted all 18 files for
  package `sha256:643320de7c56ab1e677c179af9ad138e88ac2f4239c9b03b50509368b0131f39`.
  Direct read-only DuckDB aggregates found 25 rows, 25 distinct ObjectIds, seven non-null opaque
  values, seven JSON-valid values, zero invalid JSON values, and zero `_cdf_variant` rows. The
  procedure inspected only aggregate counts and schema metadata, not document values.
- The first post-change connector certificate correctly rejected the stale built-in catalog
  fixture after the MongoDB option-schema identity changed; refreshing only the two MongoDB hashes
  made the catalog integrity check pass. The certificate then exposed a pre-existing keyed-package
  defect: merge execution finalized `KeyedChanges` content while its destination commit preimage
  still stamped ordinary `Rows` content. The package builder now supplies its finalized content
  authority to the commit preimage. Focused DuckDB and Postgres drift/quarantine runs and a merge
  dedup run plus artifact-only replay pass with exact keyed intent, canonical key order, receipt,
  and checkpoint evidence.
- `DUCKDB_DOWNLOAD_LIB=1 cargo nextest run -p cdf-conformance --locked --no-fail-fast` now executes
  all 92 selected tests: 91 pass and one pre-existing MVP CLI demo fails because `cdf plan` attempts
  live discovery against the intentionally synthetic `api.github.test` endpoint. A detached
  `cf37cc5c` worktree reproduces that exact failure before the MongoDB depth implementation. The
  same worktree also reproduced the keyed-package failure before its repair. Thus the connector
  certificate's leaf, catalog, and 91 local conformance laws pass; its aggregate verdict remains
  failed solely on the separately owned CLI fixture/contact mismatch.
- The 2026-08-08 `MONGODB-AWS` addition introduced only caller-contract constructions in the
  existing frozen source scope. Focused tests prove URI credential/session-token splitting,
  secret-safe proposal rendering, `$external` validation, and portable-plan retention of secret
  references. The SDK authentication variant continues through the existing central classifier as
  `Auth`; no raw credential value is incorporated into CDF diagnostics.
- The user-ratified 7.0 floor changes no SDK/error construction. One new test assertion proves
  7.0 admission and that an older server remains a caller-repairable `Contract` failure with the
  observed major version and 7.0 remediation boundary.
- The authorized Atlas core cluster reported 7.0.40. The sandbox resource selected the real
  `floqast-fq12.changeStreamCollections` collection without recording document values. Compile
  established state-backed schema generation 1 with ObjectId, string, and governed variant output.
  Plan selected one bounded full-replace partition. Debug execution produced and verified package
  `sha256:c67ee6a4e7e1a6bf8647281e4f33077c1b46e2efc3dba5b7dcc4f1c539cba596`:
  one 25-row/5,210-byte segment, 18 identity files, a DuckDB receipt for 25 inserts, and final
  `checkpointed` lifecycle. A direct DuckDB aggregate returned 25 rows, 25 distinct IDs, and 25
  non-null governed variant rows. The full-scan checkpoint is a deterministic foreign-state
  completion identity over the non-secret scan authority; it does not claim MongoDB transaction
  snapshot isolation or resumable incrementality.
- A subsequent release-binary preflight against that committed checkpoint exposed a lifecycle
  defect: generic resume binding copied the full-scan completion identity into the next scan as if
  it were a cursor. MongoDB now source-owns resume binding. Cursor resources retain their exact
  cursor behavior; a cursorless full replacement validates the MongoDB completion protocol and
  deliberately reopens from the beginning. Focused tests and strict connector Clippy passed. A
  second debug execution against the existing sandbox state completed a fresh 25-row replacement,
  verified package
  `sha256:78bb0b02ca48ff4d17f94d00c883a042ef84f8fc5e1149fb9fbee837b5f0a62e`,
  and left the DuckDB target at 25 rows, 25 distinct IDs, and 25 governed variant rows.
- The requested release-binary rerun at revision `db895465` then completed against the same
  persisted checkpoint and live Atlas 7.0.40 collection in 3 seconds. It read 25 documents into
  one 5.1 KiB segment, committed and checkpointed package
  `sha256:ee47de42518efbdf3fad43b9b08ee6ec9feefdcc93a3a9a391b1872d52c3ab92`,
  and passed `cdf package verify` over all 18 identity files. The direct DuckDB aggregate again
  returned 25 rows, 25 distinct IDs, and 25 non-null governed variant rows. The three Atlas IAM
  secret files remained owner-readable only (`0600`).
- `.10x/evidence/.storage/2026-08-04-mongodb-source-roofline.json` records five samples over
  100,000 rows from clean fat-LTO revision `89786e35`. The selected 32,768-row batch and
  one-client pool produced a 111,340,625 ns CDF median versus 102,665,917 ns for the
  semantics-equivalent raw-driver baseline, ratio 0.922088. All six cells stayed below the 10%
  dispersion bound; three cells cleared the ratio gate and selection followed the recorded
  minimum-pool-then-fastest-passing policy. Rows, useful Arrow bytes, and content checksum matched
  the direct driver in every sample.
- Release revision `d4ddde9a` read the authorized Atlas 7.0.40 `depreciation-items` collection with
  the exact 21-field projection and an 8,192-row wire cursor independent from adaptive decode/output
  batches. It extracted all 417,114 documents in 21.1 seconds (19.7k documents/s), completed the
  DuckDB replace lifecycle in 21.88 seconds, quarantined zero rows, and retained 250,610 rows with
  governed variant evidence. User CPU was 7.18 seconds, system CPU 2.50 seconds, and maximum RSS
  438,124,544 bytes. The prior 1,000-row CDF wire request took 180 seconds for the same row set; a
  controlled 40-row request had measured 6.6x slower than 1,000 in the direct client. Live mongosh
  sweeps rejected oversized 32,768 and 100,000 requests and selected 8,192 as the remote operating
  point, although full-run latency varied materially with Atlas/network state.
- Final release revision `9009a1bb` (binary SHA-256
  `911b3467906bf6818e7fe8092855286943746eb021948118b0339cb3c26d508a`) compiled a fresh
  `atlas_throughput.depreciation_items_portable` resource, established schema generation 1, and
  exported canonical plan
  `sha256:06c93654e9f3f4dacdb2c043b1a9bbe7702b7e5d0ffa90e30b53048c59439438`.
  `cdf run --plan` first re-read the hashed collection UUID/collation/validator generation on the
  managed I/O runtime, passed whole-plan preflight, then loaded all 417,114 rows. This slower remote
  sample took 124.8 seconds of package execution / 129.74 seconds wall time but only 9.78 seconds of
  host CPU, further isolating Atlas/network wait variance rather than local decode saturation.
  Direct DuckDB aggregates returned 417,114 rows, 417,114 distinct ids, and 250,610 non-null
  `_cdf_variant` rows. Package inspection verified manifest/ledger hash
  `sha256:e3b5a1b35c068b645ba5fb8b697a2ef81f5cc89f80fcbfbb103aafc246f29060`, one segment,
  available receipt, committed checkpoint, and `checkpointed` lifecycle.
- `DUCKDB_DOWNLOAD_LIB=1 ... cargo nextest run -p cdf-conformance --locked --no-fail-fast`
  passed all 92 executed tests (eight governed skips) after repairing current query-first
  integration defects exposed by the source certificate.
- `DUCKDB_DOWNLOAD_LIB=1 cargo nextest run --workspace --no-fail-fast` executed 2,219 tests in
  157.184 seconds: 2,214 passed, 52 were skipped, three failures belong to the separately active CLI
  ergonomics workstream, one failure requires an unavailable `CDF_CLICKHOUSE_ENDPOINT`, and the
  remaining stale source-position fixture expectation was repaired and passed with focused
  nextest run `423a70af-4e00-432c-b332-6e3e2c2afc61`.
- The current authenticated public CLI lifecycle passed live against MongoDB 8.0.13. It exercised `init`, dry-run
  and persisted `add`, discover, pin, compile, validate, plan, preview, doctor, run, and replay;
  kept secret references active across every contact-bearing command, injected a BSON Int32 cursor
  value after pinning BSON Int64, and proved the separately persisted physical reconciliation did
  not populate `_cdf_variant`. It compared exact file content or normalized invocation-specific
  semantics, segment identity/content, checkpoint positions, receipt transaction values, and
  verify parameters across `--jobs 1` and `--jobs 4`.
- Strict Clippy passed for `cdf-kernel`, `cdf-source-mongodb`, `cdf-engine`, `cdf-conformance`, and
  `cdf-benchmarks`. The explicit cognitive-complexity diagnostic added no finding in the MongoDB
  repair; the previously known preview coordinator remains 34/25 and is review input rather than a
  failed gate.
- First-party `jscpd` reported 10,376 duplicated lines out of 411,909 (2.52%), below the 10%
  threshold. `cargo machete --with-metadata` found no unused dependency. `graphify update .` was
  unavailable because the executable is not installed in this environment.
- Three independent final rereviews passed the neutral materialization authority, runtime
  materialization/evidence path, and MongoDB decoder. The performance repair rereview found one
  overlapping-source payload undercount; the repair routes every equality/prefix overlap and all
  Lists to exact estimation, adds a 36 MiB duplicate-payload boundary test, and passed rereview.
- `tools/certify-connector.py --kind source --id mongodb --core-impact` passes format, all 42
  connector leaf laws, and built-in catalog integrity. General conformance passes 91/92; the
  remaining synthetic REST contact failure predates this change and is recorded above. The former
  endpoint/egress semantic blocker is resolved by the accepted residual-risk decision cited in the
  owning ticket.

## What this supports or challenges

This supports the source ticket's mapping, boundedness, live conformance, error-ownership, and
release-performance acceptance criteria. The live paths also challenged stale
query-first integration assumptions: synchronous SQL analysis could be called from an async host,
pinned contract commands needed schema hydration, manifest SQL security needed to admit SQL
whitespace, and metadata comparison needed map-order independence. Those faults were repaired at
their shared authorities without compatibility paths.

## Limits

The live evidence covers both a local digest-pinned MongoDB Community 8.0.13 server and authorized
Atlas 7.0.40 collections, including one 417,114-document throughput and portability lifecycle. The
remote timing samples vary from 21.1 to 124.8 seconds with nearly constant host CPU, so they prove a
credible high-throughput operating point and lack of local CPU saturation but not a stable Atlas
service-level floor. They do not claim cross-shard transaction-snapshot isolation. The source is
finite collection reading only; change streams and resume tokens are explicit non-goals. The
aggregate workspace run is not globally green because the explicitly separate ergonomics worker
owns three failures and local ClickHouse integration credentials were unavailable; neither limit
intersects the MongoDB source leaf or its executed generic source matrix.
