Status: recorded
Created: 2026-08-04
Updated: 2026-08-04

# MongoDB source connector closure evidence

## Observation

The finite MongoDB 8.0+ source uses the official asynchronous raw-BSON cursor, preserves the
ratified BSON-to-Arrow meanings, carries current source-position authority, and remains bounded by
the shared execution host. Its generic source matrix completes package, destination receipt,
checkpoint, and replay laws. The final clean release roofline for revision `89786e35` clears the
required 0.90 ratio. Connector closure remains blocked only on ratifying the endpoint/egress
surface before the final connector certificate is meaningful.

## Procedure

- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-mongodb --all-targets` passed all 32 current
  unit tests, including residual cardinality, overlapping payload accounting,
  duplicate/dotted/nested BSON shape, and exact physical-reconciliation subtype evidence
  boundaries. The contract suite passed 99 tests with two ignored. The engine suite passed 238
  executable tests plus the updated package-identity golden in a focused rerun, with six
  release/performance tests ignored.
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
  `.10x/evidence/.storage/2026-08-04-mongodb-source-error-sites.tsv` classify all 295
  construction/direct-kind lines: 255 production and 40 test rows. The 47 production invariant
  rows are CDF or official-driver invariant failures; SDK and I/O failures retain typed
  provenance, retry delay, and redacted diagnostics. The file-list, classifier, and ledger SHA-256
  values are respectively `7878346fa5a01b9ebe941fb55fa6307042603af82dc0b095ce78e65ec3847ba9`,
  `f5a4a3e5a7b9ca1e31890e665e759a4989430a7fab1017c6b3274d69839fcd9f`, and
  `bdaf728d4fada57d089bc12115f80815fc00df83dc62ce62e96cbde703e2985f`.
- `.10x/evidence/.storage/2026-08-04-mongodb-source-roofline.json` records five samples over
  100,000 rows from clean fat-LTO revision `89786e35`. The selected 32,768-row batch and
  one-client pool produced a 111,340,625 ns CDF median versus 102,665,917 ns for the
  semantics-equivalent raw-driver baseline, ratio 0.922088. All six cells stayed below the 10%
  dispersion bound; three cells cleared the ratio gate and selection followed the recorded
  minimum-pool-then-fastest-passing policy. Rows, useful Arrow bytes, and content checksum matched
  the direct driver in every sample.
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
- The final `tools/certify-connector.py --kind source --id mongodb --core-impact` remains pending.
  Running it before the endpoint/egress semantic blocker is resolved would certify a surface that
  still changes under either valid decision.

## What this supports or challenges

This supports the source ticket's mapping, boundedness, live conformance, error-ownership, and
release-performance acceptance criteria. The live paths also challenged stale
query-first integration assumptions: synchronous SQL analysis could be called from an async host,
pinned contract commands needed schema hydration, manifest SQL security needed to admit SQL
whitespace, and metadata comparison needed map-order independence. Those faults were repaired at
their shared authorities without compatibility paths.

## Limits

The live evidence covers a local digest-pinned MongoDB Community 8.0.13 server, not Atlas. Atlas
was not exercised because no authenticated/cost-authorized Atlas fixture was provided. The source
is finite collection reading only; change streams and resume tokens are explicit non-goals. The
aggregate workspace run is not globally green because the explicitly separate ergonomics worker
owns three failures and local ClickHouse integration credentials were unavailable; neither limit
intersects the MongoDB source leaf or its executed generic source matrix. Closure nevertheless
remains blocked because the current advertised topology-discovery surface exceeds the egress
authority that can be enforced before connection.
