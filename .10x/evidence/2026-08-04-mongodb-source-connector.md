Status: recorded
Created: 2026-08-04
Updated: 2026-08-04

# MongoDB source connector closure evidence

## Observation

The finite MongoDB 8.0+ source uses the official asynchronous raw-BSON cursor, preserves the
ratified BSON-to-Arrow meanings, carries current source-position authority, and remains bounded by
the shared execution host. Its release roofline clears the required 0.90 ratio, and its generic
source matrix completes package, destination receipt, checkpoint, and replay laws.

## Procedure

- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-mongodb --lib --locked` passed all 22 current
  unit tests, including residual cardinality, duplicate/dotted/nested BSON shape, and exact
  preserved-projection subtype evidence boundaries. `cdf-kernel` passed all 84 unit tests, and the
  focused materialized-null evidence regression passed in `cdf-engine`.
- The digest-pinned MongoDB 8.0.13 live source matrix executed 15 supported destination/disposition
  cells and recorded three destination-sheet exclusions. Every executed cell verified its package,
  receipt-gated checkpoint, duplicate no-op replay, and fresh-artifact replay.
- The error inventory was regenerated from its frozen nine-file scope with this exact command:

  ```sh
  error_test_line=$(rg -n '^#\[cfg\(test\)\]' crates/cdf-source-mongodb/src/error.rs | cut -d: -f1)
  xargs -0 rg -n --no-heading -- 'CdfError::(new|transient|rate_limited|auth|contract|data|destination|environment|internal|from)|ErrorKind::(Transient|RateLimited|Auth|Contract|Data|Destination|Environment|Internal)|MongoErrorKind::[A-Za-z]+|mongodb::error::Error::custom|mongodb::error::ErrorKind::[A-Za-z]+' < .10x/evidence/.storage/2026-08-04-mongodb-source-error-files.nul | LC_ALL=C sort | awk -v error_test_line="$error_test_line" -f .10x/evidence/.storage/2026-08-04-mongodb-source-error-classify.awk > .10x/evidence/.storage/2026-08-04-mongodb-source-error-sites.tsv
  ```

  The frozen outputs in
  `.10x/evidence/.storage/2026-08-04-mongodb-source-error-files.nul` and
  `.10x/evidence/.storage/2026-08-04-mongodb-source-error-sites.tsv` classify all 234
  construction/direct-kind lines: 200 production and 34 test rows. The 31 production invariant
  rows are CDF or official-driver invariant failures; SDK and I/O failures retain typed
  provenance, retry delay, and redacted diagnostics. The file-list, classifier, and ledger SHA-256
  values are respectively `7878346fa5a01b9ebe941fb55fa6307042603af82dc0b095ce78e65ec3847ba9`,
  `90c0a77d0cfa3ef7530ed94d1a7255495c30ed605f8315f08452910e1a5b9a84`, and
  `2cbb6df78036e8309e5d973b16b9f2cf3ba4658a58245c852c1257d271ec09ca`.
- `.10x/evidence/.storage/2026-08-04-mongodb-source-roofline.json` records five samples over
  100,000 rows. The selected 65,536-row batch and one-client pool produced a 119,474,542 ns CDF
  median versus 108,078,000 ns for the semantics-equivalent raw-driver baseline, ratio 0.904611.
  All six sweep cells passed the ratio and dispersion gates.
- `DUCKDB_DOWNLOAD_LIB=1 ... cargo nextest run -p cdf-conformance --locked --no-fail-fast`
  passed all 92 executed tests (eight governed skips) after repairing current query-first
  integration defects exposed by the source certificate.
- `DUCKDB_DOWNLOAD_LIB=1 cargo nextest run --workspace --no-fail-fast` executed 2,219 tests in
  157.184 seconds: 2,214 passed, 52 were skipped, three failures belong to the separately active CLI
  ergonomics workstream, one failure requires an unavailable `CDF_CLICKHOUSE_ENDPOINT`, and the
  remaining stale source-position fixture expectation was repaired and passed with focused
  nextest run `423a70af-4e00-432c-b332-6e3e2c2afc61`.
- The current public CLI lifecycle passed live against MongoDB 8.0.13. It exercised `init`, dry-run
  and persisted `add`, discover, pin, compile, validate, plan, preview, doctor, run, and replay;
  checked decoded and URL-encoded credential redaction; and compared package layout/file inventory,
  segment identity/content, checkpoint positions, and receipt semantics across `--jobs 1` and
  `--jobs 4`.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace --all-targets --locked -- -D warnings` passed.
  The explicit cognitive-complexity diagnostic found no MongoDB source issue; its changed preview
  coordinator remains one cohesive 34/25 authority and is review input rather than a failed gate.
- First-party `jscpd` reported 10,376 duplicated lines out of 411,909 (2.52%), below the 10%
  threshold. `cargo machete --with-metadata` found no unused dependency. `graphify update .` was
  unavailable because the executable is not installed in this environment.
- The final `tools/certify-connector.py --kind source --id mongodb --core-impact` result is added
  below when the closure barrier completes.

## What this supports or challenges

This supports the source ticket's mapping, boundedness, live conformance, error-ownership, and
release-performance acceptance criteria. The live and certificate paths also challenged stale
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
intersects the MongoDB source leaf or its executed generic source matrix.
