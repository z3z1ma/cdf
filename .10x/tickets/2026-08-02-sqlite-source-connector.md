Status: active
Created: 2026-08-02
Updated: 2026-08-08
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md

# SQLite source connector

## Scope

Implement and ship `cdf-source-sqlite` under the SQLite table-source contract. Add only the minimal
protocol-sharing surface justified by the paired destination. Enroll the driver through
`cdf-builtin-drivers`, add data-driven conformance/live fixtures, operator documentation, and a
release-mode direct-`rusqlite` source roofline cell. Complete it with native read queries and
resource-scoped discovery, output, busy, cache, and mmap controls. In the same tranche, correct the
shipped Postgres source kind from the broad `sql` category to the concrete `postgres` identity and repair
all Postgres-owned configuration, fixture, example, discovery, generated-artifact, and conformance
uses. Do not mechanically rewrite synthetic/generic `sql` test values that are not Postgres
identity.

## Non-goals

Destination writes, CDC/triggers/session changesets, persistent pragma changes,
network-filesystem support, parallel-connection snapshot claims, and generic dialect multiplexing.

## Acceptance Criteria

- Source configuration, discovery, add, compile, portability, health, preview, and run follow
  `.10x/specs/sqlite-table-source.md` and `.10x/specs/sqlite-native-query-source.md` with exact
  redaction and error ownership.
- SQLite owns source kind `sqlite`; Postgres owns source kind `postgres`; no first-party driver owns
  the broad kind `sql`; and every identity-bearing Postgres/SQLite artifact agrees without an alias.
- Read-only transaction streaming, schema freeze/drift handling, exact/inexact pushdown, stable
  cursor tie-breaking, and explicit temporal cursor encodings have unit and live tests.
- The source uses one accounted blocking lane and bounded Arrow builders with cancellation and no
  private executor/pool.
- Built-in catalog integrity, generic source matrix, jobs invariance, package/replay/checkpoint
  laws, and `tools/certify-connector.py --kind source --id sqlite --core-impact` pass.
- The source macro benchmark records raw samples and reaches the 0.90 direct-`rusqlite` roofline.
- An independent reviewer records a pass or every concern is repaired and re-reviewed.

## References

- `.10x/specs/sqlite-table-source.md`
- `.10x/specs/sqlite-native-query-source.md`
- `.10x/decisions/connector-native-capability-before-commons.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/decisions/database-source-kind-identity.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/research/2026-08-02-sqlite-clickhouse-mongodb-connector-shaping.md`
- `docs/connector-authoring.md`

## Assumptions

- Finite snapshot plus numeric/timestamp/date cursor behavior is user-ratified.
- Table and native read-query resources are user-ratified adapter-owned surfaces; a generic SQL
  source grammar remains excluded.
- The bundled SQLite 3.53.2 is above the WAL-reset fix and is record-backed by the research.
- Unique `sqlite` and `postgres` source kinds were explicitly user-ratified on 2026-08-02.

## Journal

- 2026-08-08: The user superseded table-only authoring and rejected a universal connector grammar.
  This ticket now owns SQLite-native read queries and connection-local resource controls under
  `.10x/specs/sqlite-native-query-source.md`; read-only statement proof and snapshot semantics
  remain mandatory.

- 2026-08-02: Executable ticket opened from the ratified source and roofline contracts.
- 2026-08-02: Execution started. Read this ticket, its parent, every direct reference, the source
  spec's governing resource/checkpoint authorities, the roofline's performance authorities, the
  extension invariant's runtime/concurrency/product guidance, and the error-ownership audit
  procedure. Scope remains the SQLite source only; no destination or later connector work is
  authorized. The orchestration instruction forbids graph tooling, commits, and pushes.
- 2026-08-02: The SQLite/rusqlite boundary is in the mandatory error audit scope. Before writing
  product code, the executor will inventory the selected source archetype, catalog/conformance
  enrollment surfaces, existing SQLite dependency authority, and benchmark/report conventions.
- 2026-08-02: Implemented the isolated leaf through its first compile/test milestone: validated
  identifiers, read-only catalog discovery, normalized local locators and portable-plan rejection,
  a one-lane read-only transaction stream with bounded/accounted Arrow builders and cancellation,
  schema-drift/type checks, exact/inexact predicate classification, cursor stable-key validation,
  explicit temporal encodings, typed rusqlite error ownership, and contact-free compile/resolve
  surfaces. The leaf check passes and 11 unit tests pass.
- 2026-08-02: Catalog enrollment exposed a governing-contract conflict. The active source runtime
  contract requires registration to reject duplicate source kinds, `SourceRegistry::register`
  enforces that rule, and the shipped Postgres driver already owns kind `sql`. The active SQLite
  source spec independently requires driver id `sqlite` and kind `sql`. Registering the new driver
  in `cdf-builtin-drivers` therefore fails exactly with "Contract: source kind 'sql' is already
  registered." No executor-safe implementation can satisfy all three authorities: changing the
  SQLite kind, changing Postgres, or adding generic dialect multiplexing each changes a ratified
  public/configuration or registry semantic.
- 2026-08-02: User ratified the recommended unique SQLite kind and expanded this same tranche to
  remove Postgres ownership of `sql`. Governing decision
  `.10x/decisions/database-source-kind-identity.md` now fixes `sqlite` and `postgres` as their
  concrete kinds, extends the same rule to the already-planned ClickHouse/MongoDB specs, preserves
  duplicate-kind rejection, and rejects aliases/dialect multiplexing. The blocker is resolved;
  ticket returned to open for a fresh executor turn.
- 2026-08-02: Execution resumed after reading the complete updated ticket, active identity
  decision, and amended SQLite/runtime/roofline specifications. The resolved selector contract is
  concrete and executable: SQLite owns `sqlite`, Postgres owns `postgres`, and generic/synthetic
  uses of `sql` remain untouched unless source authority proves they identify Postgres.
- 2026-08-02: Completed the concrete-kind migration across both driver descriptors, add proposals,
  discovery identity, catalog identity, Postgres-owned CLI/project fixtures, the Postgres example,
  and the source run-matrix catalog. The generic `sql` conformance archetype/file was renamed to
  `postgres`; a distinct `sqlite` archetype now creates a strict local table and resolves the
  production SQLite driver through the shared runtime. SQL command names, synthetic receipt kinds,
  and the existing private Postgres partition label remain intentionally descriptive rather than
  source-driver ownership.
- 2026-08-02: Validation milestone: an offline all-target check of `cdf-source-sqlite`,
  `cdf-source-postgres`, and `cdf-conformance` passed. The SQLite and Postgres source unit suites
  (11 and 12 tests) and the built-in catalog suite (3 tests) passed with the regenerated catalog
  hashes. The first SQLite run-matrix attempt reached only the harness bootstrap and was denied a
  localhost bind by the filesystem/network sandbox; an escalated rerun was interrupted before it
  returned a result, so the matrix criterion remains open rather than inferred.
- 2026-08-02: Added focused production-path tests before retrying the broad harness. Under a
  bounded 120-second command, all 15 SQLite source tests completed in 3.19 seconds. The four new
  live cases prove equal-cursor stable-key ordering, explicit temporal decode/checkpoint units,
  dynamic SQLite storage-class drift classified as Data, and one read snapshot across batches
  while a WAL writer commits a change. This rules out a SQLite producer/termination hang.
- 2026-08-02: The first valid release roofline measured 0.755 (CDF 141.033 ms median versus direct
  `rusqlite` 106.417 ms) with low variance. Hoisting invariant temporal metadata lookup and schema
  hashing improved the unchanged 1M-row cell to 0.891. Increasing the bounded row fetch target from
  8,192 to 32,768 on both CDF and the direct comparator removed enough fixed per-batch governance
  cost to pass at 0.926233: CDF median 117.089 ms, direct median 108.451 ms, MAD 1.548/1.266 ms.
  The byte ceilings and 256-row cancellation cadence did not change.
- 2026-08-02: The macOS sandbox rejects `/usr/bin/time -l` after child execution because its
  `kern.clockrate` sysctl is forbidden. The benchmark observer now recognizes only that exact
  failure and falls back to bounded monotonic child timing plus the safe `nix::getrusage` wrapper.
  The final five raw samples include CPU and peak RSS counters (CDF CPU 128.8--130.2 ms, cumulative
  child peak RSS 27,508,736 bytes), so missing counters do not launder the roofline into a pass.
- 2026-08-02: Completed the mandatory rusqlite error-ownership audit over an exact six-file Rust
  manifest and 96-site constructor ledger. The review corrected Arrow `RecordBatch` assembly from
  Data to Internal, re-statted `SQLITE_CANTOPEN` paths to preserve missing-file versus host-I/O
  ownership, and stopped rusqlite `InvalidPath` from forwarding its path-bearing Display. Sixteen
  focused tests now cover typed direct/nested wrapper preservation, a 275 ms retry delay, raw I/O
  ownership splits, and path redaction. Durable evidence is
  `.10x/evidence/2026-08-02-sqlite-source-error-ownership.md`.
- 2026-08-02: The final naming/layout pass removed category-level names from concrete Postgres
  source fixtures (`resources/postgres.toml`, `postgres-dsn`, Postgres-named helpers/tests), while
  preserving the generic `cdf sql` command, synthetic SQL receipt vocabulary, and the existing
  private Postgres partition discriminator. Strict Clippy then exposed an unused public
  eight-argument SQLite open function; the function is now a private three-argument execution
  helper receiving one validated resource object, removing the only leaky construction surface.
- 2026-08-02: Strict all-target/all-feature Clippy passed for the SQLite/Postgres source,
  built-in-driver, conformance, benchmark, project, and CLI packages. Formatting and diff checks
  passed. The source catalog/shard integrity test and the generic no-destination-branch guard pass.
- 2026-08-02: The bounded core-impact certificate passed formatting, all 16 connector leaf laws,
  and built-in catalog integrity. General conformance then reported two failures: a generic guard
  false positive caused by concrete database-source convenience methods in the generic matrix
  model, and the sandbox's denial of local Postgres port allocation. The convenience methods were
  removed and both the focused guard and source-catalog tests pass. Per the orchestration boundary,
  the broad certificate was not rerun; its only remaining unobserved gate requires a host that can
  bind local Postgres. No certificate pass or independent review is inferred.
- 2026-08-02: Closure repair resumed after reading the complete independent Review. An escalated
  certificate attempt was aborted at the tool boundary after 654.5 seconds and produced no live
  session handle; it is non-closure evidence. Scope is exactly the seven reviewed findings:
  limit/residual/cursor semantics, stable-key uniqueness, pre-copy bounds and VM cancellation,
  truthful capabilities/type policy, roofline measurement authority, private coherent modules,
  and a post-repair certificate. SQLite destination and later connectors remain excluded.
- 2026-08-02: Repaired findings 1--4 without widening the source contract. SQLite no longer
  compiles or executes SQL `LIMIT`; the shared engine remains the sole post-residual/window limit
  authority. Catalog discovery now retains conservative single-column primary-key/nonpartial
  unique evidence, compiled cursor resources require that evidence, and execution revalidates the
  live constraint before opening its snapshot. Variable-cell and cumulative-byte bounds run
  before Arrow append, and a rusqlite VM progress hook observes run cancellation every 8,192
  operations. Snapshot execution capabilities no longer claim resume/canonical order, while
  cursor resources do. Dynamic storage conversion now consumes the compiled coercion/lossy
  allowances. The focused suite passes 23/23, including the new adversarial assertions.
- 2026-08-02: Repaired the public/module boundary. The crate root exports only
  `SqliteSourceDriver`; catalog, identifiers, resources, capabilities, query helpers, temporal
  codecs, and construction are private or crate-private. Driver resolution has one canonical
  constructor that installs compiled schema/type-policy/evidence and execution services together.
  The former 2,686-line source monolith is now `source.rs` (resource contract/support),
  `source/query.rs`, `source/execution.rs`, `source/temporal.rs`, and `source/tests.rs`; their line
  counts are 828/403/544/251/673. The split compiles and preserves all 23 tests.
- 2026-08-02: Repaired roofline measurement authority and replaced the raw artifact with report
  schema v2. The provider identity is fixed before sampling; every isolated worker supplies final
  `RUSAGE_SELF` CPU and peak-RSS counters; physical read bytes are explicitly unavailable/zero and
  database length is fixture metadata only. Comparability records base Git revision
  `5a0169e5ba5b6f7e73aebb69f02d04b28ec3267f`, workspace-content digest
  `sha256:4554dcd4fa171d040982be2881932c6f6194a7fd014e123add379598b9b73591` over 16 enumerated
  inputs, and executable digest
  `sha256:75505432af896bd2e175efd0eb43200a8ee3e982be3dd6121e76a22346004c5d`. The required five
  1M-row samples per cell pass at 0.915959: CDF median 208,295,060 useful B/s versus direct
  227,406,511, with low MAD, complete CPU/RSS counters, and zero spill.
- 2026-08-02: Regenerated the mandatory error audit after the module split. The exact manifest is
  ten Rust files and the exact ledger is 98 constructor sites: 55 Contract, 34 Data, six Internal,
  two classified-boundary `new`, and one fixture RateLimited; 96 are production and two are typed
  regression fixtures. The SQLite VM interruption has explicit run-cancellation ownership. The
  durable audit record and operator documentation now match the repaired implementation and v2
  roofline limits.
- 2026-08-02: Fresh affected-package validation passed without repeating unrelated workspace
  suites: 38 SQLite/Postgres/built-in library tests; all-target checks for the seven affected
  packages; strict all-target/all-feature Clippy for the same packages; formatting; diff hygiene;
  source-catalog shard coverage; and the generic no-destination-identity-branch guard. One focused
  conformance invocation omitted the established `DUCKDB_DOWNLOAD_LIB=1` setting and failed only
  at link time on `-lduckdb`; the corrected bounded invocations passed both exact assertions.
- 2026-08-02: The first post-repair escalated certificate tool call was interrupted after 403.2
  seconds before returning output. Read-only inspection proved that
  `target/quality/connector-sqlite-core-impact.json` remained the old 07:59 failed report; no fresh
  verdict was written. It is non-evidence and the one bounded final rerun remains pending.
- 2026-08-02: Repaired two integration defects exposed by the fresh certificate and a real-path
  regression. Cursor uniqueness is now owned by the correct phases: contact-free compilation
  validates the declared cursor/stable-key shape without inventing catalog evidence, discovery
  preserves `cdf:sqlite_unique=true`, and execution rejects a live table whose stable key is no
  longer a single-column primary key or nonpartial unique constraint. The SQLite execution
  boundary also stopped relabeling normalized output as its physical observation. With effective
  schema evidence, it now re-observes the catalog inside the same read transaction, requires the
  full live hash/schema to equal the verified observation, projects that physical schema through
  the compiled logical-to-source mapping, requires the projected hash to equal the partition's
  planned authority, and records that physical schema on the materialized batch.
- 2026-08-02: Focused post-integration evidence passed. The SQLite source library passed 23/23.
  `run_matrix::sqlite_fixture::tests::discovered_primary_key_proof_survives_compilation_and_preview`
  passed against a real primary-key database and the production discovery, compilation,
  resolution, engine planning, and preview path. The first exact parity attempt was sandbox-denied
  only when local Postgres tried to bind. The orchestrator's escalated rerun of
  `run_matrix::data_onramp::p2_preview_run_parity_law_covers_supported_archetypes` passed 1/1 in
  9.32 seconds. No certificate had been launched at that checkpoint.
- 2026-08-02: The orchestrator-observed full core-impact certificate passed formatting, leaf laws,
  built-in catalog, all 98 general-conformance assertions, the selected SQLite source run matrix,
  and the remaining source-extension phases. Its workspace core profile then reported 1,597
  passes and one failure: the closed workspace-safety assertion still expected 52 members after
  `cdf-source-sqlite` made the exact workspace count 53. This is not a connector or behavioral
  failure. The count was updated to 53 without changing the separate 52-unsafe-block inventory.
  The first focused invocation omitted `DUCKDB_DOWNLOAD_LIB=1` and stopped at link time without
  executing a test; the corrected exact workspace-safety assertion passed 1/1 with 269 filtered
  out. The orchestrator owns the final certificate rerun through its existing live workflow.
- 2026-08-02: Refreshed the current-source error inventory after the compile/physical-observation
  repairs. The ten-file manifest remains exact; the ledger now exactly reconciles 109 constructor
  sites: 58 Contract, 42 Data, six Internal, two classified-boundary `new`, and one fixture
  RateLimited. Of those, 107 are production and two are typed regression fixtures. The 11 added
  sites make compile-shape and live catalog/physical-schema drift ownership explicit; direct
  file/line/constructor/source comparison found no missing or extra row.
- 2026-08-02: Repeated the required release five-by-1M-row roofline against the current 16-input
  implementation. Report schema v2 passes at 0.907516 (907,516 ppm): CDF/direct median useful
  throughput is 207,300,206/228,425,851 B/s and median wall time is 115.774/105.067 ms. The current
  workspace-content identity is
  `sha256:f7bea87139ce077376c358056f78f75d173d09c853150e08f096bcbde99d80a2`; the executable
  identity is `sha256:631680eabe8d291a06dae6f0a999c67d037bab4d50c90db1b800b1ed3c8f63a3`.
  Every sample retains CPU/RSS evidence, physical bytes remain explicitly unavailable/zero, and
  spill remains zero.
- 2026-08-02: The orchestrator-observed certificate rerun passed every phase through the
  core-regression profile, including 2,108/2,108 workspace tests, then failed only workspace
  Clippy after 8.5 seconds; wrapper truncation hid the diagnostic. The exact cached workspace,
  all-target, all-feature, offline `-D warnings` command exposed one `needless_borrow` in the new
  catalog helper. Removing only the redundant reference made that exact Clippy gate pass in 5.73
  seconds. No error site or line changed.
- 2026-08-02: Because the one-line lint repair changed an enumerated SQLite source input, repeated
  the required release five-by-1M-row roofline again. Report schema v2 passes at 0.917027 (917,027
  ppm): CDF/direct median useful throughput is 204,370,839/222,862,262 B/s and median wall time is
  117.434/107.690 ms. The current workspace-content identity is
  `sha256:9cb8b6b5008790087b1bc2693fc6e1985a72b9454db319b26d3f70a1d2aab141`; the optimized
  executable remains `sha256:631680eabe8d291a06dae6f0a999c67d037bab4d50c90db1b800b1ed3c8f63a3`,
  independently matching `shasum`. All ten samples retain CPU/RSS evidence and zero spill; physical
  bytes remain explicitly unavailable/zero.
- 2026-08-02: The final orchestrator-observed core-impact certificate passed. Report
  `target/quality/connector-sqlite-core-impact.json` records verdict `passed`, started
  `2026-08-02T17:02:19Z`, and finished `2026-08-02T17:06:07Z`. All eight checks passed: format
  (2.051s), SQLite leaf laws (23 tests, 5.325s), built-in catalog (5.654s), general conformance (98
  tests, 24.639s), selected SQLite source matrix (14.845s), source-extension policy (1.057s),
  workspace core regression (2,108 tests, 172.828s), and workspace Clippy (1.382s).
- 2026-08-02: Repaired both significant findings from the fresh closure re-review. Declared and
  discovered execution now observe the live SQLite catalog inside the same read transaction and
  project that physical schema through the compiled logical-to-source mapping; declared resources
  no longer substitute logical output as physical evidence, while discovered resources retain the
  full and projected pinned-hash checks. A live declared-schema drift assertion proves the physical
  header changes from Utf8 to Int64 while the permitted logical Int64 output remains stable.
- 2026-08-02: Replaced the bidirectional wildcard module graph with an explicit directional DAG.
  `source.rs` names only the child symbols it owns; production children have explicit external,
  crate, and sibling imports; shared schema policy and temporal encoding ownership moved to
  `source/schema.rs`; and no production child imports its parent wholesale. The exact structural
  guard rejects parent child-globs, child `use super::*`, schema back-edges, temporal-to-query
  edges, and query-to-execution edges.
- 2026-08-02: Focused correctness evidence passed under the reduced validation cadence. The SQLite
  leaf suite passed 25/25. The exact declared-schema generic-engine preview passed with a live TEXT
  primary key, declared Int64 output, explicit coercion, lossy mapping disabled, two rows, and zero
  quarantine/residual rows. The existing discovered-schema preview and exact production-module
  boundary guard each passed 1/1. All-target checks and strict all-target/all-feature Clippy passed
  for the seven affected packages. No workspace-wide suite or certificate was rerun.
- 2026-08-02: Refreshed the mandatory error ledger after the schema-module split. The manifest is
  exactly eleven Rust files and the ledger exactly reconciles 109 constructor sites by
  file/line/constructor/source: 58 Contract, 42 Data, six Internal, two classified-boundary `new`,
  and one RateLimited fixture; 107 are production and two are typed-preservation fixtures.
- 2026-08-02: The required current 32K-row-batch roofline was observed twice and did not pass. The
  first run measured 0.873142 (CDF/direct medians 117.600/102.682 ms); the bounded repeat measured
  0.884562 (114.612/101.382 ms). Both used 17 enumerated inputs with content identity
  `sha256:30d679b6805f16c978c207ff731482cc1a4d0e89ab63b8b8ee49f569c2cd86f1` and executable
  identity `sha256:eb4d5949817e87ab4bca6aff6a9f9cb8018123c329bc27c35e197b4a2ee7a464`, complete CPU/RSS
  counters, low MAD, and zero spill. One bounded 64K diagnostic also failed at 0.889339
  (116.602/103.699 ms; content `sha256:f44215b91f91835eaa3063b8555b05db4b3743184f14914b4c78a767e922e285`, executable
  `sha256:73d5e8f7aaf8df2fc4546ef6cc44fa2c022398359e37e7efc55ee77b43845878`) and was reverted
  because it neither cleared the gate nor improved memory use. The prior 0.917027 pass and full
  certificate are baseline evidence only. Per the user's reduced cadence, one fresh current
  roofline measurement and one fresh workspace/core-impact certificate are deferred to parent
  final integration rather than repeated here.

## Blockers

Implementation blockers are resolved and the focused independent repair re-review passes. The
latest current-source roofline observations remain below the 0.900 gate; one fresh measurement is
deferred to parent final integration together with the fresh workspace/core-impact certificate.
The earlier 0.917027 roofline and eight-phase certificate remain baseline evidence, not current
closure evidence. This ticket stays active until those parent-owned gates resolve.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo check -p cdf-source-sqlite --all-targets --locked -j 12`: passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-sqlite --locked -j 12`: 11 passed, 0 failed.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-builtin-drivers
  tests::catalog_matches_the_data_driven_first_party_fixture --locked -j 12 -- --exact
  --nocapture`: fails before the assertion with "source kind 'sql' is already registered." This is
  direct evidence of the active authority conflict, not a test or implementation defect to bypass.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo check -p cdf-source-sqlite
  -p cdf-source-postgres -p cdf-conformance --all-targets --offline -j 12`: passed after the
  dependency graph was aligned on the workspace's `rusqlite` 0.40.1 line.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-source-sqlite
  -p cdf-source-postgres -p cdf-builtin-drivers --lib --offline -j 12`: 26 passed, 0 failed
  (11 SQLite, 12 Postgres, 3 built-in catalog).
- `CDF_RUN_MATRIX_SOURCE=sqlite DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test
  -p cdf-conformance --lib --offline -j 12 registered_source_shard_cells_persist_output --
  --ignored --nocapture`: sandboxed attempt failed before any cell at local Postgres port allocation
  with `Operation not permitted`; proves only that the harness requires an escalated localhost bind.
- `timeout 120s env CARGO_BUILD_JOBS=12 cargo test -p cdf-source-sqlite --lib --offline -j 12
  -- --nocapture`: 15 passed, 0 failed in 3.19 seconds; includes four production stream live cases.
- `timeout 300s env DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12
  CDF_SQLITE_ROOFLINE_SAMPLES=5 CDF_SQLITE_ROOFLINE_ROWS=1000000 cargo run --release --offline
  -p cdf-benchmarks --bin sqlite-source-roofline -j 12`: passed with ratio 0.926233. Raw samples,
  medians, MAD, CPU, peak RSS, row/useful/file bytes, versions, settings, bounds, comparability key,
  and semantic bias are stored at
  `.10x/evidence/.storage/2026-08-02-sqlite-source-roofline.json`.
- `timeout 120s env CARGO_BUILD_JOBS=12 cargo test -p cdf-source-sqlite --lib --offline -j 12
  -- --nocapture`: 16 passed, 0 failed after the final error-redaction and public-surface repairs.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-source-sqlite
  -p cdf-source-postgres -p cdf-builtin-drivers --lib --offline -j 12`: 31 passed, 0 failed
  (16 SQLite, 12 Postgres, 3 built-in catalog).
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo check -p cdf-source-sqlite
  -p cdf-source-postgres -p cdf-builtin-drivers -p cdf-conformance -p cdf-benchmarks
  -p cdf-project -p cdf-cli --all-targets --offline -j 12`: passed.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-sqlite
  -p cdf-source-postgres -p cdf-builtin-drivers -p cdf-conformance -p cdf-benchmarks
  -p cdf-project -p cdf-cli --all-targets --all-features --offline -j 12 -- -D warnings`: passed.
- `cargo test -p cdf-conformance --lib --offline
  run_matrix::tests::registered_run_matrix_shards_cover_source_catalog -- --exact`: passed.
- `cargo test -p cdf-conformance --lib --offline
  destination_catalog::generic_conformance_engines_do_not_branch_on_destination_identity --
  --exact`: passed after removing concrete database-source conveniences from the generic matrix
  model.
- Final identity scan found zero stale `resources/sql.toml`, `sql-dsn`, `CDF_CLI_SQL`,
  `write_sql_project_with_secret`, `run_sql_project_with_jobs`, or `POSTGRES_SQL_KIND` names in the
  affected Postgres source/product fixtures. Remaining literal `sql` values are the system SQL
  command, synthetic receipt verification, and private Postgres partition discriminator.
- `.10x/evidence/.storage/2026-08-02-sqlite-source-error-files.txt` contains six paths and
  `.10x/evidence/.storage/2026-08-02-sqlite-source-error-sites.tsv` contains one header plus 96
  classified sites. `cargo fmt --all -- --check` and `git diff --check` pass.
- `timeout 900s env DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 python3
  tools/certify-connector.py --kind source --id sqlite --core-impact --report
  target/quality/connector-sqlite-core-impact.json`: formatting, 16 leaf laws, and built-in catalog
  passed; general conformance failed on the subsequently repaired generic guard and the remaining
  sandbox-denied Postgres bind. The report verdict is failed and is not closure evidence.
- A broad sandboxed CLI suite observed 271 passes and 29 failures after local HTTP/Postgres binds
  were denied and the shared live-test mutex was poisoned. Focused non-contact Postgres planning,
  secret-failure, redaction, and query-rejection CLI tests passed; the broad result is recorded only
  as an environmental limit, not as a product pass.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-sqlite --lib --locked --offline -j 12`: 23 passed,
  0 failed after all correctness, bounds, capability, type-policy, and module repairs.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-source-sqlite
  -p cdf-source-postgres -p cdf-builtin-drivers --lib --locked --offline -j 12`: 38 passed,
  0 failed (23 SQLite, 12 Postgres, 3 built-in catalog).
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo check -p cdf-source-sqlite
  -p cdf-source-postgres -p cdf-builtin-drivers -p cdf-conformance -p cdf-benchmarks
  -p cdf-project -p cdf-cli --all-targets --locked --offline -j 12`: passed.
- The corresponding seven-package `cargo clippy --all-targets --all-features --locked --offline
  -j 12 -- -D warnings` passed. `cargo fmt --all -- --check` and `git diff --check` passed.
- Both exact conformance assertions passed with the established DuckDB build setting:
  `run_matrix::tests::registered_run_matrix_shards_cover_source_catalog` and
  `destination_catalog::generic_conformance_engines_do_not_branch_on_destination_identity`.
- The last passing v2 release roofline, 0.917027 over 16 enumerated inputs, is baseline evidence
  only. The two current 17-input 32K observations failed at 0.873142 and 0.884562. The raw artifact
  at `.10x/evidence/.storage/2026-08-02-sqlite-source-roofline.json` contains the subsequent failed
  and reverted 64K diagnostic at 0.889339; it is not a current-source or passing certificate.
- `.10x/evidence/.storage/2026-08-02-sqlite-source-error-files.txt` contains eleven paths and
  `.10x/evidence/.storage/2026-08-02-sqlite-source-error-sites.tsv` contains one header plus 109
  exact classified sites. A direct file/line reconciliation has no missing or extra ledger site.
- Orchestrator-observed core-impact certificate after the SQLite integration repairs: formatting,
  leaf laws, built-in catalog, 98 general-conformance assertions, selected SQLite source matrix,
  and source-extension phases passed. Workspace core recorded 1,597 passes and one mechanical
  closed-count failure (`expected 52`, `actual 53`); the result is precise failure evidence, not
  final closure evidence.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo test -p cdf-project
  tests::project_files::workspace_safety_lint_policy_and_exception_set_are_closed --locked -j 12
  -- --exact --nocapture`: 1 passed, 0 failed, 269 filtered out after updating only the exact
  workspace-member count to 53. An earlier invocation without the DuckDB setting failed at link
  time and executed no test.
- Orchestrator-observed certificate rerun after the member-count repair: every phase through
  `core-regression-profile` passed, including 2,108/2,108 workspace tests; only
  `workspace-clippy` failed after 8.5 seconds, with its diagnostic truncated by the wrapper. This
  is exact progression/failure evidence, not final certificate closure evidence.
- `DUCKDB_DOWNLOAD_LIB=1 CARGO_BUILD_JOBS=12 cargo clippy --workspace --all-targets
  --all-features --locked --offline -j 12 -- -D warnings`: first run exposed only
  `clippy::needless_borrow` at `crates/cdf-source-sqlite/src/catalog.rs:85`; after changing
  `unique_single_columns(&connection, table)` to `unique_single_columns(connection, table)`, the
  exact same gate passed in 5.73 seconds.
- `target/quality/connector-sqlite-core-impact.json`: baseline verdict `passed`, started
  `2026-08-02T17:02:19Z`, finished `2026-08-02T17:06:07Z`. All eight checks pass: format (2.051s),
  leaf laws (23 tests, 5.325s), built-in catalog (5.654s), general conformance (98 tests, 24.639s),
  selected SQLite source matrix (14.845s), source-extension policy (1.057s), workspace core (2,108
  tests, 172.828s), and workspace Clippy (1.382s). It predates the final declared-observation and
  directional-module repairs; a fresh certificate is deferred to parent final integration.

## Review

Independent red-team review performed 2026-08-02 without rerunning tests or changing
implementation. The review read every active authority referenced by this ticket (including their
active transitive authorities), the complete tranche diff and assertions, the error-ownership
manifest/ledger, the raw roofline artifact, and the core-impact certificate report.

### Findings

1. **Significant — pushed `LIMIT` is not semantics-preserving.**
   `SqliteTableScan::from_intent` retains the caller limit for every negotiated scan
   (`crates/cdf-source-sqlite/src/source.rs:915`), while query construction omits every inexact
   predicate from SQLite and nevertheless appends the SQL limit (`source.rs:1015`,
   `source.rs:1063`). The database can therefore return the first `N` rows of the unfiltered
   superset before the engine reapplies an inexact predicate, permanently losing qualifying rows
   after `N`. The same limit can cut a cursor-equal group; emitted checkpoints contain only the
   maximum cursor value, and resume uses `cursor > checkpoint` (`source.rs:989`), so unconsumed rows
   with that cursor value are skipped. No assertion covers inexact-filter-plus-limit or an
   equal-cursor group split by a limit.

2. **Significant — the declared stable tie-breaker is not proven stable.**
   Cursor validation proves only that `stable_key` exists, differs from the cursor, is non-null,
   and has one of three types (`source.rs:444-484`). It does not require a primary-key/unique
   catalog constraint, and discovery does not retain uniqueness authority. Duplicate
   `(cursor, stable_key)` tuples therefore leave the SQL order non-total and can change batch
   membership/checkpoint evidence between executions, contrary to the deterministic cursor
   contract. The equal-cursor test uses an integer primary key and does not falsify this case.

3. **Significant — the preaccounted memory and cancellation bounds are not enforced at the
   SQLite/Arrow boundary.** A variable-width value is measured and then copied into an Arrow
   builder before the maximum is checked, and the check occurs only after the whole row has been
   appended (`source.rs:703-742`). One oversized TEXT/BLOB cell can therefore allocate far beyond
   both the 32 MiB emitted-batch ceiling and its lease before returning an error, despite the
   `Preaccounted`/`bounded` capability claim. Cancellation is checked around returned rows, but no
   SQLite progress handler interrupts work inside `rows.next()` or a pre-row `ORDER BY`; a large
   sort or VM operation can run without a bounded cancellation observation. The suite has neither
   an oversized-cell assertion nor an execution-cancellation assertion.

4. **Significant — execution/schema capabilities overstate the implemented contract.**
   The kind-level execution profile declares every SQLite resource `resumable: true`
   (`crates/cdf-source-sqlite/src/driver.rs:445-464`), including full-snapshot resources whose
   resource capabilities advertise no replay and whose query builder rejects a start cursor. It
   simultaneously declares no retry granularity while partitions forbid retry
   (`source.rs:599-606`), so the static declaration does not describe either resource shape
   truthfully. Separately, the resource publicly accepts type-policy allowances
   (`source.rs:148-156`), but its dynamic storage-class decoder returns an unconditional Data error
   at the first mismatch (`source.rs:1285-1320`); no normalization/quarantine branch consumes the
   allowance. That does not implement the spec's configured normalization/quarantine behavior.

5. **Significant — the 0.926233 roofline arithmetic is correct, but the artifact is not
   closure-grade evidence.** Both workers record the database file length as `physical_bytes`
   (`crates/cdf-benchmarks/src/sqlite_source_roofline.rs:488-493`, `:586-591`), not observed bytes
   read, then publish physical-throughput medians. The portable observer records the cumulative
   `RUSAGE_CHILDREN.ru_maxrss` value as each child's peak (`:155-229`), producing the identical
   27,508,736-byte value for all ten samples; `evaluate` treats any `Some` value as valid
   (`:669-678`). The raw artifact also labels the direct cell
   `bsd-time-l-child-process` while the run-level and CDF cells say the portable fallback, and binds
   the result only to `cdf_revision: "working-tree"`, with no source/lock content identity. The
   comparable SQL, projection, conversion, medians, and ratio calculation are fair, but these
   counters/provider identities are inaccurate; under the roofline authority, missing or
   unreliable required counters make the gate inconclusive rather than pass.

6. **Significant — the crate still exposes a noncanonical construction path and lacks a coherent
   module boundary.** `SqliteTableResource::new`, `with_execution`, `with_type_policy`, and
   `open_owned` are public (`source.rs:93-169`) even though no in-repository external consumer needs
   them. `new` constructs a resource without a compiled plan hash, effective-schema runtime, or
   observation catalog, bypassing the canonical driver/compiled-plan path. The crate facade also
   reexports low-level validation, negotiation, planning, lane, and predicate helpers
   (`crates/cdf-source-sqlite/src/lib.rs:9-16`). Meanwhile one 2,289-line `source.rs` owns resource
   construction, capabilities, scan negotiation, SQL generation, execution, Arrow builders,
   cursor/temporal codecs, and tests. This contradicts the ticket's claim that the leaky
   construction surface was removed and leaves compiler-visible responsibilities materially less
   clear than the surrounding leaf-adapter boundary requires.

7. **Significant closure blocker — the certificate is failed, not merely absent.**
   `target/quality/connector-sqlite-core-impact.json` records a failed general-conformance phase and
   a failed overall verdict. The ticket correctly refuses to infer the unobserved Postgres-backed
   gate. A SQLite source shard contains a declared Postgres destination cell, so its need for the
   shared Postgres environment is not, by itself, evidence of connector-identity branching; the
   catalog and shard-integrity assertions remain properly data-driven. The certificate must still
   be rerun after repairs on a host that can provide its declared Postgres coverage.

### Checks that held

- The frozen error audit is internally exact: six Rust files, 96 implementation constructor sites,
  and the ledger's 56 Contract / 32 Data / five Internal / two classified-boundary / one test-only
  RateLimited counts match. Typed-wrapper preservation and locator-redaction ownership are
  structurally implemented; platform-specific SQLite extended codes remain the recorded limit.
- The concrete database-source identity migration is precise in the reviewed diff: registry,
  catalog, fixture, examples, and product tests use `postgres`; remaining `sql` names belong to the
  generic SQL command/synthetic SQL vocabulary or the private Postgres partition discriminator.
- Built-in registration and source-fixture/shard enrollment are data-driven; no reviewed generic
  engine branch was added for SQLite. Snapshot lifetime is covered by a real two-batch WAL writer
  assertion, and the direct/CDF benchmark timed regions perform the same selected/order/Arrow work.

### Verdict

**Fail.** The significant correctness, boundedness, capability, evidence-authority, and public
boundary findings above falsify the current closure claim independently of the already-failed
certificate. Keep this ticket non-terminal until the implementation and evidence are repaired and
freshly reviewed.

### Residual risk

No tests or certificate command were rerun by this independent review, as required. The current
suite does not exercise limit/residual-filter interaction, cursor-window truncation, duplicate
stable keys, oversized variable-width values, cancellation inside SQLite VM work, every temporal
encoding/lag boundary, or every platform SQLite extended error code. The roofline is one warm-cache
developer-host observation; even after its measurement defects are repaired, it does not establish
cold-cache or cross-platform performance.

## Retrospective

- Inspecting registry admission early caught the broad-`sql` selector conflict before catalog
  enrollment could weaken duplicate-kind rejection. Concrete `sqlite`/`postgres` identities keep
  ownership local and avoid inventing a dialect multiplexer.
- Focused production stream tests were a better diagnostic than repeatedly invoking the broad
  harness: they proved snapshot lifetime, cancellation/termination, stable equal-cursor order,
  temporal units, and dynamic storage drift without requiring unrelated destination services.
- The roofline was useful design feedback, not only a gate. Hoisting invariant schema/temporal work
  and raising the already-byte-bounded row target from 8,192 to 32,768 improved the ratio from
  0.755 to 0.926 without weakening cancellation or memory ceilings.
- Strict lint found the architectural cleanup that visual inspection had missed: a public
  eight-argument open function with no external caller. Passing the validated resource internally
  removed that leaky surface instead of suppressing the lint.
- The error audit caught two failures ordinary success-path tests did not: foreign path Display
  leaked a locator, and Arrow assembly assigned repair ownership to source data. The durable
  manifest/ledger made the final count reproducible after line-moving cleanup.
- The generic conformance destination guard currently treats a destination identity literal as
  forbidden even when it names a source archetype. Keeping concrete source identities solely in
  the source fixture catalog both satisfies the guard and produces a cleaner authority boundary.
- The remaining work is operational rather than ambiguous: rerun the bounded certificate where
  localhost Postgres is permitted, then obtain the required independent review. Until both pass,
  this ticket is blocked rather than done.
- Correctness pushdown and performance pushdown need separate proof. Leaving SQL `LIMIT` out keeps
  the adapter honest across residual predicates and cursor-equal groups; the engine can still
  enforce the user-visible limit after it owns the complete semantics.
- Stable-key types and nullability are insufficient authority. Carrying conservative catalog
  uniqueness into the compiled schema and rechecking the live table closes both stale-plan and
  hand-forged-plan paths without inventing runtime deduplication.
- Process resource counters are most trustworthy when the measured worker reports its own
  `RUSAGE_SELF` snapshot. A fallback selected after samples begin and cumulative child high-water
  marks cannot support per-sample claims, even when throughput arithmetic is otherwise correct.
- Splitting by query, execution, temporal codec, and tests made privacy useful evidence: once the
  crate root exported only the driver, compiler visibility exposed exactly which seams the driver
  and resource contract genuinely require.
- A source can emit normalized Arrow payloads while still owing the engine exact physical-schema
  evidence. Treating the output schema as the observation hid that distinction; observing the live
  catalog inside the read transaction and binding both full and projected hashes makes drift
  detection explicit without moving catalog authority into the generic engine.
- Closed workspace inventories should fail when a crate is added; the failure proved the safety
  fence was working. The correct repair was the one-number member-count update, leaving the
  unrelated unsafe-syntax inventory untouched.
- The final certificate sequence justified narrow iteration: each failure identified one concrete
  authority boundary or closed invariant, while the final run re-established the complete
  connector-to-workspace chain. Closure now rests on current artifact identities plus one full
  eight-check pass, not on combining partial historical runs.
- A file split is not a module boundary when parent and children wildcard-import each other.
  Making ownership directional required a dedicated schema-policy leaf and an executable guard
  over the production import graph, not just smaller files.
- A stable CDF wall time does not itself pass a relative roofline when the direct comparator moves.
  Recording both failed current observations and reverting an ineffective batch-size experiment
  preserves the evidence boundary; parent integration owns the single fresh measurement rather
  than this executor sampling until a favorable result appears.

## Fresh closure re-review — 2026-08-02

Independent closure re-review performed without rerunning tests or changing implementation. The
review reread every active authority in this ticket's reference graph, the complete current
tranche diff and assertions, the original seven-finding review, the ten-file/109-site error
artifacts, the raw v2 roofline artifact, and the passed core-impact certificate report.

### Findings

1. **Significant — declared-schema execution still substitutes logical output for a physical
   observation.** Ordinary declared resources compile with `effective_schema_runtime: None`
   (`crates/cdf-declarative/src/compiled.rs:305-325`). At execution, that state takes the early
   return at `crates/cdf-source-sqlite/src/source/execution.rs:216-218`, assigns the projected
   logical output schema as `physical_schema`, hashes it as the observation, and marks it as the
   materialized physical output (`execution.rs:57-60`, `execution.rs:184`). Only discovered/effective
   resources execute the live-catalog observation and full/projected hash checks at
   `execution.rs:219-270`. This leaves the common declared run-matrix path without an observed
   physical schema and contradicts the active schema authority that declared schemas constrain and
   project observed reality rather than replace it. The discovered-primary-key preview assertion
   proves the discovered path only; no assertion falsifies the declared path's substitution.

2. **Significant — the file split retains exactly the production dependency graph that the active
   module-boundary authority rejects.** The parent module wildcard-imports every extracted sibling
   (`crates/cdf-source-sqlite/src/source.rs:50-54`), and each production child wildcard-imports the
   parent (`source/execution.rs:1`, `source/query.rs:1`, `source/temporal.rs:1`). The active extension
   invariant says this bidirectional wildcard pattern preserves a monolith and is not
   modularization; production cross-module edges must be explicit and acyclic. The crate-level
   facade and construction path are now private and canonical, but that does not repair the
   internal ownership graph, so original finding 6 is only partially closed.

### Repairs that held

- Query construction has no SQL `LIMIT`, compiled SQLite partitions retain no pushed limit, and
  inexact predicates remain engine-owned; the reviewed assertions cover both residual-filter and
  cursor shapes.
- Cursor resources require a separate non-null supported stable key, discovery conservatively
  records only single-column primary-key or nonpartial unique evidence, and execution revalidates
  that constraint in the live read transaction before querying.
- Variable-cell and cumulative byte checks occur before Arrow append under a 64 MiB lease for a
  32 MiB emitted ceiling, and the installed 8,192-operation SQLite VM progress hook maps host
  cancellation explicitly.
- Snapshot and cursor execution capabilities are shape-specific, and the Arrow conversion path
  consumes both compiled coercion and lossy-mapping allowances.
- The v2 roofline artifact is internally coherent: five 1M-row samples per cell, fixed
  `RUSAGE_SELF` provider identity, populated CPU/RSS, explicitly unavailable zero physical-byte
  counters, zero spill, 0.917027 ratio, and 16 enumerated source/benchmark inputs. All 16 inputs
  predate the artifact, so its recorded workspace-content identity remains applicable to the
  reviewed benchmark implementation.
- The ten-file error manifest and 110-line ledger (header plus 109 sites) reconcile to 58 Contract,
  42 Data, six Internal, two boundary-classified `new`, and one test-only RateLimited constructor.
  Typed-wrapper preservation, cancellation ownership, and locator redaction remain coherent.
- The Postgres migration uses `postgres` for driver, kind, catalog, fixture, and source identity.
  Remaining `sql` values in the reviewed scope are generic SQL receipt/command vocabulary or the
  private Postgres partition discriminator, not construction selectors.
- The core-impact report is admissible, records the same 69-path changed-file inventory visible at
  review time, acknowledges every generic-core path, and records all eight checks passed. Its
  content digest names the certificate-time snapshot; the ticket's certificate journal entry and
  this independent review necessarily postdate that snapshot, so it must not be described as a
  byte-for-byte hash of those later execution-record edits. No benchmark input or implementation
  file was observed to postdate the passed report.

### Verdict

**Fail.** The correctness repairs, performance evidence, error audit, identity migration, public
facade, and certificate checks hold, but declared-schema physical-observation authority and the
compiler-visible internal module boundary remain significant closure defects. Keep the ticket
non-terminal until both are repaired and independently reviewed.

### Residual risk

No tests, roofline command, certificate command, or implementation mutation was performed by this
review. The roofline remains a warm-cache observation from one developer host and does not prove
cold-cache or cross-platform behavior. The error audit does not inject every platform-specific
SQLite extended code. The certificate proves its named laws for its recorded snapshot; later
execution-record-only edits are outside its content digest.

## Focused two-finding repair re-review — 2026-08-02

Independent re-review was limited to the two significant findings in the preceding closure
addendum, their current implementation, focused assertions/guard, and ticket evidence. No tests
were rerun and no implementation was changed. Fresh roofline and full core-impact certification
remain explicitly deferred parent integration gates and are not part of this verdict.

### Finding dispositions

1. **Resolved — declared-schema physical observation authority.**
   `execution_physical_schema` now unconditionally discovers the live catalog inside the same
   read transaction and projects that physical schema through the compiled logical-to-source
   mapping before branching on effective-runtime presence
   (`crates/cdf-source-sqlite/src/source/execution.rs:264-276`). A declared resource returns that
   live projected schema; a discovered/effective resource additionally enforces the full verified
   observation and projected planned hash (`execution.rs:278-322`). The focused leaf assertion
   changes the live `id` declaration from TEXT to INTEGER across two executions, proves logical
   Int64 output remains stable under the compiled coercion policy, proves the materialized physical
   type changes Utf8 to Int64, and proves the observed hash follows physical rather than logical
   schema. The conformance assertion crosses declarative compile, canonical resolution, engine
   plan binding, and preview with a declared Int64/live TEXT mismatch and explicit lossless
   coercion.

2. **Resolved — production module dependency DAG.**
   The current production graph is explicit and acyclic: `schema` is a leaf; `temporal` depends on
   `schema`; `query` depends on `schema` and `temporal`; `execution` depends on `query`, `schema`,
   and `temporal`; and `source.rs` is the composition layer. Parent imports/reexports enumerate
   symbols, every production child enumerates its external/crate/sibling imports, and no production
   module uses `use super::*` or a sibling glob. The focused guard rejects the former parent globs,
   child parent-globs, and the selected back-edges, while direct inspection confirms the complete
   current edge set.

### Verdict

**Pass for the two reviewed findings.** Both prior significant defects are repaired. Keep the
ticket non-terminal until the parent-owned current roofline and fresh full core-impact certificate
gates are resolved; this focused verdict makes no claim about either gate.

### Residual risk

This review relied on the journaled focused test results and did not rerun them. The module guard is
a source-text fence over the named forbidden edges rather than a general Rust dependency-cycle
analyzer; direct inspection supplies the current-DAG proof. The declared-schema assertions cover
projected scalar type drift and the generic preview path, not every supported projection, rename,
temporal encoding, or type-policy combination.
