Status: active
Created: 2026-08-02
Updated: 2026-08-08
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md
Depends-On: .10x/tickets/done/2026-08-02-clickhouse-destination-connector.md

# MongoDB source connector

## Scope

Implement and ship `cdf-source-mongodb` for MongoDB 7.0+ finite collection reads using the official
asynchronous driver's raw BSON cursor. Add bounded discovery/schema evidence, exact pushdown and
cursor semantics, BSON-to-Arrow mapping, catalog enrollment, live fixtures, documentation, and a
direct raw-BSON source roofline cell.

## Non-goals

Change streams, resume tokens, CDC operations, ObjectId checkpoints, arbitrary aggregation
pipelines, map-reduce, or implicit Extended JSON coercion.

## Acceptance Criteria

- Configuration, bounded discovery, freeze/drift, mapping, projection/filter fidelity,
  numeric/date/timestamp cursor ordering, compile/portability, health, and execution implement
  `.10x/specs/mongodb-collection-source.md`.
- Raw BSON mapping and variant/quarantine behavior cover heterogeneous documents, missing/null,
  nested arrays/documents, ObjectId, Decimal128, DateTime, duplicate keys, and unsupported types.
- Resource-scoped `schema_depth` defaults to 1, accepts only `1..=32`, and bounds retained nested
  inference. Boundary and discovery-time heterogeneous values use deterministic tagged Canonical
  Extended JSON without turning nested map keys into schema fields. Generated discovery SQL lists
  only fields retained by that authority.
- One reusable client/pool streams byte-accounted batches under injected async host, memory,
  cancellation, retry, and egress authorities without a private runtime or unbounded queue.
- Built-in catalog integrity, generic source matrix, jobs invariance, package/replay/checkpoint
  laws, and `tools/certify-connector.py --kind source --id mongodb --core-impact` pass against a
  digest-pinned MongoDB 8.0+ fixture plus an authorized MongoDB 7.0 Atlas lifecycle.
- The source macro benchmark reaches the 0.90 direct raw-BSON driver roofline with pool/batch
  settings recorded.
- Independent review passes after closure repair.

## References

- `.10x/specs/mongodb-collection-source.md`
- `.10x/decisions/exact-value-text-fallbacks.md`
- `.10x/specs/database-connector-roofline.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/research/2026-08-02-sqlite-clickhouse-mongodb-connector-shaping.md`
- `docs/connector-authoring.md`

## Assumptions

- Finite source semantics, deferred change streams, MongoDB 7.0+, and the 90% roofline are
  user-ratified.
- Collection-only authoring is the smallest complete finite document-source surface.
- Top-level-only discovery by default, optional bounded per-resource depth, opaque boundary values,
  and a UTF-8 fallback for destinations without proven native semi-structured fidelity are
  user-ratified.

## Journal

- 2026-08-02: Ticket opened; execution waits for complete ClickHouse tranche closure.
- 2026-08-03: Exact-value audit corrected schemaless BSON Decimal128 to tagged canonical text;
  schema-proven fixed domains remain Arrow Decimal128 and special values never become floats.
- 2026-08-04: Execution resumed after the ClickHouse source/destination implementations and their
  independent reviews stabilized. Their remaining non-terminal gate is the connector parent's
  deliberately final six-cell certificate, so treating it as a prerequisite to connector five
  would create a cycle. Read the complete ticket and governing source, roofline, exact-value,
  extension, checkpoint, and error-ownership authorities. Selected official `mongodb` 3.8.0 with
  BSON 3 and the raw-batch cursor API. The error audit scope is the new source leaf; catalog and
  conformance enrollment remain supporting boundaries and do not own SDK error classification.
- 2026-08-04: Implemented the `cdf-source-mongodb` leaf with contact-free compilation, typed
  configuration and secret references, one reusable official async client/pool, bounded discovery,
  raw BSON batch decoding, sampled effective-schema authority, exact ObjectId/Decimal128/DateTime
  mappings, typed filters/projection/cursors, cancellation, egress, and byte-accounted memory.
  Added the canonical MongoDB semantic definitions to the built-in registry and enrolled the
  source in the built-in catalog and generic source matrix.
- 2026-08-04: The digest-pinned MongoDB 8.0.13 live shard passed 15 executed append/replace/merge
  cells and three sheet-governed exclusions across ClickHouse, DuckDB, Parquet, Postgres, SQLite,
  and Quasar. Every executed cell proved package verification, receipt-gated checkpointing,
  duplicate no-op replay, and fresh-artifact replay. The live path exposed and repaired three
  shared current-contract defects without compatibility behavior: ClickHouse's sheet now publishes
  its real namecase/type rows, its conformance fixture creates disposition-truthful table engines,
  and packages persist the source schema-admission program separately from the destination-
  normalized validation program.
- 2026-08-04: Froze the nine-file source error scope and 146 construction-bearing lines under
  `.10x/evidence/.storage/2026-08-04-mongodb-source-error-files.nul` and
  `.10x/evidence/.storage/2026-08-04-mongodb-source-error-sites.tsv`. All 12 Internal-bearing lines
  are CDF/official-driver invariant sites. Direct and nested typed errors preserve kind, message,
  and retry delay before raw I/O or SDK fallback classification.
- 2026-08-04: Added the release raw-BSON source roofline. The favorable direct path uses the same
  official asynchronous client, projection, stable sort, duplicate-key rejection, BSON field
  conversion, Arrow construction, and full payload verification while omitting CDF governance.
  The final five-sample, 100,000-row MongoDB 8.0.13 sweep selected the minimum pool required for
  declared concurrency and then the fastest passing CDF median: 65,536 rows, pool size one,
  119,474,542 ns CDF median versus 108,078,000 ns direct median, ratio 0.904611. All six batch/pool
  sweep cells cleared 0.90 with median absolute deviation below 10%. The production defaults now
  match the selected bounds, and the report binds the digest-pinned image, workspace content, and
  executable hashes.
- 2026-08-04: The aggregate closure barrier exposed shared query-first integration defects rather
  than MongoDB leaf failures. Repaired current-only schema provenance canonicalization before
  relational execution, query-level limit ownership across preview/runtime partition frontiers,
  tracked source-row placement, residual predicate reporting, injected destination-registry
  propagation, Postgres discovery-to-runtime schema observation identity, error-kind preservation,
  and sequential receipt-gated multi-target promotion checkpoints. Updated current fixtures and
  deleted unreachable legacy financial-schema tests instead of retaining compatibility behavior.
- 2026-08-04: Workspace nextest executed 2,219 tests: 2,214 passed, three failed in the separately
  owned CLI ergonomics workstream, one required an unavailable `CDF_CLICKHOUSE_ENDPOINT`, and one
  source-position fixture expectation was then repaired and passed in isolation. Strict workspace
  Clippy passed. Focused cognitive-complexity diagnostics added no MongoDB finding; the changed
  preview coordinator remains at 34/25 and is retained as one cohesive authority. First-party
  `jscpd` reported 2.52% duplicated lines, below the 10% threshold, and `cargo machete
  --with-metadata` found no unused dependencies. `graphify update .` could not run because the
  executable is unavailable in this environment.
- 2026-08-04: Three independent red-team reviews falsified the first closure candidate. Repaired
  execution-time schema admission to submit the actual observation instead of prebinding the
  pinned hash; filtered Postgres catalog conversion to selected columns; preserved Int32 cursor
  domain after lag arithmetic; made MongoDB discovery and cursorless scans deterministic; enforced
  progressive residual-evidence cardinality/memory/path bounds; validated nested duplicate and
  unknown BSON fields; made a full output queue cancellation-aware; strictly bound collection
  type, collation, validator, and validation settings; percent-decoded add inputs; corrected stable
  MongoDB error ownership; and replaced declarative benchmark/conformance setup with the current
  query-first public lifecycle. The affected-package strict Clippy barrier passed, `cargo machete
  --with-metadata` found no unused dependency, and 19 pre-existing MongoDB unit tests plus the new
  residual-cardinality boundary test passed.
- 2026-08-04: Rereview repair made runtime observation IDs partition-bound, worker-safe hashes;
  rejected literal dotted BSON keys and recursively validated JavaScript-with-scope documents;
  enforced residual memory/cardinality bounds before evidence allocation; and separated preserved
  physical reconciliation from logical nulling. BSON Int32 observed under a pinned BSON Int64
  domain now retains the exact Int32 value and physical metadata without quarantining or changing
  the partition observation identity. `cdf-kernel` passed 84 unit tests, `cdf-source-mongodb`
  passed 22, and the focused engine null-evidence regression passed.
- 2026-08-04: The repaired live MongoDB matrix again passed all 15 supported cells with three
  destination-sheet exclusions. The public CLI lifecycle passed and now compares package
  layout/file inventory, segment identity/content, checkpoint positions, and receipt semantics
  across one and four jobs while checking both decoded and URL-encoded credential forms. The
  mechanically regenerated error ledger now contains 256 classified rows (219 production, 37
  test), including direct standard-I/O constructions and 39 explicit production invariant rows.
  Benchmark build/runtime cleanliness now includes untracked participating source and binds the
  benchmark build script itself; a fresh clean-snapshot roofline remains pending after commit.
- 2026-08-04: Second-rereview repair removed source-asserted typed-projection bypasses and split
  compatible physical reconciliation from residual drift end to end. MongoDB now vectorizes
  compatible subtype evidence independently of the 65,536 residual-candidate ceiling, validates
  it against the actual materialized Arrow cell before publication, persists exact observed and
  expected BSON metadata, bounds document-shape allocation progressively, and holds one canonical
  nullable materialized schema across clean and drifting batches. The verified source-owned
  physical observation catalog supplies exact BSON expectations after generic aggregation strips
  reserved metadata at its trust boundary. The authenticated public CLI lifecycle passed with a
  clean BSON Int64 batch followed by a BSON Int32-to-Int64 reconciliation batch, `batch_rows=1`,
  null `_cdf_variant`, jobs-one/jobs-four package semantics, redaction, and replay. MongoDB passed
  23 unit tests, the engine passed 237 executable library tests with six performance tests ignored,
  and the live source matrix again passed 15 supported cells with three governed exclusions.
  Strict affected-package Clippy and `cargo machete --with-metadata` passed; the explicit cognitive
  complexity diagnostic reported only previously known functions outside this repair.
- 2026-08-04: Closed the exact source-materialization authority gap without connector-specific
  logic in neutral crates. Kernel plans now carry typed source materialization rules and canonical
  source evidence; contract reconciliation, serialized-plan validation, project/runtime
  propagation, and engine execution exact-compare the constraint-relevant rule set. MongoDB owns
  recursive Decimal128 materialization and separates physical decoder schema from logical output
  schema. Contract passed 99 tests with two ignored; MongoDB passed its then-current 31 tests;
  engine passed 238 executable tests plus the updated package-identity golden in a focused rerun.
  Strict affected-package Clippy passed. Independent materialization-authority and MongoDB runtime
  rereviews passed with no critical or significant findings.
- 2026-08-04: The clean closure roofline exposed a real decoder regression: correctness matched,
  but duplicate preflight/value/name/path work reduced the selected ratio to 0.600252. Restored
  bounded throughput by statically preflighting prefix-disjoint fixed-cardinality schemas,
  compiling decoder source paths once, allocating residual names only for actual residuals, and
  fusing shape validation with top-level unknown classification. Lists and overlapping source
  paths use the exact estimator. A red-team finding showed that parent/child source paths could
  duplicate retained payload; the final repair detects equality/prefix overlap recursively and a
  two-document 36 MiB duplicate-payload test proves rejection at the 32 MiB progressive bound.
  All 32 MongoDB tests and strict Clippy passed; both performance rereviews passed with no findings.
- 2026-08-04: The final clean fat-LTO MongoDB 8.0.13 roofline at revision `89786e35` passed. The
  selection policy chose 32,768 rows and pool size one: 111,340,625 ns CDF median versus
  102,665,917 ns direct, ratio 0.922088. All samples matched rows, useful Arrow bytes, and checksum;
  every cell met the dispersion bound. The mechanically reproducible error ledger contains 295
  rows: 255 production and 40 test, with file-list/classifier/ledger SHA-256 values recorded in
  `.10x/evidence/2026-08-04-mongodb-source-connector.md`.
- 2026-08-07: Unblocked without code change. The user ratified shipping `mongodb+srv` and topology
  discovery with the egress bypass accepted as residual risk
  (`.10x/decisions/mongodb-srv-topology-egress-residual-risk.md`). Status moved `blocked` → `active`.
  Remaining closure work is the ordinary acceptance-criteria sweep plus the two documentation and
  research obligations now recorded under Blockers. A4 MongoDB CDC extends this same crate and
  inherits the same posture.
- 2026-08-08: A user-authorized live Atlas sandbox exercise exposed that the finite connector's
  generic username/password fields could not express the supplied temporary `MONGODB-AWS`
  authority. Added an explicit current-only IAM mechanism, `$external` validation, an optional STS
  session-token secret reference, and `cdf add` translation from the standard Atlas URI into three
  owner-only secret files plus a credential-free SRV endpoint. The parser rejects duplicate,
  conflicting, or unsupported query options without rendering their values. The official driver
  owns authentication; CDF's existing SDK classifier retains `Auth` ownership and controlled,
  credential-free diagnostics.
- 2026-08-08: Both authorized Atlas targets reported MongoDB 7.0.40. The user explicitly
  superseded the initial 8.0 minimum with 7.0+ so the finite connector matches the real deployment
  authority. The digest-pinned 8.0.13 fixture remains the deterministic conformance and roofline
  environment; the Atlas sandbox lifecycle supplies the 7.0 compatibility observation.
- 2026-08-08: The first real Atlas compile exposed three shared integration defects that the
  synthetic and cursor-based matrices had not covered. Manifest validation incorrectly required
  nested relational fields to survive governed variant capture; effective-schema output
  canonicalization deleted exact BSON metadata required by MongoDB semantic types; and project run
  rejected a bounded full `REPLACE` even though the finite-source spec explicitly supports
  cursorless snapshot reads. The repairs retain strict scalar/schema checks, strip physical
  metadata only when an actual coercion plan marks it transient, and admit cursorless execution
  only for bounded `IncrementalShape::Full` plus `REPLACE`. MongoDB full reads now attach one
  deterministic foreign-state completion authority binding resource, database, collection,
  partition, and scan intent; it is completion/checkpoint evidence, not a transaction-snapshot or
  incremental-resume claim. Existing cursor and page-token guards remain unchanged.
- 2026-08-08: The real Atlas 7.0.40 sandbox lifecycle completed against
  `floqast-fq12.changeStreamCollections`: compile established schema generation 1, plan resolved
  one bounded full-replace partition, and debug execution read 25 documents into one 25-row
  package segment. Package `sha256:c67ee6a4e7e1a6bf8647281e4f33077c1b46e2efc3dba5b7dcc4f1c539cba596`
  verified with 18 identity files and status `checkpointed`; its DuckDB receipt recorded 25 inserts
  and zero updates/deletes. A direct destination query returned 25 rows, 25 distinct ObjectIds,
  and 25 governed variant rows. The obsolete sandbox-only schema-v2 state DB was preserved as
  `.cdf/state.db.v2-backup-20260808` before CDF created current schema v3 state.
- 2026-08-08: A release-binary rerun against the first full-scan checkpoint exposed that generic
  resume binding treated the finite replacement's deterministic completion identity as a MongoDB
  resume cursor. The connector now owns this binding boundary: cursor checkpoints retain their
  existing field-checked binding, while cursorless full replacements validate the MongoDB
  completion protocol and clear the next partition's start position so replacement restarts from
  the beginning. Focused unit coverage and strict connector Clippy passed. A second real Atlas
  debug run against the existing state produced and verified a fresh 25-row package and left the
  replacement target at exactly 25 rows rather than duplicating or refusing the run.
- 2026-08-08: The final release binary at `db895465` reran the real Atlas 7.0.40 resource from the
  existing persisted checkpoint in 3 seconds. It committed and checkpointed one 25-row/5.1 KiB
  segment, verified package
  `sha256:ee47de42518efbdf3fad43b9b08ee6ec9feefdcc93a3a9a391b1872d52c3ab92`
  across 18 identity files, and left DuckDB at 25 rows, 25 distinct IDs, and 25 governed variant
  rows. Atlas IAM secrets remained outside project artifacts in three owner-only files.
- 2026-08-08: The user superseded recursive schemaless inference after identifying the common
  map-key explosion failure mode. Shaping established resource option `schema_depth = 1..32`,
  default 1, with the root document's fields at level 1. Consistent retained primitives stay typed;
  documents, arrays, and heterogeneous values at the boundary become deterministic tagged
  Canonical Extended JSON. Nested key changes inside opaque values are data, not schema drift;
  later incompatibility against a typed primitive remains governed by the existing compiled
  variant/quarantine/fail disposition. The replacement contract is
  `.10x/specs/mongodb-collection-source.md`; the former recursive contract is preserved at
  `.10x/specs/superseded/mongodb-collection-source-recursive-shape-discovery.md`.
- 2026-08-08: Implemented the ratified bounded discovery model. Resource option `schema_depth`
  defaults to 1, validates `1..=32`, and is bound into add proposals, compiled plans, candidate
  identity, discovery evidence, and schema metadata. Default discovery retains typed primitives
  but converts top-level documents, arrays, and sampled heterogeneous fields to deterministic
  tagged Canonical Extended JSON UTF-8. Higher depths expand only through their boundary, and
  retained-shape accounting recomputes the actual surviving schema so a field collapsed to opaque
  cannot leave phantom nested cardinality. The runtime decoder preserves these values without
  treating nested key changes as drift, while later mismatches against primitive pins remain
  governed residuals. All 42 MongoDB unit tests, six semantic-registry tests, strict affected-
  package Clippy, dependency hygiene, and diff checks passed. The refreshed error ledger contains
  320 rows: 277 production and 43 test.
- 2026-08-08: The connector certificate caught the expected stale MongoDB catalog hashes and then
  exposed a pre-existing shared keyed-package correctness bug. Merge execution finalized exact
  `KeyedChanges` authority, but the destination commit preimage still carried ordinary `Rows`
  authority, making verified replay fail before destination mutation. The package builder now
  exposes its finalized content authority to the project artifact writer, and the commit preimage,
  manifest, state delta, destination request, and receipt agree. DuckDB and Postgres drift/
  quarantine conformance plus merge dedup and artifact-only replay pass with keyed intent and
  canonical key order. Full conformance now passes 91/92; the sole failure is the pre-existing MVP
  CLI fixture contacting synthetic `api.github.test` during plan discovery, reproduced unchanged
  in a detached `cf37cc5c` worktree. Package passed 94 executable unit tests with four performance
  tests ignored, strict affected-package Clippy passed, and the complexity diagnostic added no
  finding in changed production code.
- 2026-08-08: Closed the implementation barrier with the release binary at `8260bfb9` and a fresh
  default-depth resource identity against Atlas 7.0.40. `schema_depth` was intentionally omitted,
  so compile exercised the default of 1 and established generation 1 with four retained fields.
  The complex `startAfterToken` field compiled as nullable UTF-8 with physical type
  `bson:document` and semantic `mongodb.document_extended_json@1`, not as recursive fields or
  residual drift. Plan selected one bounded full replacement. Run read and committed 25 documents
  in one 5.6 KiB segment, and package
  `sha256:643320de7c56ab1e677c179af9ad138e88ac2f4239c9b03b50509368b0131f39`
  verified across 18 identity files. Direct DuckDB checks returned 25 rows, 25 distinct ObjectIds,
  seven non-null opaque values, seven valid JSON values, zero invalid JSON values, and zero
  `_cdf_variant` rows. No document value or credential was recorded.
- 2026-08-08: Reopened after the user requested a throughput run over the largest authorized Atlas
  collection. Catalog statistics identified `depreciation-items` at 417,114 documents and
  6,024,520,493 logical BSON bytes. Full and typed-projection runs repeatedly crossed the 32 MiB
  progressive decode bound even when configured batch cardinality and measured document size
  proved they should fit. Source inspection found the cause: `build_query` validated logical
  projection fidelity but omitted a MongoDB projection document, and execution never called
  `Find::projection`. Atlas therefore transmitted full documents while plan evidence claimed no
  wider source fetch. This contradicts the existing projection-fidelity acceptance criterion and
  owns a focused current-contract repair before the throughput run can be accepted.

## Blockers

None.

Resolved on 2026-08-07. The former blocker — the official MongoDB driver initiating topology-monitor
connections to hosts learned after the initial socket is authorized, which never reach
`SourceEgressAuthorizer` (`crates/cdf-runtime/src/source.rs:424`) — was closed by user ratification
rather than by code. `mongodb+srv` and topology discovery ship as advertised, and the bypass is
recorded as accepted residual risk in
`.10x/decisions/mongodb-srv-topology-egress-residual-risk.md`.

Two obligations follow from that decision and belong to this ticket's closure:

- any documentation or operator-facing claim that `EgressAllowlist` bounds egress MUST exclude
  MongoDB, because for this driver it does not;
- the hypothesis that the MongoDB SRV specification requires returned hosts to share the seed's
  parent domain is **unverified**. It is not relied upon. Confirming or refuting it is assigned to
  A4 MongoDB change-stream protocol research, not to this ticket.

## Evidence

- Forty-two focused source unit tests pass, including exact BSON mapping, depth-bounded discovery,
  opaque Canonical Extended JSON, drift, duplicate-key,
  injection, portability/redaction, cursor, payload-bound, and error-wrapper behavior.
- The authorized Atlas 7.0.40 finite lifecycle reaches compile, plan, package, DuckDB receipt,
  checkpoint, package verification, and direct destination row-count verification over real data.
- The selected live generic source shard passes all required cells against MongoDB 8.0.13,
  PostgreSQL 17, and the clean digest-pinned ClickHouse fixture.
- Error inventory and classification are frozen at the paths recorded in the journal.
- The final release raw-BSON roofline passes at 0.922088 with the selected pool/batch settings recorded
  in `.10x/evidence/.storage/2026-08-04-mongodb-source-roofline.json`.
- Workspace integration, strict Clippy, duplication, and dependency-hygiene observations are
  recorded in the journal and `.10x/evidence/2026-08-04-mongodb-source-connector.md`.
- All historical implementation and performance rereviews pass. The certificate's MongoDB leaf
  and catalog gates pass; 91/92 general conformance laws pass, with the remaining pre-existing CLI
  fixture failure recorded in the journal. The fresh default-depth Atlas release lifecycle passed
  at `8260bfb9` with an opaque document column and no governed residual rows.

## Review

- First closure review verdict: fail. Critical: topology-discovered hosts could bypass injected
  egress authorization. Significant: prebound execution observations, nondeterministic reads,
  post-hoc residual memory accounting, cancellation while the output queue is full, incomplete
  nested BSON evidence, lax collection metadata, stale error evidence, and benchmark/public-CLI
  paths that did not exercise the current surface. The reviewers also found shared Postgres
  projection admission and Int32 cursor-domain defects.
- All non-semantic findings are repaired. Independent shared-authority, evidence/runtime, and
  decoder/performance rereviews pass on stable pushed commits with no critical or significant
  findings. The topology/egress limitation is explicitly accepted as residual risk in the active
  decision cited under Blockers and is not represented as a stronger transport guarantee.
- The user explicitly required no additional subagent/red-team pass for the bounded-depth change;
  closure relies on the existing independent connector reviews plus the focused behavioral,
  conformance, package-integrity, and live Atlas evidence recorded above.

## Retrospective

- The largest throughput regression came from individually reasonable safety passes compounding
  into repeated raw-BSON traversal and per-value allocation. Release profiling, rather than more
  test-loop repetition, isolated the preflight, source-path, and known-name costs.
- Conservative static memory accounting needs an explicit aliasing proof. Prefix-disjoint paths
  make raw-document bytes a payload upper bound; overlapping paths must retain exact per-value
  accounting. The reviewer-found parent/child alias is now a focused behavioral boundary test.
- Compile stable decoder metadata once and allocate evidence only when evidence exists. This both
  restored throughput and made the distinction between governed values and retained drift more
  literal.
- Final performance evidence must use the exact default release profile and a clean committed
  participating snapshot. Faster non-LTO builds were useful iteration signals but never treated as
  closure evidence.
- The topology-learned-host limitation remains an explicit accepted residual risk and a boundary
  for future transport work; it is no longer a closure blocker for the finite source.
- A schemaless source needs an explicit inference boundary, not merely a sampling limit. Treating
  complex boundary values as opaque semantic values avoids map-key cardinality explosions while
  preserving useful top-level primitive typing and ordinary governed drift for later primitive
  incompatibilities.
