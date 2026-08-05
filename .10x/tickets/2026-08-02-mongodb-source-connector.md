Status: blocked
Created: 2026-08-02
Updated: 2026-08-04
Parent: .10x/tickets/2026-08-02-sqlite-clickhouse-mongodb-connector-program.md
Depends-On: .10x/tickets/done/2026-08-02-clickhouse-destination-connector.md

# MongoDB source connector

## Scope

Implement and ship `cdf-source-mongodb` for MongoDB 8.0+ finite collection reads using the official
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
- One reusable client/pool streams byte-accounted batches under injected async host, memory,
  cancellation, retry, and egress authorities without a private runtime or unbounded queue.
- Built-in catalog integrity, generic source matrix, jobs invariance, package/replay/checkpoint
  laws, and `tools/certify-connector.py --kind source --id mongodb --core-impact` pass against a
  digest-pinned MongoDB 8.0+ fixture.
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

- Finite source semantics, deferred change streams, MongoDB 8.0+, and the 90% roofline are
  user-ratified.
- Collection-only authoring is the smallest complete finite document-source surface.

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
  residual-cardinality boundary test passed. The regenerated error ledger contains 216 classified
  construction-bearing rows, including 21 production invariant rows whose ownership is explicit.

## Blockers

- The official MongoDB driver can initiate topology-monitor connections to hosts learned after the
  initial socket is authorized. The current spec advertises `mongodb+srv` and topology discovery,
  but the injected egress authority has no pre-connect hook for those learned hosts. Closure needs
  user ratification to narrow the current release to one explicit direct `mongodb://host[:port]`
  authority, or a separately shaped egress-aware transport design.

## Evidence

- Twenty focused source unit tests pass, including exact BSON mapping, drift, duplicate-key,
  injection, portability/redaction, cursor, and error-wrapper behavior.
- The selected live generic source shard passes all required cells against MongoDB 8.0.13,
  PostgreSQL 17, and the clean digest-pinned ClickHouse fixture.
- Error inventory and classification are frozen at the paths recorded in the journal.
- The release raw-BSON roofline passes at 0.904611 with the selected pool/batch settings recorded
  in `.10x/evidence/.storage/2026-08-04-mongodb-source-roofline.json`.
- Workspace integration, strict Clippy, duplication, and dependency-hygiene observations are
  recorded in the journal and `.10x/evidence/2026-08-04-mongodb-source-connector.md`.
- The final connector certificate and closure review remain pending.

## Review

- First closure review verdict: fail. Critical: topology-discovered hosts could bypass injected
  egress authorization. Significant: prebound execution observations, nondeterministic reads,
  post-hoc residual memory accounting, cancellation while the output queue is full, incomplete
  nested BSON evidence, lax collection metadata, stale error evidence, and benchmark/public-CLI
  paths that did not exercise the current surface. The reviewers also found shared Postgres
  projection admission and Int32 cursor-domain defects.
- All non-semantic findings are repaired and await independent rereview on a stable pushed commit.
  Residual risk remains the direct-host semantic decision recorded in Blockers.

## Retrospective

Pending executor handback.
