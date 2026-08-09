Status: open
Created: 2026-08-09
Updated: 2026-08-09
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-07-a2-log-source-runtime-archetype.md`, `.10x/tickets/2026-08-07-a6-2-routed-target-families.md`

# MongoDB change-stream CDC

## Scope

Extend the existing first-party `mongodb` source with receipt-gated collection- and database-level
change streams, explicit latest/snapshot bootstrap, exact post-image and delete-key effects,
resumable event-prefix positions, deterministic collection admission, typed and envelope database
representations, routed output schema families, continuous finite-drain execution, bounded package
retention, and live replica-set/Atlas certification.

## Non-goals

- Cluster-wide watches, `updateLookup`, sparse patch updates, or source transaction-atomicity
  claims.
- Runtime schema creation, union-schema inference across collections, recursive discovery beyond
  configured `schema_depth`, or target-name template interpolation.
- Per-route SQL override blocks, implicit fallback routes, silent DDL/invalidation handling, or a
  second MongoDB source kind.

## Acceptance criteria

- [ ] Static authoring and compilation support collection/database watch scope, explicit
      bootstrap, typed/envelope representation, include/exclude collection patterns, source
      defaults with resource overrides, protected collection routing, and closed option schemas
      without source contact.
- [ ] Collection discovery reuses the exact finite MongoDB discovery/mapping authority; typed
      database discovery deterministically inventories and independently discovers every admitted
      collection with configured `schema_depth` defaulting to `1`, bounded concurrency, aggregate
      evidence, and all-or-nothing schema-family publication.
- [ ] Envelope mode preserves BSON type/value meaning as deterministic Canonical Extended JSON;
      typed mode decodes complete images against per-output schema authority without unioning
      unrelated collections.
- [ ] Change-stream execution requires MongoDB 7.0+, supported topology, enabled collection
      post-images, exact `fullDocument: required`, exact document keys, and rejects missing images,
      malformed/unsupported events, invalidation, DDL, and resume-history loss before token advance.
- [ ] Collection events settle one target; database events route atomically across deterministic
      physical targets with one package, receipt, checkpoint, and resume token. A newly matching
      collection or one failed target advances nothing.
- [ ] Latest bootstrap records a source-issued resume token and excludes prior history; snapshot
      bootstrap opens the stream first and completes a bounded, gapless snapshot-to-stream handoff.
- [ ] Continuous run repeats finite drain epochs, reports liveness/retries/backoff, resumes from the
      receipt-covered token, and garbage-collects only packages made collectible by durable
      destination settlement/checkpoint/retention authority.
- [ ] Driver/codec/network errors preserve typed ownership, source provenance, retry metadata, and
      redaction; no URI credentials, BSON payload, pipeline, or resume token is displayed.
- [ ] Synthetic, local replica-set, crash/replay, jobs/rechunking, bounded-memory, and
      same-semantics throughput certificates pass. Final acceptance includes a production-binary
      sandbox run against the authorized Atlas deployment watching the full admitted database and
      proving real multi-collection fan-out; a synthetic or single-collection run is insufficient.

## References

- `.10x/specs/mongodb-change-stream-source.md`
- `.10x/specs/mongodb-collection-source.md`
- `.10x/specs/cdc-log-source-foundation.md`
- `.10x/specs/cdc-source-position-artifacts.md`
- `.10x/specs/cdc-resource-authoring-and-continuous-run.md`
- `.10x/specs/routed-destination-target-families.md`
- `.10x/specs/retention-aware-package-collection.md`
- `.10x/knowledge/error-ownership-taxonomy.md`
- `.10x/research/2026-08-03-cdc-protocol-position-contract.md`

## Assumptions

- User-ratified: collection and database watches, both typed and envelope database
  representations, typed-by-default behavior, include/exclude patterns, deterministic ordinary
  discovery rather than change-stream sampling, and configurable per-collection `schema_depth`
  with default `1`.
- User-ratified: generic routed schema authority belongs to resource plus output binding; a newly
  matching collection fails before its token advances until explicit discovery/compile.
- User-ratified: final acceptance requires a production binary to run full admitted-database CDC
  against the authorized Atlas deployment.
- Record-backed: MongoDB event-prefix resume tokens, exact post-images, destination receipts,
  checkpoints, and package retention are the only safe advancement/collection authorities.

## Journal

- 2026-08-09: Opened after the focused MongoDB CDC contract and heterogeneous routed-schema
  extension were explicitly ratified. Implementation waits for the neutral finite-drain
  certificate and generic routed-family dependency; no behavior-changing assumptions remain.

- 2026-08-09: The neutral finite-drain dependency closed with a production package/receipt/
  checkpoint crash certificate for MongoDB event-prefix resume tokens. Generic routed target
  families are now the sole implementation dependency.

- 2026-08-09: Implemented the first executable change-stream slice without coupling CDC to the
  resource cursor grammar. `mode => 'cdc'` now requires `CDC_APPLY`, forbids a declared cursor,
  compiles an exact native resume-token scope, and advertises unbounded/resumable stream
  capabilities. Snapshot resources retain their existing optional cursor/full-replacement path.

- 2026-08-09: Added collection/database watch compilation, explicit latest/snapshot bootstrap,
  typed/envelope selection, source defaults with resource overrides, closed `$match`-only change
  pipelines, deterministic include/exclude glob admission, per-collection database discovery, and
  a compiled collection inventory. Database envelope discovery inspects metadata but never samples
  documents or infers nested fields.

- 2026-08-09: Added executable latest-bootstrap change streams for typed collection and envelope
  database modes. Runtime preflight proves MongoDB 7+, replica-set/sharded topology, and enabled
  post-images for every admitted collection. Events carry exact BSON resume-token positions and
  begin/data/terminal event-prefix settlement; Canonical Extended JSON envelope rows preserve BSON
  meaning and unknown collections/DDL fail before their token advances. Gapless snapshot bootstrap
  and typed heterogeneous database execution remain open.

- 2026-08-09: Repaired the neutral event-prefix settlement runtime to accept a source-proven empty
  scanned prefix. This is required for `bootstrap => 'latest'`: MongoDB can issue a post-batch
  resume token before any event exists, and CDF must durably checkpoint that exact token without
  inventing a data row. Committed-transaction units still reject empty settlement.

- 2026-08-09: Added current SQL authoring for `DISPOSITION CDC_APPLY(<keys>)` and mandatory
  `DELETE HARD|IGNORE|SOFT(<boolean marker>)`, then bound the compiled delete policy into the
  engine's native keyed-effect authority. Live Atlas compilation exposed and closed three
  integration defects: source-level snapshot `max_time_ms` no longer poisons latest change
  streams, the admitted database inventory now comes from bound discovery evidence rather than a
  duplicate physical-plan copy, and compiled/resolved MongoDB CDC uses one capability authority.
  Database envelope planning binds one representative observation only after proving every
  admitted collection has the identical physical envelope schema; the full evidence inventory
  remains the routing and unknown-collection authority. Active schema reuse now also requires the
  current schema-binding-stable source semantics, so a driver capability correction cannot keep an
  obsolete executable source plan merely because its discovery interpretation is unchanged.

- 2026-08-09: Live Atlas planning exposed that the registry-validation and window-scoping runtime
  decorators forwarded the base schema but silently discarded routed output schema inventory and
  its required routing field. Both decorators now preserve the complete `QueryableResource`
  authority, including routed outputs and baseline observation schemas. The production binary now
  compiles and plans the admitted two-collection Atlas database resource through `CDC_APPLY` with
  one drain partition and six routed destination migrations.

- 2026-08-09: The first production-binary Atlas executions opened a real database change stream
  and exposed five integration defects that synthetic compilation could not: an absent optional
  comment was serialized as an illegal `$changeStream.comment: null`; settlement controls omitted
  their exact Arrow memory lease; upserts labeled their logical rather than physical observation
  schema; live collection admission was not applied to database traffic; and zero-row latest-token
  epochs had no schema-attestation path. Optional comments are now omitted, settlement memory is
  exact and preaccounted, upserts/control markers carry source-materialized physical evidence,
  database preflight requires the matching collection inventory to equal compiled discovery, and
  a protected server-side pipeline filters ordinary events to the compiled inventory while still
  surfacing DDL/invalidation. The engine now accepts verified schema evidence on settlement control
  batches, so an idle latest-bootstrap epoch can checkpoint without fabricating a data row.

- 2026-08-09: A subsequent live routed delete reached exact-key reduction and exposed that the
  dedicated CDC delete path recorded surviving routed effects but omitted the corresponding routed
  input count. The delete path now observes the protected key-plus-route batch before spill/dedup,
  matching the existing upsert path and preserving per-output reduction invariants.

- 2026-08-09: The first multi-event live epoch then exposed a superseded routed-CDC restriction:
  the engine rejected any repeated destination key even though exact source-protocol reduction is
  the required behavior. Routed reduction now tracks the distinct kept ordinal for every dropped
  row per output binding, preserving truthful per-route input, survivor, and duplicate-key counts.
  The release binary subsequently committed one atomic two-target Atlas package: invoices reduced
  one upsert plus one delete to one hard-delete effect, while orders reduced two upserts to the
  final post-image. Both outputs share one package hash, destination receipt, and Mongo resume-token
  checkpoint; DuckDB contains the final `status=settled` order and no corresponding invoice row.

## Blockers

- `.10x/tickets/2026-08-07-a6-2-routed-target-families.md` must establish generic heterogeneous
  output schemas and atomic destination family settlement.

## Evidence

- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-mongodb --lib` — 55 adapter behavioral tests
  passed, including native resume-token BSON round-trip, cursor prohibition for CDC, unbounded
  stream capability compilation, redacted pipeline evidence, canonical Extended JSON, and routed
  delete envelope shape. This is synthetic evidence and does not certify a live MongoDB server.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
  cdc_apply_reduces_complete_upserts_and_key_only_deletes_across_effect_families --lib` — existing
  receipt/package CDC effect reduction remains green after routed delete-key projection changes.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-source-mongodb -p cdf-kernel -p cdf-engine -p
  cdf-cli --all-targets -- -D warnings` — strict affected-package Clippy passed.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-runtime
  event_prefix_accepts_a_source_proven_empty_scanned_prefix --lib` — the neutral runtime accepts
  the MongoDB post-batch-token bootstrap frontier with zero fabricated rows while retaining the
  existing nonempty requirement for committed-transaction settlement.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-mongodb cdc_ --lib` — 5 focused CDC compilation
  and discovery-authority tests passed, including cursor-free CDC, latest-versus-snapshot native
  option precedence, and inventory recovery from bound discovery evidence.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
  cdc_apply_reduces_complete_upserts_and_key_only_deletes_across_effect_families --lib` — the
  existing exact effect-order/reduction certificate passes after the routed delete count repair.
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy -p cdf-engine -p cdf-source-mongodb --all-targets -- -D
  warnings` — strict Clippy passes for the live-runtime repair surface.
- Release-binary Atlas planning evidence: `cdf compile mongo_live.atlas_database_cdc` and `cdf plan
  mongo_live.atlas_database_cdc` succeed against two isolated, post-image-enabled collections in
  the authorized Atlas database. The plan reports one drain partition, `cdc_apply`,
  `effectively_once_per_position`, five logical fields, six routed destination migrations, and
  `ready 1/1`.
- Release-binary Atlas acceptance: package
  `pkg-mongo-live-atlas-database-cdc-35018-1786308101634419000` committed with two routed segments,
  a single DuckDB receipt, and checkpoint
  `checkpoint-mongo-live-atlas-database-cdc-35018-1786308101634419000`. Its package authority
  records per-route `duplicate_key_count=1`; invoice effects are input `{upserts:1,deletes:1}` to
  surviving `{upserts:0,deletes:1}`, and order effects are input `{upserts:2,deletes:0}` to
  surviving `{upserts:1,deletes:0}`. The receipt acknowledges both physical targets and both
  segments, and the proposed checkpoint carries the terminal event resume token. This certifies
  real Atlas database-watch fan-out, exact update/delete reduction, atomic routed destination
  settlement, receipt-gated checkpointing, and hard-delete application with the production binary.

## Review

Not started.

## Retrospective

Pending execution.
