Status: active
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

- [x] Static authoring and compilation support collection/database watch scope, explicit
      bootstrap, typed/envelope representation, include/exclude collection patterns, source
      defaults with resource overrides, protected collection routing, and closed option schemas
      without source contact.
- [x] Collection discovery reuses the exact finite MongoDB discovery/mapping authority; typed
      database discovery deterministically inventories and independently discovers every admitted
      collection with configured `schema_depth` defaulting to `1`, bounded concurrency, aggregate
      evidence, and all-or-nothing schema-family publication.
- [x] Envelope mode preserves BSON type/value meaning as deterministic Canonical Extended JSON;
      typed mode decodes complete images against per-output schema authority without unioning
      unrelated collections.
- [x] Change-stream execution requires MongoDB 7.0+, supported topology, enabled collection
      post-images, exact `fullDocument: required`, exact document keys, and rejects missing images,
      malformed/unsupported events, invalidation, DDL, and resume-history loss before token advance.
- [x] Collection events settle one target; database events route atomically across deterministic
      physical targets with one package, receipt, checkpoint, and resume token. A newly matching
      collection or one failed target advances nothing.
- [x] Latest bootstrap records a source-issued resume token and excludes prior history; snapshot
      bootstrap opens the stream first and completes a bounded, gapless snapshot-to-stream handoff.
- [x] Continuous run repeats finite drain epochs, reports liveness/retries/backoff, resumes from the
      receipt-covered token, and garbage-collects only packages made collectible by durable
      destination settlement/checkpoint/retention authority.
- [x] Driver/codec/network errors preserve typed ownership, source provenance, retry metadata, and
      redaction; no URI credentials, BSON payload, pipeline, or resume token is displayed.
- [x] Synthetic, local replica-set, crash/replay, jobs/rechunking, bounded-memory, and
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

- 2026-08-09: Implemented heterogeneous typed database watches end to end. Every admitted
  collection now keeps its independently discovered logical/physical schema and observation id
  through manifest compilation, query compilation, engine admission, spill/dedup, package replay,
  destination planning, and atomic routed application. MongoDB `_id` is the non-null logical `id`
  while exact physical key decoding remains source-name aware. Multiple route observations may
  attest the same terminal event token, but distinct or mixed resume tokens remain invalid.

- 2026-08-09: Live replay exposed that typed CDC rejected residual evidence inside the adapter,
  bypassing the compiled `capture_variant` policy that already governs finite MongoDB reads. Typed
  CDC now attaches bounded residual and physical-reconciliation evidence to the materialized batch
  and lets the engine apply the resource policy; delete keys remain exact and cannot use residual
  fallback. The same replay also closed heterogeneous spill-family collisions, rowless drain
  checkpoint frontiers, target-scoped DuckDB key-index identity, DuckDB catalog type aliases, and
  unique package/checkpoint identities for every portable plan.

- 2026-08-09: Implemented finalized single-target CDC application for collection watches without
  forcing key-only deletes through DuckDB's row-oriented staged ingress. Upserts, hard deletes,
  the unique key authority, receipt mirror, and checkpoint proof now share one DuckDB transaction;
  duplicate package replay returns the committed receipt. Empty latest-bootstrap epochs write only
  receipt/checkpoint proof and do not create or replace a target. Destination planning now previews
  keyed package authority for merge/CDC rather than a fictitious row package.

- 2026-08-09: Implemented integrated snapshot bootstrap for collection and database watches. The
  adapter opens the exact change stream first, binds a server-issued post-batch/event resume token,
  scans the canonical admitted inventory through the native raw-BSON batch and Arrow decoder path,
  emits snapshot documents as complete CDC upserts inside one token-bound settlement prefix, then
  consumes changes after that token. A first event encountered while establishing the token is
  included in the same prefix, so it cannot be skipped by checkpoint recovery. Cursor/decode
  memory is reserved before allocation, source cancellation remains active throughout, and the
  compiled transaction ceiling fails the handoff before checkpoint advancement if the complete
  bootstrap unit is too large. Snapshot bootstrap requires primary read preference; a lagging
  secondary cannot establish gapless initial state relative to the stream token.

- 2026-08-09: Added a runtime-level graceful-stop authority distinct from hard cancellation.
  MongoDB change streams now wake immediately from an idle server wait on the first process
  interrupt, finish any already-observed event-prefix unit, and let the normal receipt/checkpoint
  gate close the drain; a second interrupt cancels promptly without advancing unfinished work.
  This remains independent of resource cursors: bounded and replace runs need none, while MongoDB
  CDC resumes from its native token frontier. Continuous drain epochs now invoke the canonical
  retention collector after each committed checkpoint rather than accumulating package buffers
  until the command exits.

- 2026-08-09: The first production-profile continuous Atlas database run committed two routed CDC
  epochs from one long-lived command, collected the first package under one-run retention, then
  accepted SIGINT during the next idle change-stream wait and exited successfully at the last
  committed frontier. That run also exposed two reporting/steady-state defects: automatic
  retention scanned unrelated multi-gigabyte packages, and the final CLI summary reported only
  the last epoch. Collection is now candidate/resource scoped, and drain reports carry aggregate
  rows, bytes, segments, and wall time for truthful terminal/JSON summaries.

- 2026-08-09: Repeated the continuous certificate with the optimized production binary. Two live
  Atlas mutations in different collections committed as two independently receipt-gated epochs;
  the next change-stream read began immediately after each checkpoint rather than pausing for a
  repository-wide package scan. The first package was tombstoned after the second checkpoint under
  one-run retention, SIGINT woke an idle third epoch and exited at the last committed token, and
  the terminal report aggregated both epochs rather than presenting only the final one.

- 2026-08-09: Final envelope acceptance exposed a logical/physical schema boundary defect:
  discovered governed authority carries identity-preserving source-name metadata and may carry the
  framework residual column, while the MongoDB adapter required byte-for-byte equality with the
  bare four-field physical envelope. Validation now accepts only the exact four envelope fields,
  their identity source-name metadata, and an optional genuine trailing framework residual field.
  Runtime materializes the bare discovered physical envelope so normal admission owns the logical
  residual column. A production plan then resumed from its receipt-covered token and routed two
  tagged Atlas events into distinct envelope tables.

## Blockers

None. Implementation and acceptance evidence are complete; tranche-level independent review is the
remaining closure gate.

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
- Production-profile typed Atlas acceptance: portable plan
  `atlas-database-typed-cdc-e2e8-release.plan.json` passed preflight and package
  `pkg-portable-mongo-live-atlas-database-typed-cdc-e2e3-54997-1786317120895846000` committed one
  database-watch epoch across independently typed invoices and orders tables. Its routed receipt
  proves invoices `{upserts:1,deletes:1}` with `rows_inserted=1`, `hard_deletes=1`, and no missing
  keys; orders `{upserts:2,deletes:0}` with `rows_inserted=1`, `rows_updated=1`; and one receipt plus
  one event-issued resume-token checkpoint covers all four events. DuckDB inspection proved the
  old invoice absent, the existing order updated, both new rows present, opaque document/array
  fields preserved as Extended JSON, and no residuals for the governed final run.
- Production-profile telemetry acceptance: the same 30-second Atlas run continuously redrew the
  active Read phase with monotonic elapsed time, added exact row/byte/batch counters when events
  arrived, and retained final Read, Packaged, Verified, and Committed summaries. The local release
  build used the pinned downloaded DuckDB tuple and normal optimized release profile, not the
  fat-LTO benchmark profile.
- Drift/replay acceptance: a prior Atlas event introduced two undiscovered top-level fields. The
  first run failed before checkpoint advancement; after the CDC evidence repair, a new portable
  plan replayed from the unchanged committed token, captured both fields in `_cdf_variant`,
  atomically applied the remaining governed effects, and committed the terminal event token. This
  proves field drift obeys compiled policy rather than being silently dropped or adapter-rejected.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-mongodb --lib` — all 60 MongoDB adapter tests
  passed after the typed CDC residual-evidence repair. `DUCKDB_DOWNLOAD_LIB=1 cargo clippy` with
  warnings denied passed for kernel, runtime, engine, package, MongoDB, DuckDB, project, and CLI
  across all targets; formatting and `git diff --check` passed without `RUST_MIN_STACK`.
- Focused DuckDB certificate:
  `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-dest-duckdb
  single_target_cdc_applies_upserts_hard_deletes_and_replays_once --lib` passed. It proves two
  exact upserts, one hard delete, one surviving row, keyed commit counts, and duplicate receipt
  replay without row-oriented staging.
- Live Atlas collection-watch iteration: package
  `pkg-portable-mongo-live-atlas-collection-typed-cdc-e2e-73369-1786318414455677000` resumed from
  its prior event token and atomically applied one complete post-image update plus one exact hard
  delete. Its receipt records `rows_updated=1`, `hard_deletes=1`, and
  `missing_delete_keys=0`; the proposed delta advances from the prior event token to the terminal
  event-issued resume token. Read-only DuckDB inspection returned only the updated `-a` document
  (`status=collection-updated-a`, `amount=2201`) and no deleted `-b` document. This run used the
  debug binary as an integration iteration; the required production-profile final certificate
  remains separately open.
- Live Atlas typed database snapshot handoff: portable plan
  `atlas-database-typed-cdc-snapshot1.plan.json` opened one database stream before scanning the two
  compiled collections and package
  `pkg-portable-mongo-live-atlas-database-typed-cdc-snapshot-83998-1786319275018883000` atomically
  committed 32 independently typed snapshot upserts (9 invoices and 23 orders) across two routed
  tables under one receipt/checkpoint. A document written during the handoff window is present in
  the orders target. The next portable plan resumed from that checkpoint, applied only one
  post-token update (not the 32-row snapshot), and the destination now contains
  `status=handoff-resumed`, `amount=4401`. These were debug-binary integration iterations against
  authorized Atlas; the separate production-profile full-database CDC certificate above remains
  the final deployment-profile proof.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-mongodb --lib` — all 61 MongoDB adapter tests
  pass after snapshot handoff, including multi-row typed route authority and primary-only snapshot
  bootstrap validation. Strict affected-package Clippy passes for MongoDB, builtin composition,
  project, and CLI; the explicit cognitive-complexity diagnostic reports only the pre-existing
  kernel Arrow type parser and no changed MongoDB function.
- Final production-profile continuous Atlas certificate: portable plan
  `.cdf/atlas-live-continuous2.plan.json` watched the admitted Atlas database from one long-lived
  process and committed one orders event followed by one invoices event as two epochs. Each epoch
  entered its next Read phase immediately after checkpoint settlement. The first package was
  collected after the second checkpoint (`22` files / `132 KiB` reclaimed), while the newest
  package retained its Arrow payload and the older package retained only manifest/receipt proof.
  First SIGINT during the idle third stream wait printed the graceful-stop notice and exited `0`.
  The final report truthfully aggregated `2` rows, `19 KiB`, `2` segments, and `1m 57s`. Read-only
  DuckDB inspection returned the tagged order and invoice in distinct physical tables
  `atlas_database_typed_cdc_continuous__cdf_cdc_acceptance_orders` and
  `atlas_database_typed_cdc_continuous__cdf_cdc_acceptance_invoices`, with their exact
  `source_collection` routing values and no residual variant evidence. The binary was built with
  the normal optimized release profile; neither source nor any validation command used
  `RUST_MIN_STACK`.
- Production-profile envelope Atlas certificate: after focused adapter test/check/strict Clippy,
  `.cdf/atlas-envelope-final2.plan.json` passed portable preflight and resumed the admitted
  two-collection database watch from its committed token. It committed exactly two tagged events
  as two routed segments under one package/receipt/checkpoint. Read-only DuckDB inspection found
  the order only in `atlas_database_cdc__cdf_cdc_acceptance_orders` and the invoice only in
  `atlas_database_cdc__cdf_cdc_acceptance_invoices`; `source_collection` matched each route and
  `_cdf_variant` was null. The stored deterministic Canonical Extended JSON preserved the order's
  Decimal128 as `$numberDecimal`, the invoice's Int64 as `$numberLong`, timestamps as `$date` plus
  `$numberLong`, and nested document/array structure. This used the normal optimized release
  binary and the authorized Atlas database.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-source-mongodb
  envelope_cdc_accepts_the_framework_residual_column --lib --locked` passed, reproducing the
  discovered logical envelope shape with source-name metadata plus the genuine framework residual
  field. `DUCKDB_DOWNLOAD_LIB=1 cargo check -p cdf-source-mongodb --all-targets --locked` and
  strict affected-crate Clippy passed.
- Local replica-set certificate: the existing MongoDB 8.0.13 `rs0` sandbox source at
  `localhost:27020` passed discovery, compilation, portable planning, and collection CDC without a
  resource cursor. One epoch reduced two updates of `_id=1001` to the final post-image and applied
  the hard delete of `_id=1002`; DuckDB contained only `id=1001`, `status=cdc-local-final`, and
  `amount=202.2`. Re-running the retained package through `cdf run --package` returned the existing
  receipt as an idempotent no-op with no second destination load.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-project
  mongo_event_prefix_drain_recovers_receipt_checkpoint_crash_without_source_reopen --lib --locked`
  passed after bringing its destination fixture onto the finalized atomic CDC boundary. It proves
  receipt-before-checkpoint recovery commits the exact terminal Mongo token without reopening the
  source or writing destination effects twice.
- `DUCKDB_DOWNLOAD_LIB=1 cargo test -p cdf-engine
  package_identity_is_invariant_to_source_batch_rechunking --lib --locked`,
  `fixed_drain_epoch_packages_are_jobs_invariant`, and the randomized jobs-invariance matrix all
  passed. The first test's one-batch/many-batch identity assertions remained intact; its serialized
  golden was refreshed only after tracing the intended compiled-admission evidence added by
  `daf49326c`.

## Review

Not started.

## Retrospective

- Live Atlas acceptance found integration defects that adapter-only fixtures could not: logical
  versus physical envelope metadata, idle-stream shutdown, per-epoch collection cost, and aggregate
  reporting. Keeping the production binary and destination inspection in the acceptance loop was
  essential.
- MongoDB database CDC is safest when discovery inventories ordinary collections deterministically
  and runtime treats any uncompiled collection as a stop condition. Change-stream traffic is not a
  schema-discovery sample.
- Resource cursors and native CDC positions are different authorities. Keeping Mongo resume tokens
  below the resource grammar avoided forcing cursor semantics onto replace/bounded resources.
