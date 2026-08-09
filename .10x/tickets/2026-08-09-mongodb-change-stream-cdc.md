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

## Blockers

- `.10x/tickets/2026-08-07-a6-2-routed-target-families.md` must establish generic heterogeneous
  output schemas and atomic destination family settlement.

## Evidence

None yet.

## Review

Not started.

## Retrospective

Pending execution.
