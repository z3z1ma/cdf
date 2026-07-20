Status: active
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/done/2026-07-19-iceberg-f1-neutral-object-access.md, .10x/tickets/done/2026-07-19-iceberg-f2-dependency-foundation.md, .10x/tickets/done/2026-07-19-iceberg-f3-table-snapshot-position.md

# Iceberg I1: catalog bindings, discovery, and compiled snapshot plan

## Scope

Implement the Iceberg source driver, config/schema, REST and Glue catalog bindings, generation-bound metadata reuse, exact schema discovery/pinning, compiled snapshot physical plan, add/deep-validation/inspect/doctor hooks, and local filesystem/REST discovery conformance.

## Non-goals

No data-file scan, deletes, incrementality, Glue conventional external tables, catalog mutation, or generic command branch.

## Acceptance Criteria

- Local/filesystem, REST, and Glue bindings produce identical table/snapshot semantics.
- Discovery reads metadata only, preserves Iceberg field/schema metadata, pins through ordinary CDF snapshots, and reuses same-command observations.
- The compiled plan binds exact catalog/table/ref/snapshot/metadata/schema/spec/predicate/capability authority with redacted options.
- Add/deep validation/inspect/doctor use registry hooks; local REST conformance and jobs-independent plan hashes pass.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/specs/source-extension-runtime-contract.md`

## Assumptions

- User-ratified 2026-07-19: REST neutral interface with Glue as binding.

## Journal

- 2026-07-19: Activated after F1-F4 closure. This lane owns only metadata/catalog discovery and compiled snapshot planning; payload reads/deletes remain I2. The implementation will compose injected object access and the source registry, and will not introduce generic Iceberg/Glue branches.
- 2026-07-19: Added strict typed source/resource configuration and a catalog-local registry with filesystem, Iceberg REST, and injected Glue bindings. All object metadata reads use `cdf-object-access`; REST uses injected `cdf-http`; Glue exposes only a CDF-owned pointer contract to its host adapter. Metadata bodies, JSON parse amplification, REST control responses, and Glue pointer state are charged to the shared discovery ledger through explicit configurable limits.
- 2026-07-19: Corrected REST routing to follow the protocol lifecycle: `GET /v1/config?warehouse=...` negotiates `uri`/`prefix`, then load-table uses the negotiated path. Warehouse is never treated as a path prefix. Both responses are retained for same-command reuse and report actual transferred bytes without counting internal copies.
- 2026-07-19: Catalog selection now freezes exact snapshot/ref/timestamp authority, accepts truly empty current tables without fabricating a snapshot, rejects invalid refs and unsupported format versions, and preserves Iceberg field/schema IDs, docs, defaults, source names, required flags, and physical types in the Arrow schema.
- 2026-07-19: Focused verification: `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib` passed 14 tests; `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --lib -- -D warnings` passed. These observations cover typed configuration, protocol routing, schema conversion, task invariants, and lint cleanliness; driver/manifest/live catalog conformance remains pending.
- 2026-07-19: Installed the Iceberg driver boundary and a canonical external-task planner. Discovery retains the exact generation-bound table metadata for same-command resolution; the physical plan contains only typed/redacted source and resource options; planning reads manifest-list and manifest bytes through the injected object-access authority and writes append/spill-backed task artifacts through `cdf-task-store`. Task and concurrency limits are explicit user knobs rather than hidden caps.
- 2026-07-19: Corrected a schema-evolution authority defect before execution: the task-set now records one fixed `output_schema_id`, while every data task records its historical `file_schema_id`. Projection and equality-delete semantics bind to the output schema; partition tuples bind to the file schema and its historical partition spec. Conflating these would have rejected valid evolved tables or decoded partitions under the wrong schema.
- 2026-07-19: Focused verification after driver/planner integration: `CARGO_BUILD_JOBS=12 cargo test -p cdf-source-iceberg --lib` passed 14/14; `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-source-iceberg --all-targets -- -D warnings` passed. This proves crate compilation/lint cleanliness and existing catalog/config/task assertions; local catalog fixtures and task-planner behavioral coverage remain pending.
- 2026-07-19: Implemented the production Glue `GetTable` binding without importing the AWS SDK runtime into the source. The binding signs through official AWS SigV4 types, sends through CDF's injected HTTP transport and exact egress scope, resolves only redacted `secret://` or standard environment credentials, bounds both request and response memory, propagates Glue `VersionId` as catalog generation, and admits only `table_type=ICEBERG` plus an exact nonempty `metadata_location`. HTTP request bodies are now a generic zero-copy `Bytes` capability whose diagnostics expose only byte length.
- 2026-07-19: Corrected Hadoop filesystem metadata selection to the Apache reference lifecycle: `version-hint.text` is best-effort authority, later versions are admitted only through a contiguous chain, and malformed/stale hints fall back to the highest canonical metadata version. Discovery identity now includes catalog generation so reuse cannot cross a changed pointer.
- 2026-07-19: Added real local catalog conformance through the public driver boundary. A filesystem fixture exercises the actual file transport, exact empty-table metadata discovery, prepared-observation reuse, external task-store planning, and health hook. A REST fixture proves `/v1/config` negotiation followed by the negotiated load-table route, then asserts resolution and negotiation perform no third catalog request.
- 2026-07-19: Focused verification for the catalog tranche: strict `cargo clippy` over `cdf-source-iceberg`, `cdf-http`, and `cdf-transport-http` passed with all targets and `-D warnings`; Iceberg tests passed 19/19, HTTP transport tests passed 16/16, and HTTP policy tests passed 6/6. The transport suite includes a loopback POST assertion proving body delivery and redacted diagnostics. The workspace CLI check is temporarily blocked by unrelated uncommitted run-report API migration in another lane; the Iceberg CLI composition root had compiled before that migration and remains source-isolated.
- 2026-07-19: Pre-commit integration review found and removed conflicting blocking-lane authority: the composition root had provisionally injected `u16::MAX` into local object access while compiled resources record their configured ceiling. The runtime factory now receives the exact compiled lane and passes it unchanged into object access. The local conformance host now rejects conflicting declarations like production, and the 19-test suite remains green; no hidden concurrency cap or duplicate executor policy remains.
- 2026-07-19: Supply-chain gates for the Glue signing graph passed: `cargo vet --locked` succeeded after adding the 15 new locked transitive versions to the existing explicit exemption backlog, `cargo deny check` reported advisories/bans/licenses/sources all okay, and `cargo audit` found only the separately ratified `paste 1.0.15` maintenance warning. No Glue SDK, TLS stack, or independent HTTP runtime entered the graph.
- 2026-07-19: Removed a latent default-concurrency failure before payload work: the portable `u16::MAX` knob is now explicitly `RuntimeResolvedRequired`, then tightens to the admitted host CPU slots and is injected unchanged into object access. This is not a product cap; the plan retains the user's ceiling and the host derives the executable ceiling. The conformance host proves an uncapped plan resolves to its two available slots rather than constructing a 65,535-worker pool. Iceberg tests remain 19/19 and strict Clippy remains green.
- 2026-07-19: Separated shared host-pool authority from per-source scheduling authority. The single Iceberg blocking lane resolves once from host capacity and therefore composes across resources with different `maximum_concurrency` knobs; each source retains its own recorded scheduler ceiling. This removes both conflicting lane declarations and accidental per-resource worker pools.
- 2026-07-19: Added a real nonempty v2 table fixture using upstream Iceberg manifest and manifest-list writers. The manifest list is intentionally reverse-ordered, contains one historical-schema file and one current-schema file, and plans through the actual filesystem catalog/object-access/task-store path. The test proves canonical path ordering, contiguous ordinals, output schema 1 versus file schemas 0/1, exact 8-row/333-byte estimates, per-task validation, and identical task-set plus `ScanPlan` hashes at source concurrency 1 and 16.
- 2026-07-19: Replaced sequential manifest loading with a bounded host-owned planning window. Up to `min(manifest count, source knob, admitted host slots)` workers fetch and parse metadata through the injected object authority; a coordinator assigns only one bounded window, retains at most that window's accounted payload/parse state, and emits tasks strictly in canonical manifest order into the spill-backed store. No independent runtime/pool, fixed fallback cap, completion-order identity, or all-task materialization was introduced. Full Iceberg tests passed 20/20 and strict all-target Clippy passed.
- 2026-07-19: Added Iceberg's canonical gzip table-metadata form (`<version>-<id>.gz.metadata.json`) without an expanded-buffer escape from the ledger. CDF retains the already-accounted compressed object, performs one bounded decoded-size/CRC pass against `maximum_metadata_bytes`, reserves parse amplification from the decoded size, and then streams JSON decoding directly from gzip. Hadoop `vN` and UUID metadata version selection now treats `.gz` as a codec suffix rather than part of the version. Real filesystem discovery through `version-hint.text`, focused corruption/expansion tests, and strict all-target Clippy pass.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
