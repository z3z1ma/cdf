Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Destination bulk-path runtime

## Purpose and scope

This specification governs bulk-path declaration/preparation, schema eligibility, bounded input, fallback, tuning, evidence, and first-party strategy requirements. Commit, staged ingress, memory, host, and extension behavior remain governed by their focused specs.

## Descriptor and preparation

Every writable destination MUST publish at least one truthful bulk path or explicitly declare scalar-only with measured limits. Descriptors MUST be versioned data available through inspection/sheets. Runtime conformance MUST fail a declaration whose live path, accepted schema, concurrency, staging, or throughput identity differs.

Preparation receives the compiled destination mapping plan, semantic commit request, package schema/segment descriptors, destination introspection, and execution capabilities. It returns eligible paths in driver-owned priority order plus exact rejection reasons. Generic runtime MUST NOT match path ids, destination names, or field types to choose behavior.

Eligibility MUST be complete before payload mutation where schema/introspection can decide it. Type fallback cannot weaken exactness. If no path satisfies the mapping/guarantee, plan fails with field-level fixes.

## Input and acknowledgements

Bulk writers MUST consume the bounded durable-segment reader from `.10x/specs/streaming-operator-graph.md`. Each segment and yielded batch is identity-verified/accounted. Writers may release a batch only after its data is transferred to driver/native-library ownership that is itself accounted as CDF memory or declared external staging.

Per-segment acknowledgement MUST cover exact segment id, input rows/bytes, and accepted state. Final receipt counts/checksums/target transaction evidence MUST be derived from actual writer outcomes, not request values. Partial batch/segment acceptance is either fully resumable with exact identity or treated as failed attempt requiring rollback/redrive.

Row provenance MUST use the compact, lossless destination-neutral model in `.10x/decisions/compact-lossless-destination-row-provenance.md`. Bulk writers receive logical package/segment/row authority, may persist compact keys only with a bijective transaction-bound mapping, and MUST measure provenance overhead against the same native path without provenance. Repeating long package or segment identifiers per payload row is not an acceptable first-party default when it breaks the overhead envelope.

## Fallback and atomicity

Preflight fallback records why a faster path is ineligible. Runtime fallback after writer start requires idempotent abort/rollback, zero committed target visibility, a new load attempt, and full segment redrive. The run ledger records both attempts. A driver that cannot prove rollback MUST fail rather than switch.

Duplicate package-token behavior, append/replace/merge semantics, corrections, receipt verification, and checkpoint gate MUST be identical across eligible paths. Conformance executes every path and forced fallback, not only the default.

## Tuning and evidence

Path descriptors declare min/preferred/max rows/bytes, max useful writers, ordering, lane/internal CPU cost, and external staging. The host resolves settings under memory/CPU/jobs/destination constraints. Adaptive settings and queue pressure remain outside package identity.

Receipt/run evidence MUST record driver/path version, settings, attempts/fallback, rows/logical/physical bytes, encode/send/commit durations, server/native timing where trustworthy, and external staging identifiers after redaction. Rendering shows the path only in verbose/explain modes unless fallback/degradation occurred.

## First-party requirements

DuckDB MUST use an Arrow-native batch path by default. A5/WS-D compare appender-arrow and Arrow vtab/`INSERT SELECT`; scalar append is a field/schema-specific compatibility path only. DuckDB's single writer uses a declared pinned/shared blocking lane and does not retain package rows.

Postgres MUST implement binary `COPY` from Arrow with exact PostgreSQL binary encodings and null framing. There is no CSV/text compatibility path before production: unsupported mappings fail during preparation with field-level remediation. Staging/disposition SQL remains transactional or follows staged-ingress final binding with no target visibility.

Parquet destination MUST stream row groups/data files and hash/publish as batches arrive. Local publication and remote bounded-object upload MUST be atomic create-or-verify against immutable content identity and remain invisible to target reads until final manifest binding. Generic object-store multipart handles are process-local and therefore MUST NOT be used for externally staged publication; multipart is eligible only through a capability that exposes persistent provider upload identity, exact recovery/abort, and initiation-ambiguity handling to the runtime-owned lease authority. Output file/row-group sizing and upload mode are bounded and deterministic where they affect receipt/object identity; live pressure cannot change package identity. No full-table buffer is permitted.

## Conformance and performance

Shared laws MUST cover each path, schema eligibility/rejection, forced fallback, abort/redrive, duplicate packages, zero rows, mixed segments, slow destination, memory pressure, cancellation, crash boundaries, counts/checksums, receipt verification, append/replace/merge, and jobs invariance.

Every path requires same-harness throughput/copy/allocation/profile evidence. Default selection must choose the measured fastest exact path for its eligible schema/host class. DuckDB/Postgres/Parquet must meet their P3 envelope rows.

## Explicit exclusions

This spec does not standardize destination wire protocols, choose parser/database client dependencies, allow semantic fallback, or put host tuning into package identity.

Pre-production compatibility policy is governed by `.10x/decisions/pre-production-current-format-only.md`.
