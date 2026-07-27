Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Pre-production current-only policy

## Context

CDF has no external production deployment and the project is customer zero. Preserving obsolete
CDF artifacts, internal APIs, command behavior, fallback execution paths, and fixtures imposes
permanent complexity without serving a real consumer.

This is not permission to ignore compatibility with external systems or to delete correctness
fences.

## Current-only rule

CDF keeps one current product representation and one current execution happy path per declared
capability. When a replacement is correct and verified:

- remove the superseded implementation;
- migrate current repository configuration/fixtures/generated artifacts in the same tranche;
- delete tests that assert only obsolete CDF behavior;
- remove abandoned feature flags, dependencies, build scripts, adapters, and docs;
- retain terminal ticket/evidence history rather than runtime compatibility code.

No generic compatibility shim, dual reader/writer, deprecated alias, schema migration command, or
fallback exists merely because an earlier development revision produced a different artifact.

## State and migration seam

The current state schema may be called V1 and use a versioned envelope so a future migration system
has an explicit seam. CDF does not need a `state migrate` product command or readers for
pre-release state variants today.

When real deployed state exists, a future migration program must name:

- supported source versions;
- ownership and backup;
- atomicity and rollback;
- compatibility window;
- deployment ordering;
- deletion trigger for old readers.

Until then, repository fixtures and local customer-zero state are regenerated or deliberately
discarded when the current schema changes.

## Compatibility that still matters

Do not confuse old-CDF compatibility with external protocol interoperability. CDF should maximize
supported, governed mappings for:

- Arrow types and metadata;
- Parquet/CSV/JSON/NDJSON/IPC and future native formats;
- DuckDB, PostgreSQL, Parquet, Iceberg, and future destinations;
- S3/GCS/Azure/HTTP/local transports;
- SQL/catalog/object-store/provider protocol versions;
- Python, Arrow C Data Interface, WASM, DataFusion, and distributed task protocols as activated.

When an external destination cannot express an Arrow value exactly, use the destination sheet,
type fidelity classification, declared lossy policy, residual/quarantine, or a best-effort native
representation such as PostgreSQL JSONB. “Unsupported” is a last resort, not the easiest branch.

External compatibility behavior must be current, measured, and conformance-tested. It is not
legacy merely because the protocol is old.

## What must never be removed as “legacy”

- package/receipt/checkpoint identity checks;
- secret redaction and egress policy;
- fail-closed authority validation;
- transactional rollback and idempotency;
- schema reconciliation/coercion verdict evidence;
- quarantine/residual preservation;
- crash recovery and staging lease fences;
- current artifact verification;
- required external protocol fallbacks declared by capability;
- tests that catch a present invariant violation.

These are Chesterton fences. Replace them only through a ratified semantics decision and equivalent
or stronger proof.

## Deletion test

Before retaining an old path, answer:

1. Which current external consumer or protocol requires it?
2. Which active spec/decision owns that compatibility?
3. How is the path exercised in conformance?
4. What is the deletion trigger?
5. What measurable cost does the duplicate path impose?

If there is no current consumer or active authority, delete it. If the compatibility is real,
represent it as a capability, not an identity-specific branch.

## Historical artifacts

Terminal `.10x` records remain valuable. They explain:

- why an approach was falsified;
- which benchmark or failure exposed it;
- what replacement won;
- which future trigger could reopen the investigation.

Preserve that history while deleting the code. History belongs in records, not in runtime branches.

## Review signal

Repeated signs that obsolete behavior survived:

- tests use “legacy,” “old,” “v0,” “compat,” or “fallback” without an active external protocol;
- two functions create the same artifact format;
- a feature flag chooses between old and new CDF internals;
- generic runtime matches on a source/destination name;
- a current writer emits one format but a reader accepts several extinct CDF versions;
- an error recommends migration for state no real deployment can possess;
- docs describe both experimental and retained happy paths.

Treat these as deletion candidates during every architecture ticket.
