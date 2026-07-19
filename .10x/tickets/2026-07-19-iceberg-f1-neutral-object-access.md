Status: open
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-19-iceberg-glue-source-program.md

# Iceberg F1: neutral object-access extraction

## Scope

Move reusable local/HTTP/S3/GCS/Azure metadata, listing, sequential/range `ByteSource`, client-pool, and transport composition authority out of `cdf-source-files` into one neutral crate consumed by the file source and future Iceberg/Glue sources. Preserve file-source semantics and measured hot paths exactly; delete the superseded source-local surface rather than keep a shim.

## Non-goals

No Iceberg dependency or protocol logic, source-position change, scan-task artifact, new transport behavior, performance tuning, generic project/runtime source-id branch, or AWS mutation.

## Acceptance Criteria

- `cdf-source-files` consumes neutral object access and retains only file-source planning/glob/discovery/compression/manifest behavior.
- The neutral crate exposes capability/request types sufficient for metadata, listing, sequential and exact-range `ByteSource` access with injected secret/egress/execution/memory authority.
- There is one client pool, retry/controller, cancellation, generation, telemetry, spool, and memory-accounting implementation; no compatibility re-export or duplicate helper remains.
- Existing local/HTTP/cloud file-source conformance and remote Parquet performance evidence remain unchanged within measurement noise.
- Architecture checks prevent source crates from depending on sibling sources and prevent generic runtime/project imports of concrete object-access implementations.

## References

- `.10x/decisions/iceberg-glue-source-boundaries.md`
- `.10x/specs/source-extension-runtime-contract.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/data-onramp-file-sources-transports.md`
- `.10x/decisions/native-format-driver-and-byte-source-boundary.md`

## Assumptions

- User-ratified 2026-07-19: neutral extraction with no compatibility shim is approved.
- Record-backed: `cdf-runtime::ByteSource` remains the payload interface; file-source-specific policy stays in `cdf-source-files`.

## Journal

- 2026-07-19: Opened as the first executable, crate-bounded Iceberg foundation lane. Existing dirty `cdf-runtime/src/worker_protocol.rs` belongs to another worker and is out of scope.

## Blockers

None.

## Evidence

Pending execution.

## Review

Pending.

## Retrospective

Pending.
