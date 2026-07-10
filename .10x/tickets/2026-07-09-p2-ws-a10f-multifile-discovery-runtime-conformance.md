Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/tickets/2026-07-09-p2-ws-a10e-file-quarantine-processed-positions.md, .10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md

# P2 WS-A10f multi-file discovery/runtime conformance

## Scope

Gate A10 with deterministic multi-file discovery, pin, compatible execution, drift quarantine, replay, budget, and preview/run parity conformance. Use local fixtures plus an injected transport facade so the law is not tied to local filesystem behavior.

## Acceptance criteria

- Parquet and Arrow IPC conformance cover cardinality one and many through the same discovery artifacts.
- Compatible union/widening/missing-null, metadata variance, add/remove/change manifest identity, immutable baseline, effective schema, package replay, and no-source-contact ordinary runs are asserted.
- Incompatible month/file drift completes with named quarantine evidence and correct manifest advancement/retry.
- Preview uses the same exhaustive file resolution/discovery/reconciliation/normalization front end and boundedly opens every planned partition; rendered rows remain under the global limit in deterministic order.
- A green preview cannot later fail run for a file/decode/schema reason within the bytes/batches preview actually inspected.
- Default and overridden executor budgets are visible; exhaustion is explicit and never changes membership or samples.
- Large-N deterministic fixtures prove bounded in-flight bytes/concurrency without requiring a production network.
- The friction registry and S2/S6/S8 rows are promoted only for the exact behavior proved; HTTP-template/cloud exclusions remain pending under WS-E/WS-I.

## Evidence expectations

Standalone conformance functions, registry owner validation, golden package hashes, recorded plan/package/manifest/quarantine artifacts, repeated determinism runs, full workspace/quality gates, and independent adversarial review.

## Explicit exclusions

No S3/GCS/Azure implementation, HTTP template enumeration, text inference, zip, distributed scheduler, or public-network CI dependency.

## Progress and notes

- 2026-07-09: Opened as the A10 closure gate; final public/cloud scenarios remain owned by WS-E/WS-I.

## Blockers

Depends on A10e.
