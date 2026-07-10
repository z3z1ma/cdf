Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-ws-a10g-explicit-sampled-binary-discovery.md, .10x/tickets/done/2026-07-10-p2-rp6-postgres-in-place-corrections.md, .10x/tickets/done/2026-07-10-p2-rp7-duckdb-in-place-corrections.md
Verdict: pass

# P2 sampled discovery and relational correction integration review

## Target

The A10g sampled-discovery implementation and the RP6/RP7 shared correction protocol plus Postgres/DuckDB physical implementations.

## Findings

### Resolved significant: correction settlement initially risked a parallel receipt protocol

The first shared-seam proposal introduced correction-specific receipt/count/verification values. That would have forced RP9 to translate destination evidence before `CheckpointStore::commit`. The implementation was repaired to reuse canonical `Receipt`, `CommitCounts`, and `ReceiptVerification`; only closed typed correction evidence is new. Correction state segments now let the ordinary receipt cover the future promotion checkpoint directly.

### Resolved significant: promoted JSON had no exact cross-destination meaning

RP3's `promoted_value_json` could not distinguish decimals, binary, temporal storage units, nested values, or non-finite floats. The active decision now makes a compiler-produced one-field `residual-json-v1` envelope the sole execution authority. Shared contract validation proves canonical bytes, path, and Arrow type before every adapter plan/begin boundary. Destinations consume the decoded Arrow value and compiled destination field; neither parses the legacy display field.

### Resolved significant: caller text and legacy display data could influence operation identity

Early request validation only checked a `sha256:` prefix and an early digest serialized the entire legacy plan. Kernel code now derives and recomputes the digest from an explicit closed authority projection and excludes `promoted_value_json`. Tests prove display changes leave identity stable while exact value/type/path changes alter it.

### Resolved significant: `cdc_apply` was briefly overloaded for schema correction

Schema correction is not source CDC and destinations may support one without the other. Canonical receipts now retain the resource disposition as target context, while versioned correction evidence declares `addressed_correction`. Capability authorization stays in `DestinationCorrectionCapabilities`; append remains keyless.

### Resolved significant: targetability claims initially under-validated provenance

Names plus a unique index are insufficient because nullable unique tuples admit multiple NULL addresses and wrong physical types cannot be targeted safely. Both relational adapters now require the exact three system columns, compatible physical types, NOT NULL, and tuple uniqueness before advertising/using targetability. Legacy targets fail with explicit rebuild/backfill remediation; no address is synthesized.

### Resolved significant: multiple source paths could target one destination field

The first request validator checked only path-to-field consistency. Postgres could then issue sequential last-writer-wins updates to one column. Kernel validation now enforces a path/output-field bijection, with adapter-local defenses and negative tests.

### Resolved significant: DuckDB dry planning used a write-capable connection

Correction planning was changed to a read-only connection. A regression asserts no WAL or database-byte change. Finalize owns one transaction for DDL, exact addressed updates, residual removal, and mirrors; stale-residual/failure tests prove rollback.

### Resolved significant: residual-readback capabilities lacked a generic falsifiable operation

The protocol now has a provided-unsupported `read_correction_residual` hook returning the exact original address and optional canonical bytes. Postgres and DuckDB implement read-only exact-tuple lookup and live tests reproduce/decode bytes before and after correction. Capability claims no longer rely on adapter-private helpers.

### Pass: source/destination extension invariant

Sampling selection is a format/transport-neutral project compiler stage over canonical candidate facts. Correction request, plan, session, receipt evidence, capability, and readback vocabulary are kernel/contract-owned with truthful unsupported defaults. Postgres and DuckDB contain only physical mapping, DDL/DML, locking, and transaction behavior. Generic orchestration contains no new concrete destination-name branch, and conformance can falsify capability claims.

## Verdict

Pass. All significant findings were repaired before the parent 913/913 all-feature workspace run and strict workspace Clippy gate. Acceptance criteria for A10g, RP6, and RP7 are supported by direct implementation, focused evidence, parent integration evidence, and failure-path tests.

## Residual risk

No residual risk lacks a durable owner. Remote multi-file sources remain WS-E; preview/runtime closure remains A10f/WS-I; Parquet sidecars remain RP8; promotion planning/execution/recovery/conformance remain RP5/RP9/RP10; distributed scheduling remains the distributed-execution ticket. These are downstream scoped capabilities, not hidden defects in the reviewed tranche.
