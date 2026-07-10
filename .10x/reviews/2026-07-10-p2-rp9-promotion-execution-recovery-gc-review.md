Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md
Verdict: concerns

# RP9 promotion execution adversarial review

## Assumptions tested

- A dry plan cannot be reconstructed during execution: execution stores and reuses the exact typed plan, old lock bytes/hash, version-3 snapshot, validation-program hash, target strategy, receipt associations, and source row addresses.
- Destination success is not checkpoint success: receipts are independently verified and checkpoints are committed afterward.
- Lock success is not publication success: lock CAS and the idempotent ledger record are separate, with a tested recovery branch.
- Replay cannot blindly rewrite: packaged/no-receipt replays the package; receipt/no-checkpoint verifies before commit; lock/no-event derives publication only from committed local checkpoints and receipts.
- GC cannot infer remote safety: its new report is limited to retained package bytes and makes no destination-readback claim.

## Findings

### Resolved — significant: validation-program hash initially used noncanonical hashing

The first end-to-end execution test rejected the staged snapshot because execution used a pretty/sorted JSON hash unlike lock generation. Execution now uses the crate’s canonical lock semantic hash and the test passes.

### Resolved — significant: Parquet runtime required materialization before protocol access

The generic executor initially accessed the protocol directly. Filesystem Parquet deliberately requires `ensure_protocol_ready()` first. The executor now invokes the runtime readiness hook before every correction settlement, preserving destination abstraction.

### Resolved — significant: post-CAS publication lacked a final lease assertion

An executor could have crossed lease expiry after lock CAS and before appending the event. A current-fence assertion now precedes publication. An expired executor leaves the narrow recoverable lock/event gap rather than publishing outside its lease.

### Open — significant: Parquet column identifier authority is absent

RP8 sidecar correction is implemented, but promotion planning rejects the Parquet sheet’s `object-key-component-v1` because it is an object-key rule, not a column normalizer. C3 explicitly deferred this semantic choice. The durable owner is `.10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md`; RP9 must not claim full cross-destination closure until it is resolved.

## Verdict

Concerns. The DuckDB execution/recovery/GC slice is well-supported and the generic executor preserves capability-driven destination sessions. Full RP9 closure is not yet coherent because Parquet sidecar promotion remains blocked at the correctly fail-closed identifier-policy boundary.

## Residual risk

- Postgres uses the same `in_place_update` session abstraction and has RP6 live/conformance evidence, but RP9 does not add a new live Postgres end-to-end promotion command test.
- Multiple-target execution is ordered and independently checkpointed, but the current crash matrix uses one target.
- The local SQLite lease duration is fixed at five minutes; long destination work is safe because fence checks prevent checkpoint/lock/event advancement after expiry, but it may require idempotent replay.

## Subsequent resolution

The open Parquet finding was resolved under `.10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md`; see `.10x/reviews/2026-07-10-parquet-promotion-identifier-policy-review.md` and `.10x/evidence/2026-07-10-parquet-promotion-identifier-policy.md`. This review's original `concerns` verdict is retained as historical truth for the RP9 slice at review time; parent closure may now reconcile it against the resolving review and evidence.
