Status: active
Created: 2026-07-07
Updated: 2026-07-07

# CommitSession Segment Write API

## Context

`VISION.md` 14.1 describes destination sessions as segment-writing commit protocols. The P0 stop-line decision `.10x/decisions/p0-structural-debt-stop-line.md` makes this shape mandatory before CDF widens into new destination, source-archetype, or streaming-supervisor lanes.

Current source has a non-streaming compatibility shape:

- `crates/cdf-kernel/src/destination.rs` defines `CommitSession::write(&mut self) -> Result<()>`, with no segment payload.
- `DestinationProtocol::begin` has an error-returning default.
- DuckDB, Parquet, and Postgres sessions are constructed with package context and then commit a fully materialized package during `write()`.
- Package readers expose `read_all_segments()`, which eagerly loads every segment before destination commit.

This blocks streaming package-to-destination commit, byte-bounded destination backpressure, generic per-segment conformance, and future per-segment retry semantics.

The kernel must stay Arrow-only and must not depend on `cdf-package`. Parquet receipts also distinguish three byte notions: requested state segment byte count, package IPC byte count, and written Parquet object byte count. The session payload must preserve that distinction.

## Decision

CDF will use a synchronous MVP per-segment write API:

```rust
pub struct CommitSegment {
    pub state: StateSegment,
    pub package_byte_count: u64,
    pub batches: Vec<RecordBatch>,
}

pub trait CommitSession {
    fn apply_migrations(&mut self) -> Result<()>;
    fn write_segment(&mut self, segment: CommitSegment) -> Result<SegmentAck>;
    fn finalize(self: Box<Self>) -> Result<Receipt>;
    fn abort(self: Box<Self>) -> Result<()>;
}
```

`CommitSegment::state` carries the state/checkpoint-facing segment identity, row count, requested byte count, scope, and output position. `package_byte_count` carries the canonical package IPC byte count from the manifest. `batches` carries the Arrow data payload for that segment.

`write_segment` acknowledges that the session accepted the segment into the destination commit session. The durable settlement remains the `Receipt` returned by `finalize`. A destination may stage segments in memory, stage them in a temporary destination structure, or perform its actual write after the last expected segment, but it must not produce a successful final receipt until all expected segments have been accepted and written according to the destination contract.

`DestinationProtocol::begin` becomes required; the error-returning default is removed.

`DestinationProtocol` also exposes trait-level receipt verification:

```rust
fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification>;
```

`ReceiptVerification` is a kernel type with `verified`, `receipt_id`, and `reason`. Destination-specific inherent `verify_receipt` helpers may remain as thin wrappers for compatibility, but generic replay/recovery paths must be able to verify through `dyn DestinationProtocol`.

`cdf-package` will expose a helper that turns a verified package plus `DestinationCommitRequest.segments` into ordered `CommitSegment` values. Package replay must feed those values to the session one at a time. This keeps the kernel free of package-manifest types while still preserving package IPC byte counts for receipts and audits.

## Alternatives considered

Use an async `SegmentStream` trait object in the kernel immediately.

Rejected for Workstream A. It matches the book's eventual spelling, but it would force async trait and lifetime choices across every destination before the current synchronous destinations are structurally cleaned up. The per-segment call is compatible with a later async stream adapter without another semantic migration.

Pass `cdf_package::SegmentEntry` directly to `CommitSession`.

Rejected. It would make the kernel depend on package internals, violating the layer rule that the kernel owns meaning over Arrow and core artifact values without depending on upper crates.

Use only `StateSegment` plus Arrow batches.

Rejected. Parquet and audit evidence need the package IPC byte count independently from the state/requested segment byte count and the destination's physical object byte count.

Keep `write()` and add optional streaming later.

Rejected by the P0 stop-line. Every new destination implemented against `write()` raises migration cost and keeps byte-bounded destination backpressure out of reach.

Make `SegmentAck` mean durable per-segment destination commit.

Rejected for MVP. Current MVP destinations commit at package/target granularity, and pretending segment durability exists before the final receipt would overstate the guarantee. `SegmentAck` means accepted into the session; the receipt remains settlement.

## Consequences

DuckDB, Parquet, and Postgres sessions must collect or stage `CommitSegment` values and validate them against the planned `DestinationCommitRequest`.

Destination tests and conformance must call `write_segment` per segment. Compatibility helpers should be updated rather than adding a second no-data write path.

Generic recovery/replay code can verify receipts through `DestinationProtocol::verify`, which is required for Workstream B's open orchestrator path.

Future async streaming can wrap `write_segment` or supersede this decision with an async `SegmentStream` decision after P0 A-C close and benchmark/backpressure evidence justify the migration.
