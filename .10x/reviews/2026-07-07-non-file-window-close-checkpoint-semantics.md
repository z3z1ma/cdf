Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: crates/cdf-project/src/runtime.rs
Verdict: pass

# Non-file window-close checkpoint semantics review

## Target

Review of the project-runtime state-delta changes for `.10x/tickets/done/2026-07-07-non-file-window-close-checkpoint-semantics.md`, governed by `.10x/decisions/non-file-window-close-checkpoint-semantics.md`.

## Assumptions tested

- Numeric, timestamp, and date cursor arithmetic is the only ratified non-file window-close advancement surface.
- Page tokens are pagination transport, not durable checkpoint high-water marks.
- Checkpoint advancement must fail before checkpoint proposal or commit when source-position semantics are ambiguous.
- Exact zero-lag REST cursor behavior remains intact.
- State-delta `output_position` is the checkpoint high-water mark, while `StateSegment.output_position` remains per-segment evidence consumed by destination mirrors and artifacts.

## Variant mapping

- `SourcePosition::Cursor` with schema `int64`: supported; aggregate max `CursorValue::I64`, subtract lag with checked signed arithmetic, and preserve raw segment cursor evidence.
- `SourcePosition::Cursor` with schema `uint64`: supported; aggregate max `CursorValue::U64`, subtract lag with checked unsigned arithmetic, and preserve raw segment cursor evidence.
- `SourcePosition::Cursor` with schema timestamp: supported; aggregate max `CursorValue::TimestampMicros`, subtract lag milliseconds converted to micros with checked arithmetic, and preserve raw segment cursor evidence.
- `SourcePosition::Cursor` with schema `date32`: supported; aggregate max epoch-day `CursorValue::I64`, subtract only whole-day lag; non-day-aligned lag fails closed. Schema lookup is required because `date32` materializes as `CursorValue::I64`.
- `SourcePosition::PageToken`: fail closed as page-token-only checkpoint state.
- `SourcePosition::Composite` containing cursor and page token: fail closed as mixed cursor/page-token state.
- `SourcePosition::Log`, `FileManifest`, or `ForeignState` for non-file cursor resources: fail closed as divergent source-position variants.
- Cursor field mismatch: fail closed before state delta construction returns a checkpointable output position.
- Schema/value kind mismatch, string cursors, decimal-string/float cursors, and unsupported schema types: fail closed as unsupported cursor value kinds.
- Unordered cursor descriptors: fail closed before extraction writes.

## Findings

- Parent correction: the worker patch initially wrote the aggregated checkpoint position into every `StateSegment`. Parent review changed this so `StateDelta.output_position` is window-closed while each `StateSegment.output_position` remains the raw observed segment position, then reran focused and full `cdf-project` gates.
- No blocking findings remain.

## Residual risk

Date support is limited to `date32` because that is the date cursor shape currently materialized by declarative REST. `Date64`, decimal-string/float, string, page-token-only, mixed page-token/cursor, foreign, log, and arbitrary composite state semantics intentionally remain unimplemented and fail closed pending future ratification. CodeQL was not rerun for this focused slice; the evidence does not claim current-tree CodeQL coverage.
