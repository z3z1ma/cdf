Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-d3-postgres-binary-copy.md
Verdict: pass

# D3 closeout review

## Findings

No critical or significant issue remains. PostgreSQL type mapping, binary framing, staging tables, transaction state, and receipts stay inside `cdf-dest-postgres`; no PostgreSQL type assumption entered generic runtime or engine code. The destination consumes bounded leased batches and advertises only `copy_binary`.

CSV production ingestion, scalar staging rows, and the unimplemented extended-insert capability were deleted under the current-format-only decision. Benchmark CSV code is an isolated before/after control, not a runtime fallback.

The exact supported matrix has live/vector coverage including NUMERIC/Decimal128. Unsupported schema planning remains field-specific and fail-closed. Transaction tests prove staged invisibility, rollback, duplicate idempotency, and correction semantics.

## Verdict

Pass. D3 is complete.

## Residual risk

The remote environment matrix remains useful but the 15.96x encoder/server headroom makes it a shared D5 deployment-envelope concern, not missing adapter functionality.
