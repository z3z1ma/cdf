Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/decisions/explicit-sampled-discovery-and-residual-promotion.md, .10x/specs/residual-variant-capture.md, .10x/specs/schema-promotion-corrections.md

# P2 residual schema capture and promotion program

## Scope

Parent plan: extend `_cdf_variant` from selected nested-column capture into exact field/path residual preservation, retain stable row provenance across destinations, and add dry-plan plus crash-safe execution for `cdf schema promote` through packages, receipts, checkpoints, leases, and atomic pin publication.

## Child sequence

- RP1 residual envelope codec/model and RP3 destination capability/provenance model may start in parallel.
- RP2 compiles and executes residual verdicts after RP1.
- RP4 adds schema-scope leases and atomic lock compare-and-swap independently of destination correction.
- RP5 builds the no-write promotion planner after RP1/RP3/RP4 and A10 sampled/effective schema facts stabilize.
- RP6/RP7/RP8 implement Postgres, DuckDB, and Parquet strategies independently after RP2/RP3.
- RP9 composes correction execution, recovery, and GC availability after RP4-RP8.
- RP10 owns conformance and closure.

## Integration points

All children use canonical Arrow/package artifacts, `ValidationProgram`, `DestinationSheet`, `ScopeKey::SchemaContract`, destination sessions, receipts, and `CheckpointStore::commit`. No child may create a destination-only shortcut or source-format-specific promotion path.

The distributed execution ticket remains owner of general worker/partition scheduling. RP4 provides only the executor-neutral fenced scope-lease primitive promotion requires and must be reusable by that later ticket.

## Acceptance criteria

- Unknown/scalar/path violations preserve conforming row projections through exact residual envelopes when safe.
- Unsafe control-field/framing violations quarantine with named rules.
- Append remains keyless; CDF provenance addresses committed rows without inferred business keys.
- Destination sheets declare and conformance-test correction/readback strategies.
- `cdf schema promote` dry-runs without writes and execution publishes a pin only after correction receipts/checkpoints settle.
- Crash recovery is deterministic at every promotion boundary.
- GC reports when promotable residual bytes are being removed and never claims unavailable readback.
- Existing package bytes, commit gate, replay, and schema-pin determinism remain intact.

## Evidence expectations

Focused child evidence/reviews, residual round-trip properties, package/replay inspection, destination correction conformance, lease fencing, crash matrix, lock compare-and-swap, GC availability, full workspace gates, and an independent parent review.

## Explicit exclusions

No implicit promotion, arbitrary user UPDATE SQL, indefinite retention, inferred semantic keys, cross-resource migration, distributed scheduler, or perpetual schema mutation.

## Progress and notes

- 2026-07-10: Opened after the user confirmed the exact selector, residual envelope/safety, and promotion lease/correction/retention contracts. Source audit confirmed Postgres already persists `_cdf_load`, `_cdf_segment`, and `_cdf_row`; the program generalizes that existing provenance rather than inventing a parallel row-id system.
- 2026-07-10: RP1 closed with integrated evidence and review. The exact residual codec is available; RP2 still waits for A10d while RP3/RP4 remain independently executable.
- 2026-07-10: RP3 and RP4 closed after P0 extension-cost review. Destination protocol capabilities, stable row provenance, store-authoritative fenced leases, and guarded atomic lock publication are available. RP5 now waits only for A10g; RP6-RP8 still wait for RP2/A10d.

## Blockers

None. Child dependencies govern execution.
