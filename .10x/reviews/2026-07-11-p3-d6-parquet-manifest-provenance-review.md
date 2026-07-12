Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-d6-compact-provenance-conformance.md
Verdict: pass

# Parquet manifest-provenance review

## Findings

No critical or significant finding remains in the Parquet slice.

- The logical address remains kernel-owned and identical to the DuckDB/Postgres correction address; the object-key/ordinal location does not leak into generic runtime code.
- The adapter reuses its canonical object manifest rather than adding a second provenance model or payload columns.
- The alias is immutable and content-verified. Receipt verification rejects disagreement between the package-token and target/package views.
- Crash recovery heals the manifest-published/alias-not-yet-published boundary before accepting duplicate completion. Correction publication cannot begin from an unresolvable base address.
- Long identifiers are stored once per manifest/object entry, not once per row, so provenance does not tax Parquet encoding throughput.

## Verdict

Pass for the Parquet/file implementation slice. The architecture preserves a homogeneous public provenance contract with an efficient destination-native physical representation and introduces no generic destination branch or compatibility path.

## Residual risk

The shared D6 cross-adapter generated matrix remains open. Manifest metadata grows with segment count and belongs in the program-wide metadata retention/inspection policy; it is not row-cardinality growth and no deletion semantics are invented here.
