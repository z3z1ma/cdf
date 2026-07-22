Status: cancelled
Created: 2026-07-21
Updated: 2026-07-22
Parent: .10x/tickets/done/2026-07-21-p3-d18-duckdb-reference-adapter-closeout.md
Depends-On: .10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md

# P3 D18E: DuckDB public-ABI scanner envelope

## Scope

Profile and optimize the sole stock-libduckdb public-C-API scanner's Arrow IPC decode, slice,
Arrow ownership transfer, vector-reference, file-claim, and callback overhead. Retain only changes
that narrow the measured stock-versus-reference gap without adding an ingress path or custom build.

## Non-goals

No nanoarrow/custom DuckDB runtime, deprecated `duckdb_arrow_scan`, high-level retaining VTab,
appender, unsafe code outside `cdf-dest-duckdb`, or speculative rewrite without a profile.

## Acceptance Criteria

- Micro and macro profiles quantify per-file schema/open cost, IPC decode, Arrow FFI conversion,
  vector-reference, callback count, and sink time for TLC and wide schemas.
- Ownership transfer remains exact-once, callback panics remain contained, schema/type mapping stays
  CDF-owned, and one worker owns each local reader state.
- Any retained optimization improves the relevant same-host median outside noise and regresses the
  other governed workload by no more than 3%.
- The final crate contains one scanner and no disabled alternative, feature flag, or compatibility
  residue.

## References

- `.10x/specs/destination-bulk-path-runtime.md`
- `.10x/decisions/duckdb-stream-scan-staged-ingress.md`
- `.10x/tickets/done/2026-07-18-p3-d14-duckdb-nanoarrow-080-lz4-revalidation.md`
- `.10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md`
- `https://duckdb.org/docs/lts/clients/c/api.html`

## Assumptions

- Record-backed: the stock raw scanner median was `5.645601216s` versus nanoarrow's `4.96s` exact-
  package median, while the stock full product was only about 8.5% slower and removed substantial
  release/extension complexity.
- User-ratified: simplicity may justify a modest residual gap, but obvious public-ABI optimization
  should still be measured and retained when it does not add architectural debt.

## Journal

- 2026-07-22: Cancelled by explicit user direction. D18A's authority-bound comparison already
  showed the sole stock public-C-API product scanner faster end to end than its independent raw
  comparator, while ordinary TLC ingestion exceeds four million rows per second. No profile-backed
  public-ABI change remained obvious enough to justify another hot-path experiment.

## Blockers

None. Cancellation is deliberate, not blocked.

## Evidence

`.10x/tickets/done/2026-07-21-p3-d18a-duckdb-wide-roofline-profile.md` records the retained scanner
and raw comparator profiles. No D18E implementation was attempted.

## Review

Cancellation keeps one scanner and adds no alternate path, feature flag, unsafe surface, or custom
build.

## Retrospective

A modest comparator gap is not an optimization mandate when the product path wins at the governed
boundary and another experiment would add complexity without a profile-backed target.
