Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transform-gzip, .10x/tickets/done/2026-07-11-p3-b1-streaming-byte-transforms.md
Verdict: pass

# Native gzip driver adversarial review

## Assumptions tested

- Input and output ownership remain visible to the unified memory ledger.
- Arbitrary transport chunk boundaries do not alter gzip semantics.
- Concatenated members are not silently truncated at the first trailer.
- Header, deflate, trailer, checksum, size, expansion, cancellation, and no-progress failures terminate rather than emit invented bytes.
- Codec dependencies remain isolated below source/project/CLI orchestration.

## Findings

- No critical or significant finding exists within the implemented leaf-driver slice.
- Minor: the driver intentionally permits one output-chunk of streaming grace while compressed size is still unknown, then applies the exact configured ratio at every member trailer and terminal EOF. This avoids false early rejection caused by decoder burst shape while remaining bounded by the absolute expanded-byte ceiling.
- Significant program remainder, not a defect in this leaf boundary: a checksum-bearing transform can report corruption after prior output chunks were consumed. Product composition must therefore stage or rollback the corresponding decode publication window; it must not claim the codec-level no-partial-fatal-window law merely because the transform stream eventually errors. B1 remains open and explicitly owns this barrier.

## Verdict

Pass for the dependency-isolated native gzip driver milestone. Do not close B1 or delete the legacy gzip path until registry composition, publication atomicity, parity, fuzzing, and performance evidence land together.

## Residual risk

Malformed-header coverage is focused rather than fuzz-generated, and throughput is unmeasured. Both are acceptance obligations on the still-open B1 ticket.
