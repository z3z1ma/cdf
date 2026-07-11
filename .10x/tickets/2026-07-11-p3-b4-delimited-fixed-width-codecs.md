Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/2026-07-11-p3-b1-streaming-byte-transforms.md, .10x/tickets/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 B4: delimited and fixed-width codecs

## Scope

Implement streaming CSV/TSV/custom dialect parsing with safe quote-aware chunk parallelism and an explicit-layout fixed-width codec across catalog encodings, including bounded dialect/layout discovery suggestions.

## Acceptance criteria

- Pinned options cover catalog semantics; runtime never re-guesses dialect/layout.
- Quoted newlines, ragged/short/long rows, multibyte boundaries, null tokens, comments, and malformed records obey exact quarantine/error policy.
- Parallel and sequential results/package hashes match; unsafe split inputs automatically use sequential decode.
- CSV meets the envelope and fixed-width reaches the selected reference ratio.

## Evidence expectations

arrow-csv/reference benchmarks, adversarial dialect/encoding corpus, split-boundary properties, fixed-layout goldens, memory/cancellation, and jobs invariance.

## Explicit exclusions

No spreadsheet parsing.

## Blockers

Depends on transforms, FX1, and L5.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
