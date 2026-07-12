Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transform-character
Verdict: pass

# Native character transforms review

## Assumptions tested

- Auto mode only uses a BOM and otherwise selects UTF-8; it never guesses locale encodings.
- Explicit/BOM conflicts fail rather than overriding configured authority.
- Arbitrary chunk boundaries cannot split validation state incorrectly.
- UTF-16 surrogate/endianness and Windows-1252 undefined values never become replacement characters.
- Output and internal carry stay bounded and accounted.
- Performance evidence does not hide input/collector copies under an unfair reference label.

## Findings

No critical or significant leaf-driver defect remains. Tests cover every catalog encoding, BOM authority, one-byte boundaries, supplementary Unicode, and invalid inputs. Internal carry is closed at four bytes and UTF-8 uses bulk validation/copy.

The first benchmark failed at 0.238x due to three CDF-side copies versus one reference copy. The test source was made zero-copy and the timed sink changed to consume each streaming chunk once, matching how the runtime uses the transform. The resulting 2.316x is explicitly attributed to bounded output lifetime rather than superior validation compute.

## Verdict

Pass for the character-transform family.

## Residual risk

Error byte offsets across every malformed multibyte permutation need property/fuzz coverage. End-to-end text codec composition remains open.

