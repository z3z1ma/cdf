Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transform-brotli
Verdict: pass

# Native Brotli driver review

## Assumptions tested

- Native state is constructed only after working-set admission.
- EOF may still flush a decoder that needs output; it is not misclassified as truncation.
- Concatenated streams reset decoder state without losing unconsumed input.
- Large-window Brotli cannot escape the standard-window reservation.
- Corruption, truncation, expansion, cancellation, and stalled-decoder states fail closed.

## Findings

No critical or significant leaf-driver finding remains. The implementation uses `BrotliState::new_strict`, incremental input/output offsets, exact cursor consumption, and output leasing. Tests caught and corrected the important EOF-flush distinction: a decoder needing more output may complete after compressed input is exhausted, whereas `NeedsMoreInput` at EOF is truncation.

The 32 MiB native reservation is conservative rather than allocator-instrumented. It is explicitly owned by B1's RSS/stress evidence and does not justify unaccounted buffering elsewhere.

## Verdict

Pass for the leaf implementation.

## Residual risk

Brotli lacks checksums and magic, so automatic detection must never rely on content alone. Exact native allocator peak remains to be measured under the constant-memory stress harness.

