Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-runtime/src/format.rs, crates/cdf-source-files/src/runtime.rs, crates/cdf-cli/src/source_registry.rs
Verdict: pass

# Standard transform registry review

## Findings

No critical or significant findings. Implementation dependencies terminate at the CLI composition root; neither `cdf-runtime` nor `cdf-source-files` imports a transform implementation. Registry lookup returns trait objects and validates ids. Strong-magic matching detects overlapping registered signatures at selection time instead of silently choosing iteration order.

One expected open condition remains: transform selection and checksum-gated spooling are not connected in this slice, so the injected registry is not yet execution authority. P3 B1 already owns that work.

## Residual risk

Character BOM signatures all intentionally belong to the single `text_auto` driver. Magic-less Brotli and explicit character encodings cannot be inferred by strong magic and must remain extension/explicit selections under the catalog rules.
