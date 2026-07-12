Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-runtime/src/format.rs
Verdict: pass

# Byte-transform allocation authority review

## Findings

- Critical, resolved: the prior transform trait could not reserve output memory through neutral authority. Implementations would have required a hidden coordinator, unaccounted allocation, or full-buffer adapter. The signature was replaced rather than shimmed.
- Significant, resolved: descriptor maxima alone could not express a lower plan/run ceiling. Requests now bind lower-or-equal expanded-byte, ratio, and output-chunk limits.
- Significant, resolved: allocation attribution could silently use the wrong ledger class. Validation requires `MemoryClass::Transform`.
- Significant, open and owned: unknown input length complicates early ratio enforcement. The request records optional size authority; each B1 driver must combine total expanded-byte enforcement with a documented safe rolling policy when size is unknown.

## Verdict

Pass as the required neutral prerequisite. It adds no source, format, executor, transport, or destination type to the runtime contract.

## Residual risk

The contract is not closure evidence for B1. Driver implementations must reserve before each output allocation, release through `AccountedBytes`, enforce cancellation/checksum/truncation/concatenated-member semantics, and prove constant memory and reference throughput.
