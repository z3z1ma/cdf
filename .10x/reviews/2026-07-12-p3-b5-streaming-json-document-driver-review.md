Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-format-json/src/lib.rs; crates/cdf-source-files/src/runtime.rs; crates/cdf-project/src/schema_discovery.rs
Verdict: concerns

# Streaming JSON-document driver review

## Findings

- No critical or high finding: execution is incremental, memory-authorized, registry-selected, and format-specific behavior stays inside the codec crate.
- Significant: discovery collects accounted sample chunks before synchronous Arrow inference. It is bounded, but its peak approaches the discovery byte bound and needs a pull-based or spill-backed inference path before B5's memory criterion can close.
- Significant: malformed records currently fail the physical stream; the contract-owned recoverable row quarantine/residual path is not yet implemented for JSON.
- Significant: the 256-level nesting limit is deterministic and safe but has not yet passed the B5 fuzz/property corpus or an operator-configured policy review.
- Minor: JSON framing currently supports the complete top-level object/array-of-objects contract only. Selector semantics remain an explicit B5 item rather than leaking an ad hoc selector into generic source code.

## Assumptions tested

The review searched generic orchestration for first-party format branches and the old decoder entry point, inspected parser dependency direction, exercised one-byte chunk boundaries and string-contained delimiters, and verified source/project live paths plus zero residual source memory.

## Verdict

Concerns raised. The slice is safe to commit as progress on FX1/B5, but neither ticket should close until the significant items and remote external-driver law are evidenced.

## Residual risk

A discovery sample close to its configured memory budget can wait for another stage reservation because inference is not yet pull-based. B5 owns that repair; it must not be normalized as an acceptable steady-state architecture.
