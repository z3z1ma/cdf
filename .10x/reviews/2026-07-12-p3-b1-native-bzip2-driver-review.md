Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transform-bzip2
Verdict: pass

# Native bzip2 driver review

## Assumptions tested

- Cumulative decoder counters are differenced per call and cannot consume trailing member bytes.
- Member CRC/magic verification remains enabled and no small-memory fallback silently changes performance.
- Decoder state is allocated only after native working-set admission.
- EOF, zero progress, `MemNeeded`, expansion, and cancellation cannot emit silent success.
- The new dependency uses the expected Rust backend and its license/source are explicit.

## Findings

No critical or significant leaf-driver finding remains. One-byte rechunking and concatenated members prove cursor accounting; corruption/truncation tests exercise decoder verification; the release comparison is 0.997x the native reader wrapper.

The Rust backend exposes a C-compatible ABI internally and contains allocator/pointer unsafe code. Algorithm modules forbid unsafe, the crate has no build script, and CDF calls only the safe `bzip2::Decompress` wrapper. This risk is explicit in evidence and supply-chain exemptions.

## Verdict

Pass for the leaf implementation and exact dependency tuple.

## Residual risk

Native allocator RSS and malformed-input breadth remain stress/fuzz obligations in B1.

