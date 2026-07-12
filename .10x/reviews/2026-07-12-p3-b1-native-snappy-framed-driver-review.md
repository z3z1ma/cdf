Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transform-snappy, crates/cdf-runtime/src/format.rs, supply-chain/audits.toml
Verdict: pass

# Native Snappy framed driver review

## Findings

- No critical or significant correctness/performance finding remains in the leaf-driver slice.
- Raw unframed Snappy is rejected with explicit framing remediation; it is not guessed from ambiguous bytes.
- The checksum is verified before the decoded chunk enters the output stream, making the natural 64 KiB Snappy chunk a fatal publication window.
- Chunk payload and decoded-size limits are enforced before unbounded allocation. Input, internal buffers, and output remain separately visible to the ledger.
- The initial frame-reader wrapper and byte-at-a-time cursor path were both deleted. The committed hot path uses the raw decoder, hardware/software CRC32C, bulk accounted reads, and zero-copy full-block output transfer.
- Release evidence exceeds the 0.6x catalog threshold at 1.680x reference on the recorded fixture.
- `crc32c 0.6.8` unsafe paths were reviewed: runtime feature gates precede target-feature calls; aligned views are bounded and lifetime-preserving; generated tables write only to Cargo `OUT_DIR`; no ambient sensitive capability exists.

## Verdict

Pass for the native framed-Snappy milestone.

## Residual risk

The performance fixture is a single deterministic data shape, and fuzz coverage is not yet present. Product-path composition is still absent. B1 remains open for both.
