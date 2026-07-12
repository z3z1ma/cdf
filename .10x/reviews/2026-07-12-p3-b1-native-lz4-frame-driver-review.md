Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-transform-lz4
Verdict: pass

# Native LZ4 frame driver review

## Assumptions tested

- Linked blocks retain only the format's 64 KiB dictionary instead of prior decoded objects.
- Block/header/content checksums are computed over the correct encoded/decoded domains.
- Raw, legacy, dictionary, oversized, truncated, and cancelled streams cannot silently select a fallback.
- The implementation allocates only beneath an admitted working-set lease and releases all input/output/native leases.
- The direct frame parser improves composition rather than duplicating source/runtime dispatch.

## Findings

No critical or significant leaf-driver defect remains. Standard and linked blocks pass against LZ4 Flex's encoder; concatenated framing, checksum corruption, truncation, expansion, cancellation, and one-byte rechunking are covered. Release throughput is 0.797x LZ4 Flex's synchronous `FrameDecoder` on the same 64 MiB fixture.

Whole-frame content-checksum authority arrives after block output. This is not hidden: B1 already requires a product-level checksum publication barrier before accepted stream visibility. The leaf driver must not be wired directly into accepted extraction without that barrier.

## Verdict

Pass for the leaf implementation. Product composition remains open in B1/FX1.

## Residual risk

The current corpus is deterministic rather than coverage-guided fuzzing. Dictionary frames are intentionally rejected until explicit dictionary identity/lifecycle authority exists.

